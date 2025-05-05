/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.streams.common

import java.time.Instant
import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.duration.DurationLong

import org.joda.time.DateTime

import cats.implicits._
import cats.data.Validated
import cats.Foldable

import cats.effect.kernel.{Async, Sync, Unique}

import fs2.{Chunk, Pipe, Stream}

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure => BadRowFailure, Payload => BadRowPayload, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.{ListOfList, Sinkable}
import com.snowplowanalytics.snowplow.runtime.syntax.foldable._

import com.snowplowanalytics.snowplow.enrich.common.EtlPipeline
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.{CollectorPayload, ThriftLoader}
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils, OptionIor}

object Processing {

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] =
    env.source.stream(EventProcessingConfig(EventProcessingConfig.NoWindowing, env.metrics.setLatency), eventProcessor(env))

  private def eventProcessor[F[_]: Async](
    env: Environment[F]
  ): EventProcessor[F] =
    _.through(parseBytes(env))
      .through(enrich(env))
      .through(serialize(env))
      .through(sinkEnriched(env))
      .through(setE2ELatencyMetric(env))
      .through(sinkFailed(env))
      .through(sinkBad(env))
      .through(emitToken)

  private case class Parsed(
    collectorPayloads: List[CollectorPayload],
    bad: List[BadRow],
    collectorTstamp: Option[DateTime],
    etlTstamp: Instant,
    token: Unique.Token
  )

  private case class Enriched(
    enriched: List[EnrichedEvent],
    failed: List[EnrichedEvent],
    bad: ListOfList[BadRow],
    collectorTstamp: Option[DateTime],
    etlTstamp: Instant,
    token: Unique.Token
  )

  private case class Serialized(
    enriched: List[Sinkable],
    failed: List[Sinkable],
    bad: ListOfList[Sinkable],
    collectorTstamp: Option[DateTime],
    token: Unique.Token
  )

  private def parseBytes[F[_]: Async](
    env: Environment[F]
  ): Pipe[F, TokenedEvents, Parsed] =
    _.parEvalMap(env.cpuParallelism) { input =>
      for {
        etlTstamp <- Sync[F].realTimeInstant
        (bad, collectorPayloads) <- Foldable[Chunk].traverseSeparateUnordered(input.events) { buffer =>
                                      Sync[F].delay {
                                        ThriftLoader.toCollectorPayload(buffer, env.badRowProcessor, etlTstamp).toEither
                                      }
                                    }
        cpFormatViolations = bad.flatMap(_.toList)
        _ <- env.metrics.addRaw(cpFormatViolations.size + collectorPayloads.size)
        collectorTstamp = collectorPayloads.headOption.map(_.context.timestamp)
      } yield Parsed(
        collectorPayloads,
        cpFormatViolations,
        collectorTstamp,
        etlTstamp,
        input.ack
      )
    }

  private def enrich[F[_]: Async](
    env: Environment[F]
  ): Pipe[F, Parsed, Enriched] = { in =>
    def enrichPayload(collectorPayload: CollectorPayload, etlTstamp: Instant): F[List[OptionIor[BadRow, EnrichedEvent]]] =
      EtlPipeline.processEvents[F](
        env.adapterRegistry,
        env.enrichmentRegistry,
        env.igluClient,
        env.badRowProcessor,
        new DateTime(etlTstamp),
        Validated.Valid(collectorPayload),
        EtlPipeline.FeatureFlags(env.validation.acceptInvalid),
        env.metrics.addInvalid(1),
        env.registryLookup,
        env.validation.atomicFieldsLimits,
        env.failedSink.isDefined,
        env.validation.maxJsonDepth
      )

    in.parEvalMap(env.cpuParallelism) { parsed =>
      Foldable[List]
        .foldM(parsed.collectorPayloads, (List.empty[BadRow], List.empty[EnrichedEvent], List.empty[EnrichedEvent])) {
          case ((lefts, boths, rights), payload) =>
            enrichPayload(payload, parsed.etlTstamp).map { list =>
              list.foldLeft((lefts, boths, rights)) {
                case ((ls, bs, rs), i) =>
                  i match {
                    case OptionIor.Left(b) => (b :: ls, bs, rs)
                    case OptionIor.Right(c) => (ls, bs, c :: rs)
                    case OptionIor.Both(b, c) => (b :: ls, c :: bs, rs)
                    case OptionIor.None => (ls, bs, rs)
                  }
              }
            }
        }
        .map {
          case (bad, failed, enriched) =>
            Enriched(
              enriched,
              failed,
              ListOfList.ofLists(bad, parsed.bad),
              parsed.collectorTstamp,
              parsed.etlTstamp,
              parsed.token
            )
        }
    }
  }

  private def serialize[F[_]: Async](
    env: Environment[F]
  ): Pipe[F, Enriched, Serialized] = { in =>
    in.parEvalMap(env.cpuParallelism) { enriched =>
      Sync[F].delay {
        val (sizeViolations, good) = enriched.enriched.foldLeft((List.empty[Sinkable], List.empty[Sinkable])) {
          case ((ls, rs), e) =>
            serializeEnriched(e, env.getPartitionKey, env.getAttributes, env.sinkMaxSize, env.badRowProcessor, enriched.etlTstamp) match {
              case Left(sv) => (sv :: ls, rs)
              case Right(e) => (ls, e :: rs)
            }
        }
        val failed = enriched.failed.map { failed =>
          serializeFailed(failed, env.getPartitionKey, env.getAttributes, env.sinkMaxSize)
        }.flatten
        val bad = enriched.bad.mapUnordered { br =>
          serializeBad(br, env.sinkMaxSize, env.badRowProcessor, enriched.etlTstamp)
        }
        Serialized(
          good,
          failed,
          bad.prepend(sizeViolations),
          enriched.collectorTstamp,
          enriched.token
        )
      }
    }
  }

  private def serializeEnriched(
    enriched: EnrichedEvent,
    getPartitionKey: EnrichedEvent => Option[String],
    getAttributes: EnrichedEvent => Map[String, String],
    maxRecordSize: Int,
    processor: BadRowProcessor,
    etlTstamp: Instant
  ): Either[Sinkable, Sinkable] = {
    val tsv = ConversionUtils.tabSeparatedEnrichedEvent(enriched)
    val bytes = tsv.getBytes(UTF_8)
    val size = bytes.length
    if (size > maxRecordSize) {
      val error = s"Enriched event exceeds the maximum allowed size of $maxRecordSize bytes"
      val sv = mkSizeViolation(tsv, maxRecordSize, processor, error, etlTstamp)
      Left(sv)
    } else
      Right(Sinkable(bytes, getPartitionKey(enriched), getAttributes(enriched)))
  }

  private def serializeFailed(
    failed: EnrichedEvent,
    getPartitionKey: EnrichedEvent => Option[String],
    getAttributes: EnrichedEvent => Map[String, String],
    maxRecordSize: Int
  ): Option[Sinkable] = {
    val tsv = ConversionUtils.tabSeparatedEnrichedEvent(failed)
    val bytes = tsv.getBytes(UTF_8)
    if (bytes.length > maxRecordSize)
      None
    else
      Some(Sinkable(bytes, getPartitionKey(failed), getAttributes(failed)))
  }

  private def serializeBad(
    badRow: BadRow,
    maxRecordSize: Int,
    processor: BadRowProcessor,
    etlTstamp: Instant
  ): Sinkable = {
    val asStr = badRow.compact
    val bytes = asStr.getBytes(UTF_8)
    val size = bytes.size
    if (size > maxRecordSize) {
      val error = s"Event failed enrichment and resulting bad row exceeds the maximum allowed size of $maxRecordSize bytes"
      mkSizeViolation(asStr, maxRecordSize, processor, error, etlTstamp)
    } else
      Sinkable(bytes, None, Map.empty)
  }

  private def sinkEnriched[F[_]: Async](
    env: Environment[F]
  ): Pipe[F, Serialized, Serialized] =
    _.evalTap {
      case Serialized(enriched, _, _, _, _) =>
        env.enrichedSink.sink(ListOfList.ofLists(enriched)) >> env.metrics.addEnriched(enriched.size)
    }

  private def setE2ELatencyMetric[F[_]: Async](
    env: Environment[F]
  ): Pipe[F, Serialized, Serialized] =
    _.evalTap {
      _.collectorTstamp match {
        case Some(t) =>
          for {
            now <- Sync[F].realTime
            e2eLatency = now - t.getMillis.milliseconds
            _ <- env.metrics.setE2ELatency(e2eLatency)
          } yield ()
        case None => Sync[F].unit
      }
    }

  private def sinkFailed[F[_]: Async](
    env: Environment[F]
  ): Pipe[F, Serialized, Serialized] =
    _.evalTap {
      case Serialized(_, failed, _, _, _) =>
        env.failedSink match {
          case Some(sink) => sink.sink(ListOfList.ofLists(failed)) >> env.metrics.addFailed(failed.size)
          case _ => Sync[F].unit
        }
    }

  private def sinkBad[F[_]: Async](
    env: Environment[F]
  ): Pipe[F, Serialized, Serialized] =
    _.evalTap {
      case Serialized(_, _, bad, _, _) =>
        env.badSink.sink(bad) >> env.metrics.addBad(bad.asIterable.size)
    }

  private def emitToken[F[_]]: Pipe[F, Serialized, Unique.Token] =
    _.map(_.token)

  private def mkSizeViolation(
    payload: String,
    maxRecordSize: Int,
    processor: BadRowProcessor,
    error: String,
    etlTstamp: Instant
  ): Sinkable = {
    val bytes = BadRow
      .SizeViolation(
        processor,
        BadRowFailure.SizeViolation(etlTstamp, maxRecordSize, payload.length, error),
        BadRowPayload.RawPayload(payload.take(maxRecordSize * 8 / 10))
      )
      .compact
      .getBytes(UTF_8)
    Sinkable(bytes, None, Map.empty)
  }
}
