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
package com.snowplowanalytics.snowplow.enrich.core

import java.time.Instant
import java.nio.charset.StandardCharsets.UTF_8
import java.lang.reflect.Field

import scala.concurrent.duration.DurationLong

import org.joda.time.DateTime

import cats.implicits._
import cats.data.Validated
import cats.Foldable

import cats.effect.kernel.{Async, Sync, Unique}
import cats.effect.implicits._

import fs2.{Pipe, Stream}

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure => BadRowFailure, Payload => BadRowPayload, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor, ListOfList, Sinkable}
import com.snowplowanalytics.snowplow.runtime.syntax.foldable._

import com.snowplowanalytics.snowplow.enrich.common.EtlPipeline
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.{CollectorPayload, ThriftLoader}
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils, OptionIor}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry

object Processing {

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] =
    env.source
      .stream(EventProcessingConfig(EventProcessingConfig.NoWindowing, env.metrics.setLatency), eventProcessor(env))
      .concurrently(Assets.updateStream(env.assets, env.assetsUpdatePeriod, env.enrichmentRegistry, env.blobClients))

  private def eventProcessor[F[_]: Async](
    env: Environment[F]
  ): EventProcessor[F] =
    _.through(PayloadProvider.pipe(env.badRowProcessor, env.decompressionConfig))
      .through(parseBytes(env))
      .through(enrich(env))
      .through(addIdentityContext(env))
      .through(collectMetadata(env))
      .through(serialize(env))
      .through(sink(env))
      .through(setE2ELatencyMetric(env))
      .through(emitToken)

  private case class Parsed(
    collectorPayloads: List[CollectorPayload],
    bad: List[BadRow],
    collectorTstamp: Option[DateTime],
    etlTstamp: Instant,
    token: Option[Unique.Token]
  )

  private case class Enriched(
    enriched: List[EnrichedEvent],
    failed: List[EnrichedEvent],
    bad: ListOfList[BadRow],
    collectorTstamp: Option[DateTime],
    etlTstamp: Instant,
    token: Option[Unique.Token]
  )

  private case class Serialized(
    enriched: List[Sinkable],
    failed: List[Sinkable],
    bad: ListOfList[Sinkable],
    collectorTstamp: Option[DateTime],
    token: Option[Unique.Token]
  )

  private def parseBytes[F[_]: Async](
    env: Environment[F]
  ): Pipe[F, PayloadProvider.Result, Parsed] =
    _.evalMap { input =>
      for {
        etlTstamp <- Sync[F].realTimeInstant
        (thriftBad, collectorPayloads) <- Foldable[List].traverseSeparateUnordered(input.payloads) { buffer =>
                                            Sync[F].delay {
                                              ThriftLoader.toCollectorPayload(buffer, env.badRowProcessor, etlTstamp).toEither
                                            }
                                          }
        bad = input.bad ::: thriftBad.flatMap(_.toList)
        _ <- env.metrics.addRaw(bad.size + collectorPayloads.size)
        collectorTstamp = collectorPayloads.headOption.map(_.context.timestamp)
      } yield Parsed(
        collectorPayloads,
        bad,
        collectorTstamp,
        etlTstamp,
        input.ack
      )
    }

  private def enrich[F[_]: Async](
    env: Environment[F]
  ): Pipe[F, Parsed, Enriched] = { in =>
    def enrichPayload(
      collectorPayload: CollectorPayload,
      etlTstamp: Instant,
      enrichmentRegistry: EnrichmentRegistry[F]
    ): F[List[OptionIor[BadRow, EnrichedEvent]]] =
      EtlPipeline.processEvents[F](
        env.adapterRegistry,
        enrichmentRegistry,
        env.igluClient,
        env.badRowProcessor,
        new DateTime(etlTstamp.toEpochMilli),
        Validated.Valid(collectorPayload),
        EtlPipeline.FeatureFlags(env.validation.acceptInvalid),
        env.metrics.addInvalid(1),
        env.registryLookup,
        env.validation.atomicFieldsLimits,
        env.failedSink.isDefined,
        env.validation.maxJsonDepth
      )

    in.parEvalMap(env.cpuParallelism) { parsed =>
      env.enrichmentRegistry.opened
        .use { enrRegistry =>
          parsed.collectorPayloads
            .parUnorderedTraverse { payload =>
              enrichPayload(payload, parsed.etlTstamp, enrRegistry)
            }
            .map { enriched =>
              enriched.flatten.foldLeft((List.empty[BadRow], List.empty[EnrichedEvent], List.empty[EnrichedEvent], 0)) {
                case ((ls, bs, rs, dropped), i) =>
                  i match {
                    case OptionIor.Left(b) => (b :: ls, bs, rs, dropped)
                    case OptionIor.Right(c) => (ls, bs, c :: rs, dropped)
                    case OptionIor.Both(b, c) => (b :: ls, c :: bs, rs, dropped)
                    case OptionIor.None => (ls, bs, rs, dropped + 1)
                  }
              }
            }
        }
        .flatMap {
          case (bad, failed, enriched, droppedCount) =>
            val updateDroppedMetric = if (droppedCount > 0) env.metrics.addDropped(droppedCount) else Sync[F].unit
            updateDroppedMetric
              .as(
                Enriched(
                  enriched,
                  failed,
                  ListOfList.ofLists(bad, parsed.bad),
                  parsed.collectorTstamp,
                  parsed.etlTstamp,
                  parsed.token
                )
              )
        }
    }
  }

  private def serialize[F[_]: Async](
    env: Environment[F]
  ): Pipe[F, Enriched, Serialized] = { in =>
    in.evalMap { enriched =>
      Sync[F].delay {
        val (sizeViolations, good) = enriched.enriched.foldLeft((List.empty[Sinkable], List.empty[Sinkable])) {
          case ((ls, rs), e) =>
            serializeEnriched(e,
                              env.partitionKeyField,
                              env.attributeFields,
                              env.sinkMaxSize,
                              env.badRowProcessor,
                              enriched.etlTstamp
            ) match {
              case Left(sv) => (sv :: ls, rs)
              case Right(e) => (ls, e :: rs)
            }
        }
        val failed = enriched.failed.map { failed =>
          serializeFailed(failed, env.partitionKeyField, env.attributeFields, env.sinkMaxSize)
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
    partitionKeyField: Option[Field],
    attributeFields: List[Field],
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
    } else {
      val partitionKey = partitionKeyField.flatMap(f => Option(f.get(enriched)).map(_.toString))
      val attributes = attributeFields.flatMap { f =>
        Option(f.get(enriched)).map(v => f.getName -> v.toString)
      }.toMap
      Right(Sinkable(bytes, partitionKey, attributes))
    }
  }

  private def serializeFailed(
    failed: EnrichedEvent,
    partitionKeyField: Option[Field],
    attributeFields: List[Field],
    maxRecordSize: Int
  ): Option[Sinkable] = {
    val tsv = ConversionUtils.tabSeparatedEnrichedEvent(failed)
    val bytes = tsv.getBytes(UTF_8)
    if (bytes.length > maxRecordSize)
      None
    else {
      val partitionKey = partitionKeyField.flatMap(f => Option(f.get(failed)).map(_.toString))
      val attributes = attributeFields.flatMap { f =>
        Option(f.get(failed)).map(v => f.getName -> v.toString)
      }.toMap
      Some(Sinkable(bytes, partitionKey, attributes))
    }
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

  private def sink[F[_]: Async](
    env: Environment[F]
  ): Pipe[F, Serialized, Serialized] =
    _.parEvalMap(env.sinkParallelism) { batch =>
      List(sinkEnriched(env, batch), sinkFailed(env, batch), sinkBad(env, batch)).parSequence_.as(batch)
    }

  private def sinkEnriched[F[_]: Async](
    env: Environment[F],
    batch: Serialized
  ): F[Unit] =
    batch match {
      case Serialized(enriched, _, _, _, _) if enriched.nonEmpty =>
        env.enrichedSink.sink(ListOfList.ofLists(enriched)) >>
          env.metrics.addEnriched(enriched.size)
      case _ =>
        Sync[F].unit
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
    env: Environment[F],
    batch: Serialized
  ): F[Unit] =
    batch match {
      case Serialized(_, failed, _, _, _) if failed.nonEmpty =>
        env.failedSink match {
          case Some(sink) => sink.sink(ListOfList.ofLists(failed)) >> env.metrics.addFailed(failed.size)
          case _ => Sync[F].unit
        }
      case _ =>
        Sync[F].unit
    }

  private def sinkBad[F[_]: Async](
    env: Environment[F],
    batch: Serialized
  ): F[Unit] =
    batch match {
      case Serialized(_, _, bad, _, _) if bad.nonEmpty =>
        env.badSink.sink(bad) >> env.metrics.addBad(bad.asIterable.size)
      case _ =>
        Sync[F].unit
    }

  private def emitToken[F[_]]: Pipe[F, Serialized, Unique.Token] =
    _.map(_.token).unNone

  private def collectMetadata[F[_]: Async](
    env: Environment[F]
  ): Pipe[F, Enriched, Enriched] =
    env.metadata match {
      case Some(reporter) =>
        _.evalTap { enriched =>
          if (enriched.enriched.nonEmpty) {
            val extracts = Metadata.extractsForBatch(enriched.enriched)
            reporter.add(extracts)
          } else Async[F].unit
        }
      case None =>
        identity
    }

  private def addIdentityContext[F[_]: Async](env: Environment[F]): Pipe[F, Enriched, Enriched] =
    env.identity match {
      case Some(api) =>
        _.parEvalMap(api.concurrency) { batch =>
          if (batch.enriched.nonEmpty || batch.failed.nonEmpty)
            Identity.addContexts(api, batch.failed ::: batch.enriched).as(batch)
          else
            Sync[F].pure(batch)
        }
      case None =>
        identity
    }

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
        BadRowPayload.RawPayload(payload.take(maxRecordSize / 10))
      )
      .compact
      .getBytes(UTF_8)
    Sinkable(bytes, None, Map.empty)
  }
}
