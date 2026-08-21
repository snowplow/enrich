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

import com.snowplowanalytics.snowplow.badrows.{
  BadRow,
  Failure => BadRowFailure,
  FailureDetails,
  Payload => BadRowPayload,
  Processor => BadRowProcessor
}
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, ListOfList, Sinkable}
import com.snowplowanalytics.snowplow.streams.compression.Compressor
import com.snowplowanalytics.snowplow.streams.compression.Decompression._
import com.snowplowanalytics.snowplow.runtime.syntax.foldable._

import com.snowplowanalytics.snowplow.enrich.common.EtlPipeline
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.{CollectorPayload, ThriftLoader}
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils, OptionIor}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry

object Processing {

  // Format version written into the compressed header for enriched TSV output.
  private val CompressedEnrichedVersion = 1

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] =
    env.source
      .decompressedStream(
        EventProcessingConfig(EventProcessingConfig.NoWindowing, env.metrics.setLatency),
        env.decompression,
        eventProcessor(env),
        env.badRowProcessor,
        toBadRow(env.badRowProcessor)
      )
      .concurrently(env.enrichmentRegistry.refreshStream(env.assetsUpdatePeriod))

  private def eventProcessor[F[_]: Async](
    env: Environment[F]
  ): DecompressedEventProcessor[F] =
    _.through(parseBytes(env))
      .through(enrich(env))
      .through(addIdentityContexts(env))
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
    enrichedCount: Int,
    enrichedBytesCount: Long,
    failed: List[Sinkable],
    bad: ListOfList[Sinkable],
    collectorTstamp: Option[DateTime],
    token: Option[Unique.Token]
  )

  private case class Compressed(
    records: List[Sinkable],
    count: Int,
    recordsBytesCount: Long,
    sizeViolations: List[Sinkable]
  )

  private def toBadRow(processor: BadRowProcessor): DecompressionError => BadRow =
    err =>
      BadRow.CPFormatViolation(
        processor,
        BadRowFailure.CPFormatViolation(err.timestamp, "compression", FailureDetails.CPFormatViolationMessage.Fallback(err.message)),
        BadRowPayload.RawPayload(err.payload)
      )

  private def parseBytes[F[_]: Async](
    env: Environment[F]
  ): Pipe[F, DecompressedTokenedEvents, Parsed] =
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
      env.enrichmentRegistry.snapshot
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
    // IMPORTANT: this MUST stay `evalMap`, not `parEvalMap`. When compression is enabled we drive a
    // single long-lived, mutable, non-thread-safe Compressor (env.compressor) from here. `evalMap`
    // guarantees these batches are processed one at a time on a single fiber, which is what makes
    // that shared Compressor safe. Parallelising this step would corrupt its native (off-heap)
    // state with no clean exception - see compressBatch below.
    in.evalMap { enriched =>
      Sync[F].delay {
        val failed = enriched.failed.flatMap { f =>
          serializeFailed(f, env.partitionKeyField, env.attributeFields, env.sinkMaxSize)
        }
        val bad = enriched.bad.mapUnordered { br =>
          serializeBad(br, env.sinkMaxSize, env.badRowProcessor, enriched.etlTstamp)
        }
        env.compressor match {
          case Some(compressor) =>
            // Partition key and attributes are not propagated when compression is enabled,
            // because multiple records are merged into a single compressed payload.
            val rawSinkables = enriched.enriched.map { e =>
              Sinkable(ConversionUtils.tabSeparatedEnrichedEvent(e).getBytes(UTF_8), None, Map.empty)
            }
            val compressed =
              compressBatch(rawSinkables, compressor, env.sinkMaxSize, env.badRowProcessor, enriched.etlTstamp)
            Serialized(
              compressed.records,
              compressed.count,
              compressed.recordsBytesCount,
              failed,
              bad.prepend(compressed.sizeViolations),
              enriched.collectorTstamp,
              enriched.token
            )
          case None =>
            val (sizeViolations, good, bytesCount) =
              enriched.enriched.foldLeft((List.empty[Sinkable], List.empty[Sinkable], 0L)) {
                case ((ls, rs, bs), e) =>
                  serializeEnriched(e,
                                    env.partitionKeyField,
                                    env.attributeFields,
                                    env.sinkMaxSize,
                                    env.badRowProcessor,
                                    enriched.etlTstamp
                  ) match {
                    case Left(sv) => (sv :: ls, rs, bs)
                    case Right(s) => (ls, s :: rs, bs + s.bytes.length.toLong)
                  }
              }
            Serialized(
              good,
              good.size,
              bytesCount,
              failed,
              bad.prepend(sizeViolations),
              enriched.collectorTstamp,
              enriched.token
            )
        }
      }
    }
  }

  // WARNING: `compressor` is a single, long-lived, mutable and NOT thread-safe object, reused across
  // every frame and every batch (its off-heap state is reset between frames rather than reallocated).
  // This is only safe because serialize drives it sequentially from a single fiber - see the comment
  // on serialize's `evalMap`. Do not refactor this into a parallelized/concurrent block.
  private def compressBatch(
    sinkables: List[Sinkable],
    compressor: Compressor,
    maxRecordSize: Int,
    processor: BadRowProcessor,
    etlTstamp: Instant
  ): Compressed = {

    @scala.annotation.tailrec
    def go(
      remaining: List[Sinkable],
      compressed: List[Sinkable],
      count: Int,
      compressedBytesCount: Long,
      oversized: List[Sinkable]
    ): Compressed =
      remaining match {
        case Nil =>
          val last =
            if (compressor.recordCount > 0) List(Sinkable(byteBufferToArray(compressor.result), None, Map.empty))
            else Nil
          Compressed(last ::: compressed, count, compressedBytesCount, oversized)
        case sinkable :: rest =>
          val bytes = sinkable.bytes
          if (compressor.addRecord(bytes, 0, bytes.length))
            go(rest, compressed, count + 1, compressedBytesCount + bytes.length, oversized)
          else {
            // The current frame is full. Take its bytes (copied out before the next reset), then
            // reset the reused compressor to start a fresh frame for this record.
            val newCompressed =
              if (compressor.recordCount > 0) Sinkable(byteBufferToArray(compressor.result), None, Map.empty) :: compressed
              else compressed
            compressor.reset(CompressedEnrichedVersion, maxRecordSize)
            if (compressor.addRecord(bytes, 0, bytes.length))
              go(rest, newCompressed, count + 1, compressedBytesCount + bytes.length, oversized)
            else {
              // Record can't fit even in a fresh frame — emit as a size violation bad row.
              // addRecord returned false, so we must reset again before adding any further records.
              compressor.reset(CompressedEnrichedVersion, maxRecordSize)
              val tsv = new String(bytes, UTF_8)
              val error = s"Enriched event exceeds the maximum allowed size of $maxRecordSize bytes after compression"
              val badRow = mkSizeViolation(tsv, maxRecordSize, processor, error, etlTstamp)
              go(rest, newCompressed, count, compressedBytesCount, badRow :: oversized)
            }
          }
      }

    compressor.reset(CompressedEnrichedVersion, maxRecordSize)
    go(sinkables, Nil, 0, 0L, Nil)
  }

  private def byteBufferToArray(buf: java.nio.ByteBuffer): Array[Byte] = {
    val arr = new Array[Byte](buf.remaining)
    buf.get(arr)
    arr
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
      case Serialized(enriched, enrichedCount, enrichedBytesCount, _, _, _, _) if enriched.nonEmpty =>
        env.enrichedSink.sink(ListOfList.ofLists(enriched)) >>
          env.metrics.addEnriched(enrichedCount) >>
          env.metrics.addEnrichedBytes(enrichedBytesCount)
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
      case Serialized(_, _, _, failed, _, _, _) if failed.nonEmpty =>
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
      case Serialized(_, _, _, _, bad, _, _) if bad.nonEmpty =>
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

  private def addIdentityContexts[F[_]: Async](env: Environment[F]): Pipe[F, Enriched, Enriched] =
    env.identity match {
      case Some(api) =>
        _.parEvalMap(api.concurrency) { batch =>
          if (batch.enriched.nonEmpty || batch.failed.nonEmpty)
            api.addIdentityContexts(batch.failed ::: batch.enriched).as(batch)
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
