/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.Base64

import org.joda.time.DateTime

import cats.data.{Ior, NonEmptyList, ValidatedNel}
import cats.{Monad, Parallel}
import cats.implicits._

import cats.effect.kernel.{Async, Sync}
import cats.effect.implicits._

import fs2.{Chunk, Pipe, Stream}

import _root_.io.sentry.SentryClient

import _root_.io.circe.syntax._

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, Processor, Payload => BadRowPayload}

import com.snowplowanalytics.snowplow.enrich.common.EtlPipeline
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.loaders.{CollectorPayload, ThriftLoader}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{AtomicFields, EnrichmentRegistry}
import com.snowplowanalytics.snowplow.enrich.common.utils.ConversionUtils

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.FeatureFlags

object Enrich {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  case class Result[+A](
    original: A,
    enriched: List[Enriched],
    collectorTstamp: Option[Long]
  )

  /**
   * Run a primary enrichment stream, reading from [[Environment]] source, enriching
   * via [[enrichWith]] and sinking into the Good, Bad, and Pii sinks.
   *
   * The stream won't download any enrichment DBs, it is responsibility of [[Assets]]
   * [[Assets.State.make]] downloads assets for the first time unconditionally during
   * [[Environment]] initialisation, then if `assetsUpdatePeriod` has been specified -
   * they'll be refreshed periodically by [[Assets.updateStream]]
   */
  def run[F[_]: Async, A](env: Environment[F, A]): Stream[F, Unit] = {

    def enrichChunk(chunk: Chunk[A]): F[List[Result[A]]] =
      env.semaphore.permit.surround {
        for {
          _ <- Logger[F].debug(s"Starting to process chunk of size ${chunk.size}")
          _ <- env.metrics.rawCount(chunk.size)
          begin <- Sync[F].realTime
          enrichments <- env.enrichments.get
          result <- chunk.parTraverseN(env.streamsSettings.concurrency.enrich) { payload =>
                      enrichWith[F, A](
                        enrichments.registry,
                        env.adapterRegistry,
                        env.igluClient,
                        env.sentry,
                        env.processor,
                        env.featureFlags,
                        env.metrics.invalidCount,
                        env.registryLookup,
                        env.atomicFields,
                        env.sinkIncomplete.isDefined,
                        env.maxJsonDepth,
                        env.getPayload,
                        payload
                      )
                    }
          end <- Sync[F].realTime
          _ <- Logger[F].debug(s"Chunk of size ${chunk.size} enriched in ${end - begin} ms")
        } yield result.toList
      }

    val enrichedStream: Stream[F, List[Result[A]]] = env.source.chunks.evalMap(enrichChunk)

    val sinkAndCheckpoint: Pipe[F, List[Result[A]], Unit] =
      _.parEvalMap(env.streamsSettings.concurrency.sink) { chunk =>
        for {
          begin <- Sync[F].realTime
          _ <- sinkChunk(chunk, env)
          end <- Sync[F].realTime
          _ <- Logger[F].debug(s"Chunk of size ${chunk.size} sunk in ${end - begin}")
        } yield chunk.map(_.original)
      }
        .evalMap(env.checkpoint)

    enrichedStream.through(CleanCancellation(sinkAndCheckpoint))
  }

  /**
   * Enrich a single `CollectorPayload` to get list of bad rows and/or enriched events
   * @return enriched event or bad row, along with the collector timestamp
   */
  def enrichWith[F[_]: Sync, A](
    enrichRegistry: EnrichmentRegistry[F],
    adapterRegistry: AdapterRegistry[F],
    igluClient: IgluCirceClient[F],
    sentry: Option[SentryClient],
    processor: Processor,
    featureFlags: FeatureFlags,
    invalidCount: F[Unit],
    registryLookup: RegistryLookup[F],
    atomicFields: AtomicFields,
    emitIncomplete: Boolean,
    maxJsonDepth: Int,
    getPayload: A => Array[Byte],
    row: A
  ): F[Result[A]] =
    for {
      bytes <- Sync[F].delay(getPayload(row))
      payload <- Sync[F].delay(ThriftLoader.toCollectorPayload(bytes, processor, featureFlags.tryBase64Decoding))
      etlTstamp <- Sync[F].realTime.map(time => new DateTime(time.toMillis))
      collectorTstamp = payload.toOption.flatMap(_.flatMap(_.context.timestamp).map(_.getMillis))
      enriched <- EtlPipeline
                    .processEvents[F](
                      adapterRegistry,
                      enrichRegistry,
                      igluClient,
                      processor,
                      etlTstamp,
                      payload,
                      FeatureFlags.toCommon(featureFlags),
                      invalidCount,
                      registryLookup,
                      atomicFields,
                      emitIncomplete,
                      maxJsonDepth
                    )
                    .handleErrorWith(recoverFromError[F](bytes, sentry, processor))
    } yield Result(row, enriched, collectorTstamp)

  /** Stringify `ThriftLoader` result for debugging purposes */
  def payloadToString(payload: ValidatedNel[BadRow.CPFormatViolation, Option[CollectorPayload]]): String =
    payload.fold(_.asJson.noSpaces, _.map(_.toBadRowPayload.asJson.noSpaces).getOrElse("None"))

  /** Log an error, turn the problematic `CollectorPayload` into `BadRow` and notify Sentry if configured */
  def recoverFromError[F[_]: Sync](
    bytes: Array[Byte],
    sentry: Option[SentryClient],
    processor: Processor
  )(
    error: Throwable
  ): F[List[Enriched]] =
    for {
      _ <- Logger[F].error("Runtime exception during payload enrichment. CollectorPayload converted to generic_error and ack'ed")
      now <- Sync[F].realTimeInstant
      badRow = genericBadRow(bytes, now, error, processor)
      _ <- sentry match {
             case Some(client) =>
               Sync[F].delay(client.sendException(error))
             case None =>
               Sync[F].unit
           }
    } yield List(Ior.left(badRow))

  /** Build a `generic_error` bad row for unhandled runtime errors */
  def genericBadRow(
    row: Array[Byte],
    time: Instant,
    error: Throwable,
    processor: Processor
  ): BadRow.GenericError = {
    val base64 = new String(Base64.getEncoder.encode(row))
    val rawPayload = BadRowPayload.RawPayload(base64)
    val failure = Failure.GenericFailure(time, NonEmptyList.one(ConversionUtils.cleanStackTrace(error)))
    BadRow.GenericError(processor, failure, rawPayload)
  }

  def sinkChunk[F[_]: Parallel: Sync, A](
    chunk: List[Result[A]],
    env: Environment[F, A]
  ): F[Unit] = {
    val (bad, enriched, incomplete) =
      chunk
        .flatMap(_.enriched)
        .foldLeft((List.empty[BadRow], List.empty[EnrichedEvent], List.empty[EnrichedEvent])) {
          case (previous, item) =>
            val (bad, enriched, incomplete) = previous
            item match {
              case Ior.Right(e) => (bad, e :: enriched, incomplete)
              case Ior.Left(br) => (br :: bad, enriched, incomplete)
              case Ior.Both(br, i) => (br :: bad, enriched, i :: incomplete)
            }
        }

    val (moreBad, good) = enriched.map { e =>
      serializeEnriched(e, env.processor, env.streamsSettings.maxRecordSize)
        .map(bytes => (e, AttributedData(bytes, env.goodPartitionKey(e), env.goodAttributes(e))))
    }.separate

    val (incompleteTooBig, incompleteBytes) = incomplete.map { e =>
      serializeEnriched(e, env.processor, env.streamsSettings.maxRecordSize)
        .map(bytes => AttributedData(bytes, env.goodPartitionKey(e), env.goodAttributes(e)))
    }.separate

    val allBad = (bad ++ moreBad).map(badRowResize(env, _))

    List(
      sinkGood(
        good,
        env.sinkGood,
        env.metrics.goodCount,
        env.metadata.observe,
        env.sinkPii,
        env.piiPartitionKey,
        env.piiAttributes,
        env.processor,
        env.streamsSettings.maxRecordSize
      ) *> env.metrics.enrichLatency(chunk.headOption.flatMap(_.collectorTstamp)),
      sinkBad(allBad, env.sinkBad, env.metrics.badCount),
      if (incompleteTooBig.nonEmpty) Logger[F].warn(s"${incompleteTooBig.size} incomplete events discarded because they are too big")
      else Sync[F].unit,
      sinkIncomplete(incompleteBytes, env.sinkIncomplete, env.metrics.incompleteCount)
    ).parSequence_
  }

  def sinkGood[F[_]: Sync](
    good: List[(EnrichedEvent, AttributedData[Array[Byte]])],
    sink: AttributedByteSink[F],
    incMetrics: Int => F[Unit],
    metadata: List[EnrichedEvent] => F[Unit],
    piiSink: Option[AttributedByteSink[F]],
    piiPartitionKey: EnrichedEvent => String,
    piiAttributes: EnrichedEvent => Map[String, String],
    processor: Processor,
    maxRecordSize: Int
  ): F[Unit] = {
    val enriched = good.map(_._1)
    val serialized = good.map(_._2)
    sink(serialized) *> incMetrics(good.size) *> metadata(enriched) *> sinkPii(enriched,
                                                                               piiSink,
                                                                               piiPartitionKey,
                                                                               piiAttributes,
                                                                               processor,
                                                                               maxRecordSize
    )
  }

  def sinkBad[F[_]: Monad](
    bad: List[Array[Byte]],
    sink: ByteSink[F],
    incMetrics: Int => F[Unit]
  ): F[Unit] =
    sink(bad) *> incMetrics(bad.size)

  def sinkPii[F[_]: Sync](
    enriched: List[EnrichedEvent],
    maybeSink: Option[AttributedByteSink[F]],
    partitionKey: EnrichedEvent => String,
    attributes: EnrichedEvent => Map[String, String],
    processor: Processor,
    maxRecordSize: Int
  ): F[Unit] =
    maybeSink match {
      case Some(sink) =>
        val (bad, serialized) =
          enriched
            .flatMap(ConversionUtils.getPiiEvent(processor, _))
            .map(e => serializeEnriched(e, processor, maxRecordSize).map(AttributedData(_, partitionKey(e), attributes(e))))
            .separate
        val logging =
          if (bad.nonEmpty)
            Logger[F].error(s"${bad.size} PII events couldn't get sent because they are too big")
          else
            Sync[F].unit
        sink(serialized) *> logging
      case None =>
        Sync[F].unit
    }

  def sinkIncomplete[F[_]: Sync](
    incomplete: List[AttributedData[Array[Byte]]],
    maybeSink: Option[AttributedByteSink[F]],
    incMetrics: Int => F[Unit]
  ): F[Unit] =
    maybeSink match {
      case Some(sink) => sink(incomplete) *> incMetrics(incomplete.size)
      case None => Sync[F].unit
    }

  def serializeEnriched(
    enriched: EnrichedEvent,
    processor: Processor,
    maxRecordSize: Int
  ): Either[BadRow, Array[Byte]] = {
    val asStr = ConversionUtils.tabSeparatedEnrichedEvent(enriched)
    val asBytes = asStr.getBytes(UTF_8)
    val size = asBytes.length
    if (size > maxRecordSize) {
      val msg = s"event passed enrichment but then exceeded the maximum allowed size $maxRecordSize bytes"
      val br = BadRow
        .SizeViolation(
          processor,
          Failure.SizeViolation(Instant.now(), maxRecordSize, size, msg),
          BadRowPayload.RawPayload(asStr.take(maxRecordSize * 8 / 10))
        )
      Left(br)
    } else Right(asBytes)
  }

  /**
   * Check if plain bad row (such as `enrichment_failure`) exceeds the `MaxRecordSize`
   * If it does - turn into size violation with trimmed
   */
  def badRowResize[F[_], A](env: Environment[F, A], badRow: BadRow): Array[Byte] = {
    val asStr = badRow.compact
    val originalBytes = asStr.getBytes(UTF_8)
    val size = originalBytes.size
    val maxRecordSize = env.streamsSettings.maxRecordSize
    if (size > maxRecordSize) {
      val msg = s"event failed enrichment, but resulting bad row exceeds allowed size $maxRecordSize"
      BadRow
        .SizeViolation(
          env.processor,
          Failure.SizeViolation(Instant.now(), maxRecordSize, size, msg),
          BadRowPayload.RawPayload(asStr.take(maxRecordSize / 10))
        )
        .compact
        .getBytes(UTF_8)
    } else originalBytes
  }
}
