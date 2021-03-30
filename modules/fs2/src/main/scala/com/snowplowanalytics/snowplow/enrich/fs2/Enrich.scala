/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.fs2

import java.time.Instant
import java.util.Base64
import java.util.concurrent.TimeUnit

import org.joda.time.DateTime
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.Applicative
import cats.implicits._

import cats.effect.{Blocker, Clock, Concurrent, ContextShift, Sync}

import fs2.{Pipe, Stream}

import _root_.io.sentry.SentryClient
import _root_.io.circe.Json
import _root_.io.circe.syntax._

import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.badrows.{Processor, BadRow, Failure, Payload => BadRowPayload}
import com.snowplowanalytics.snowplow.enrich.common.EtlPipeline
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.loaders.{CollectorPayload, ThriftLoader}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.utils.ConversionUtils

object Enrich {

  /**
   * Parallelism of an enrich stream.
   * Unlike for thread pools it doesn't make much sense to use `CPUs x 2` formulae
   * as we're not sizing threads, but fibers and memory is the only cost of them
   */
  val ConcurrencyLevel = 64

  /** Default adapter registry, can be constructed dynamically in future */
  val adapterRegistry = new AdapterRegistry()

  val processor: Processor = Processor(generated.BuildInfo.name, generated.BuildInfo.version)

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  /**
   * Run a primary enrichment stream, reading from [[Environment]] source, enriching
   * via [[enrichWith]] and sinking into the Good, Bad, and Pii sinks.
   * Can be stopped via _stop signal_ from [[Environment]]
   *
   * The stream won't download any enrichment DBs, it is responsibility of [[Assets]]
   * [[Assets.State.make]] downloads assets for the first time unconditionally during
   * [[Environment]] initialisation, then if `assetsUpdatePeriod` has been specified -
   * they'll be refreshed periodically by [[Assets.updateStream]]
   */
  def run[F[_]: Concurrent: ContextShift: Clock](env: Environment[F]): Stream[F, Unit] = {
    val registry: F[EnrichmentRegistry[F]] = env.enrichments.get.map(_.registry)
    val enrich: Enrich[F] = enrichWith[F](registry, env.blocker, env.igluClient, env.sentry, env.metrics.enrichLatency)

    env.source
      .pauseWhen(env.pauseEnrich)
      .evalTap(_ => env.metrics.rawCount)
      .parEvalMapUnordered(ConcurrencyLevel)(enrich)
      .through(Payload.sink(resultSink(env)))
  }

  def resultSink[F[_]: Concurrent](env: Environment[F]): Pipe[F, List[Validated[BadRow, EnrichedEvent]], Nothing] =
    _.flatMap(Stream.emits(_))
      .flatMap(toOutputs)
      .map(Output.serialize)
      .observe(Output.sink(badSink(env), goodSink(env), piiSink(env)))
      .drain

  def badSink[F[_]: Applicative](env: Environment[F]): ByteSink[F] =
    _.evalTap(_ => env.metrics.badCount)
      .through(env.bad)

  def goodSink[F[_]: Applicative](env: Environment[F]): ByteSink[F] =
    _.evalTap(_ => env.metrics.goodCount)
      .through(env.good)

  def piiSink[F[_]: Applicative](env: Environment[F]): ByteSink[F] =
    _.through(env.pii)

  def toOutputs[F[_]](result: Validated[BadRow, EnrichedEvent]): Stream[F, Output.Parsed] =
    result match {
      case Validated.Valid(ee) =>
        val toEmit = List(Output.Good(ee)) ++ ConversionUtils.getPiiEvent(processor, ee).map(Output.Pii(_))
        Stream.emits(toEmit)
      case Validated.Invalid(bad) =>
        Stream.emit(Output.Bad(bad))
    }

  /**
   * Enrich a single `CollectorPayload` to get list of bad rows and/or enriched events
   *
   * Along with actual `ack` the `enrichLatency` gauge will be updated
   */
  def enrichWith[F[_]: Clock: Sync: ContextShift](
    enrichRegistry: F[EnrichmentRegistry[F]],
    blocker: Blocker,
    igluClient: Client[F, Json],
    sentry: Option[SentryClient],
    enrichLatency: Option[Long] => F[Unit]
  )(
    row: Payload[F, Array[Byte]]
  ): F[Result[F]] = {
    val payload = ThriftLoader.toCollectorPayload(row.data, processor)
    val collectorTstamp = payload.toOption.flatMap(_.flatMap(_.context.timestamp).map(_.getMillis))

    val result =
      for {
        _ <- Logger[F].debug(payloadToString(payload))
        etlTstamp <- Clock[F].realTime(TimeUnit.MILLISECONDS).map(millis => new DateTime(millis))
        registry <- enrichRegistry
        enrich = EtlPipeline.processEvents[F](adapterRegistry, registry, igluClient, processor, etlTstamp, payload)
        enriched <- blocker.blockOn(enrich)
        trackLatency = enrichLatency(collectorTstamp)
      } yield Payload(enriched, trackLatency *> row.finalise)

    result.handleErrorWith(sendToSentry[F](row, sentry))
  }

  /** Stringify `ThriftLoader` result for debugging purposes */
  def payloadToString(payload: ValidatedNel[BadRow.CPFormatViolation, Option[CollectorPayload]]): String =
    payload.fold(_.asJson.noSpaces, _.map(_.toBadRowPayload.asJson.noSpaces).getOrElse("None"))

  /** Log an error, turn the problematic `CollectorPayload` into `BadRow` and notify Sentry if configured */
  def sendToSentry[F[_]: Sync: Clock](original: Payload[F, Array[Byte]], sentry: Option[SentryClient])(error: Throwable): F[Result[F]] =
    for {
      _ <- Logger[F].error("Runtime exception during payload enrichment. CollectorPayload converted to generic_error and ack'ed")
      now <- Clock[F].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli)
      _ <- original.finalise
      badRow = genericBadRow(original.data, now, error)
      _ <- sentry match {
             case Some(client) =>
               Sync[F].delay(client.sendException(error))
             case None =>
               Sync[F].unit
           }
    } yield Payload(List(badRow.invalid), Sync[F].unit)

  /** Build a `generic_error` bad row for unhandled runtime errors */
  def genericBadRow(
    row: Array[Byte],
    time: Instant,
    error: Throwable
  ): BadRow.GenericError = {
    val base64 = new String(Base64.getEncoder.encode(row))
    val rawPayload = BadRowPayload.RawPayload(base64)
    val failure = Failure.GenericFailure(time, NonEmptyList.one(error.toString))
    BadRow.GenericError(processor, failure, rawPayload)
  }
}
