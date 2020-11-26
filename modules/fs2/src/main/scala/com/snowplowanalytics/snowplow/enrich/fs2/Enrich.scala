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
import cats.data.{ValidatedNel, NonEmptyList}
import cats.implicits._

import cats.effect.{ContextShift, Blocker, Clock, Concurrent, Sync}

import fs2.Stream

import _root_.io.sentry.SentryClient
import _root_.io.circe.Json
import _root_.io.circe.syntax._

import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import natchez.{Span, EntryPoint, Kernel}

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.badrows.{Processor, BadRow, Failure, Payload => BadRowPayload}

import com.snowplowanalytics.snowplow.enrich.common.EtlPipeline
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.loaders.{CollectorPayload, ThriftLoader}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry

object Enrich {

  /**
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
   * via [[enrichWith]] and sinking into [[GoodSink]] and [[BadSink]] respectively.
   * Can be stopped via _stop signal_ from [[Environment]]
   *
   * The stream won't download any enrichment DBs, it is responsibility of [[Assets]]
   * [[Assets.State.make]] downloads assets for the first time unconditionally during
   * [[Environment]] initialisation, then if `assetsUpdatePeriod` has been specified -
   * they'll be refreshed periodically by [[Assets.updateStream]]
   */
  def run[F[_]: Concurrent: ContextShift: Clock](env: Environment[F]): Stream[F, Unit] = {
    val registry: F[EnrichmentRegistry[F]] = env.enrichments.get.map(_.registry)
    val enrich: Enrich[F] = enrichWith[F](registry, env.blocker, env.igluClient, env.sentry, env.metrics.enrichLatency, env.tracing)
    val badSink: BadSink[F] = _.evalTap(_ => env.metrics.badCount).through(env.bad)
    val goodSink: GoodSink[F] = _.evalTap(_ => env.metrics.goodCount).through(env.good)

    env.source
      .pauseWhen(env.pauseEnrich)
      .evalTap(_ => env.metrics.rawCount)
      .parEvalMapUnordered(ConcurrencyLevel)(enrich)
      .flatMap(_.decompose[BadRow, EnrichedEvent])
      .observeEither(badSink, goodSink)
      .void
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
    enrichLatency: Option[Long] => F[Unit],
    tracing: Option[EntryPoint[F]]
  )(
    row: Payload[F, Array[Byte]]
  ): F[Result[F]] = {
    tracing.map(_.root("root")).sequence.use { span =>
      val result =
        for {
          payload         <- within(span, "to-collector-payload")(ThriftLoader.toCollectorPayload(row.data, processor).pure[F])
          kernel           = getTraceHeaders(payload)
          _ = span
          collectorTstamp  = payload.toOption.flatMap(_.flatMap(_.context.timestamp).map(_.getMillis))
          _               <- Logger[F].debug(payloadToString(payload))
          etlTstamp       <- Clock[F].realTime(TimeUnit.MILLISECONDS).map(millis => new DateTime(millis))
          registry        <- enrichRegistry
          enrich           = EtlPipeline.processEvents[F](adapterRegistry, registry, igluClient, processor, etlTstamp, payload)
          enriched        <- within(span, "enrich")(blocker.blockOn(enrich))
          finalise         = within(span, "ack")(enrichLatency(collectorTstamp) *> row.finalise)
        } yield Payload(enriched, finalise)

      result.handleErrorWith(sendToSentry[F](row, sentry))
    }
  }

  def within[F[_]: Sync, A](span: Option[Span[F]], name: String)(action: F[A]): F[A] =
    span match {
      case Some(s) =>
        s.span(name).use { _ => action }
      case None => action
    }

  def getTraceHeaders(payload: ValidatedNel[BadRow.CPFormatViolation, Option[CollectorPayload]]): Kernel = {
    val headers = payload.toOption.flatten.flatMap(_.context.headers).flatMap { str =>
      str.split(":", 2) match {
        case Array(key, value) => Some((key, value))
        case _ => None
      }
    }
    Kernel(headers.toMap)
  }

  /** Stringify `ThriftLoader` result for debugging purposes */
  def payloadToString(payload: ValidatedNel[BadRow.CPFormatViolation, Option[CollectorPayload]]): String =
    payload.fold(_.asJson.noSpaces, _.map(_.toBadRowPayload.asJson.noSpaces).getOrElse("None"))

  private val EnrichedFields =
    classOf[EnrichedEvent].getDeclaredFields
      .filterNot(_.getName.equals("pii"))
      .map { field => field.setAccessible(true); field }
      .toList

  /** Transform enriched event into canonical TSV */
  def encodeEvent(enrichedEvent: EnrichedEvent): String =
    EnrichedFields
      .map { field =>
        val prop = field.get(enrichedEvent)
        if (prop == null) "" else prop.toString
      }
      .mkString("\t")

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
