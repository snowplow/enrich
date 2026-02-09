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

import cats.Show
import cats.implicits._
import cats.effect.{Async, Ref, Resource, Sync}
import cats.effect.implicits._
import fs2.{Chunk, Pipe, Stream}
import org.http4s.client.Client
import org.http4s.{MediaType, Method, Request, Status}
import org.http4s.headers.`Content-Type`
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import io.circe.syntax._
import retry.{RetryDetails, RetryPolicies, retryingOnFailuresAndAllErrors}

import com.snowplowanalytics.snowplow.runtime.AppInfo
import com.snowplowanalytics.snowplow.runtime.processing.BatchUp
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{MiscEnrichments => ME}
import com.snowplowanalytics.iglu.core.circe.implicits._

import scala.concurrent.duration.DurationInt
import java.time.Instant
import java.util.Base64
import java.nio.charset.StandardCharsets

trait MetadataReporter[F[_]] {

  /**
   * Tells the `MetadataReporter` about a new `Metadata.Aggregates`
   *
   * The job of the `MetadataReporter` is to batch up aggregates and report them after an appropriate delay.
   */
  def add(aggregates: Metadata.Aggregates): F[Unit]
}

object MetadataReporter {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  /**
   * Builds the `MetadataReporter` and runs it as a background resource so it reports aggregates at the configured intervals
   *
   * @param config Configures this reporter
   * @param appinfo Details about this instance of Enrich
   * @param httpClient The http client with which to send events to the metadata collector
   */
  def build[F[_]: Async](
    config: Config.Metadata,
    appInfo: AppInfo,
    httpClient: Client[F]
  ): Resource[F, MetadataReporter[F]] =
    for {
      ref <- Resource.eval(Ref.of[F, Metadata.Aggregates](Map.empty))
      _ <- stream(config, appInfo, httpClient, ref).compile.drain.background
    } yield new MetadataReporter[F] {
      def add(aggregates: Metadata.Aggregates): F[Unit] =
        ref.update(_ |+| aggregates)
    }

  private def stream[F[_]: Async](
    config: Config.Metadata,
    appInfo: AppInfo,
    httpClient: Client[F],
    ref: Ref[F, Metadata.Aggregates]
  ): Stream[F, Nothing] =
    Stream
      .fixedRate[F](config.interval)
      .evalMap(_ => ref.getAndSet(Map.empty))
      .filter(_.nonEmpty)
      .through(toTrackerProtocolJsonStrings(config, appInfo))
      .through(batchUpTrackerProtocolEvents(config))
      .evalMap(report(config, httpClient, _))
      .drain

  private implicit def stringifiedBatchable: BatchUp.Batchable[String, List[String]] =
    new BatchUp.Batchable[String, List[String]] {
      def weightOf(a: String): Long = a.length.toLong
      def single(a: String): List[String] = List(a)
      def combine(b: List[String], a: String): List[String] = a :: b
    }

  private def toTrackerProtocolJsonStrings[F[_]: Sync](
    config: Config.Metadata,
    appInfo: AppInfo
  ): Pipe[F, Metadata.Aggregates, Chunk[String]] =
    _.evalMap { aggregates =>
      Sync[F].realTimeInstant.map { periodEnd =>
        val periodStart = periodEnd.minusMillis(config.interval.toMillis)
        Chunk.from {
          aggregates.map {
            case (event, entitiesAndCount) =>
              toTrackerProtocolEvent(config, appInfo, event, entitiesAndCount, periodStart, periodEnd).noSpaces
          }
        }
      }
    }

  private def batchUpTrackerProtocolEvents[F[_]: Sync](config: Config.Metadata): Pipe[F, Chunk[String], List[String]] =
    _.flatMap { chunk =>
      Stream
        .chunk(chunk)
        .through(BatchUp.noTimeout[F, String, List[String]](config.maxBodySize.toLong))
    }

  private def report[F[_]: Async](
    config: Config.Metadata,
    httpClient: Client[F],
    trackerProtocolEvents: List[String]
  ): F[Unit] = {
    val body = mkPayloadData(trackerProtocolEvents)
    val request = Request[F](method = Method.POST, uri = config.endpoint.addPath("com.snowplowanalytics.snowplow").addPath("tp2"))
      .withEntity(body)
      .withContentType(`Content-Type`(MediaType.application.json))

    val policy = RetryPolicies.fibonacciBackoff[F](100.millis).join(RetryPolicies.limitRetries(10))
    def wasSuccessful(status: Status) = Sync[F].pure(status.isSuccess)
    def onFailure(status: Status, retryDetails: RetryDetails) =
      Logger[F].warn(show"Got error code ${status.code} when sending metadata. $retryDetails")
    def onError(err: Throwable, retryDetails: RetryDetails) =
      Logger[F].error(err)(show"Error when sending metadata. $retryDetails")

    retryingOnFailuresAndAllErrors(policy, wasSuccessful, onFailure, onError)(httpClient.status(request)).void
    // If the 10th attempt ends up in an exception, then the exception gets raised up and crashes the app.
    // We don't want that. We want to drop the request and keep going.
    .voidError
  }

  private def toTrackerProtocolEvent(
    config: Config.Metadata,
    appInfo: AppInfo,
    event: Metadata.MetadataEvent,
    entitiesAndCount: Metadata.EntitiesAndCount,
    periodStart: Instant,
    periodEnd: Instant
  ): Json = {
    val encoder = Base64.getEncoder

    val observedEvent = mkObservedEvent(config, periodStart, periodEnd, event, entitiesAndCount.count)
    val uePr = SelfDescribingData(ME.UnstructEventSchema, observedEvent.normalize).asString
    val uePx = new String(encoder.encode(uePr.getBytes(StandardCharsets.UTF_8)))

    val observedEntities = mkObservedEntities(entitiesAndCount.entities)
    val co = SelfDescribingData(ME.ContextsSchema, Json.fromValues(observedEntities.map(_.normalize))).asString
    val cx = new String(encoder.encode(co.getBytes(StandardCharsets.UTF_8)))

    Json.obj(
      "aid" -> appInfo.name.asJson,
      "e" -> "ue".asJson,
      "ue_px" -> uePx.asJson,
      "cx" -> cx.asJson
    )
  }

  private def mkObservedEvent(
    config: Config.Metadata,
    periodStart: Instant,
    periodEnd: Instant,
    event: Metadata.MetadataEvent,
    count: Int
  ): SelfDescribingData[Json] =
    SelfDescribingData(
      SchemaKey("com.snowplowanalytics.console", "observed_event", "jsonschema", SchemaVer.Full(6, 0, 1)),
      Json.obj(
        "organizationId" -> config.organizationId.asJson,
        "pipelineId" -> config.pipelineId.asJson,
        "eventVendor" -> event.schema.map(_.vendor).getOrElse("unknown-vendor").asJson,
        "eventName" -> event.schema.map(_.name).getOrElse("unknown-name").asJson,
        "eventVersion" -> event.schema.map(_.version.asString).getOrElse("unknown-version").asJson,
        "source" -> event.source.getOrElse("unknown-source").asJson,
        "tracker" -> event.tracker.getOrElse("unknown-tracker").asJson,
        "platform" -> event.platform.getOrElse("unknown-platform").asJson,
        "scenario_id" -> event.scenarioId.asJson,
        "eventVolume" -> Json.fromInt(count),
        "periodStart" -> periodStart.asJson,
        "periodEnd" -> periodEnd.asJson
      )
    )

  private def mkObservedEntities(entities: Set[SchemaKey]): Iterable[SelfDescribingData[Json]] =
    entities.view.map { entity =>
      SelfDescribingData[Json](
        SchemaKey("com.snowplowanalytics.console", "observed_entity", "jsonschema", SchemaVer.Full(4, 0, 0)),
        Json.obj("entityVendor" -> entity.vendor.asJson,
                 "entityName" -> entity.name.asJson,
                 "entityVersion" -> entity.version.asString.asJson
        )
      )
    }

  // It is safe to put the strings in the JSON directly because they have been created by circe from `Json`.
  // Special characters have been escaped.
  private def mkPayloadData(eventsAsJsonStrings: List[String]): String =
    s"""{"schema":"iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4","data":[${eventsAsJsonStrings.mkString(",")}]}"""

  private implicit def showRetryDetails: Show[RetryDetails] =
    Show {
      case RetryDetails.GivingUp(totalRetries, totalDelay) =>
        s"Giving up on retrying, total retries: $totalRetries, total delay: ${totalDelay.toSeconds} seconds"
      case RetryDetails.WillDelayAndRetry(nextDelay, retriesSoFar, cumulativeDelay) =>
        s"Will retry in ${nextDelay.toMillis} milliseconds, retries so far: $retriesSoFar, total delay so far: ${cumulativeDelay.toMillis} milliseconds"
    }
}
