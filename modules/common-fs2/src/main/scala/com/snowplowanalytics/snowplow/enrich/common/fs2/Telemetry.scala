/*
 * Copyright (c) 2021-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.data.NonEmptyList
import cats.implicits._

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Random

import fs2.Stream

import org.http4s.client.{Client => HttpClient}

import _root_.io.circe.Json
import _root_.io.circe.syntax._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.scalatracker.Tracker
import com.snowplowanalytics.snowplow.scalatracker.Emitter._
import com.snowplowanalytics.snowplow.scalatracker.Emitter.{Result => TrackerResult}
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.{Http4sEmitter, ceTracking}

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Telemetry => TelemetryConfig}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Cloud

object Telemetry {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def run[F[_]: Async, A](env: Environment[F, A]): Stream[F, Unit] =
    env.telemetryConfig.disable match {
      case true =>
        Stream.empty.covary[F]
      case _ =>
        val sdj = makeHeartbeatEvent(
          env.telemetryConfig,
          env.region,
          env.cloud,
          env.processor.artifact,
          env.processor.version
        )
        val tracker = initTracker(env.telemetryConfig, env.processor.artifact, env.httpClient)
        Stream
          .fixedDelay[F](env.telemetryConfig.interval)
          .evalMap { _ =>
            tracker.use { t =>
              t.trackSelfDescribingEvent(unstructEvent = sdj) >>
                t.flushEmitters()
            }
          }
    }

  private def initTracker[F[_]: Async](
    config: TelemetryConfig,
    appName: String,
    client: HttpClient[F]
  ): Resource[F, Tracker[F]] =
    for {
      implicit0(random: Random[F]) <- Resource.eval(Random.scalaUtilRandom[F])
      emitter <- Http4sEmitter.build(
                   EndpointParams(config.collectorUri, port = Some(config.collectorPort), https = config.secure),
                   client,
                   retryPolicy = RetryPolicy.MaxAttempts(10),
                   callback = Some(emitterCallback[F] _)
                 )
    } yield new Tracker(NonEmptyList.of(emitter), "tracker-telemetry", appName)

  private def emitterCallback[F[_]: Sync](
    params: EndpointParams,
    req: Request,
    res: TrackerResult
  ): F[Unit] =
    res match {
      case TrackerResult.Success(_) =>
        Logger[F].debug(s"Telemetry heartbeat successfully sent to ${params.getGetUri}")
      case TrackerResult.Failure(code) =>
        Logger[F].warn(s"Sending telemetry hearbeat got unexpected HTTP code $code from ${params.getUri}")
      case TrackerResult.TrackerFailure(exception) =>
        Logger[F].warn(
          s"Telemetry hearbeat failed to reach ${params.getUri} with following exception $exception after ${req.attempt} attempts"
        )
      case TrackerResult.RetriesExceeded(failure) =>
        Logger[F].error(s"Stopped trying to send telemetry heartbeat after following failure: $failure")
    }

  private def makeHeartbeatEvent(
    teleCfg: TelemetryConfig,
    region: Option[String],
    cloud: Option[Cloud],
    appName: String,
    appVersion: String
  ): SelfDescribingData[Json] =
    SelfDescribingData(
      SchemaKey("com.snowplowanalytics.oss", "oss_context", "jsonschema", SchemaVer.Full(1, 0, 1)),
      Json.obj(
        "userProvidedId" -> teleCfg.userProvidedId.asJson,
        "autoGeneratedId" -> teleCfg.autoGeneratedId.asJson,
        "moduleName" -> teleCfg.moduleName.asJson,
        "moduleVersion" -> teleCfg.moduleVersion.asJson,
        "instanceId" -> teleCfg.instanceId.asJson,
        "appGeneratedId" -> java.util.UUID.randomUUID.toString.asJson,
        "cloud" -> cloud.toString.toUpperCase().asJson,
        "region" -> region.asJson,
        "applicationName" -> appName.asJson,
        "applicationVersion" -> appVersion.asJson
      )
    )
}
