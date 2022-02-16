/*
 * Copyright (c) 2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.fs2.io.experimental

import java.time.Instant
import java.util.UUID

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.implicits._
import cats.Applicative
import cats.data.NonEmptyList
import cats.effect.{Async, Clock, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.effect.concurrent.Ref
import fs2.Stream
import io.circe.Json
import io.circe.parser._
import io.circe.syntax._
import org.http4s.Uri
import org.http4s.client.Client

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.snowplow.scalatracker.{Emitter, Tracker}
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.Http4sEmitter
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Metadata => MetadataConfig}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

/**
 * EXPERIMENTAL: This code is subject to change or being removed
 *
 * Aggregate relationships between events and entities
 * observed over a given period of time.
 */
trait Metadata[F[_]] {
  def report: Stream[F, Unit]
  def submit: Stream[F, Unit]
  def observe(event: EnrichedEvent): F[Unit]
}

object Metadata {
  type EventsToEntities = Map[MetadataEvent, Set[SchemaKey]]

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def build[F[_]: ContextShift: ConcurrentEffect: Timer](
    config: MetadataConfig,
    reporter: MetadataReporter[F]
  ): F[Metadata[F]] =
    MetadataEventsRef.init[F].map { observedRef =>
      new Metadata[F] {
        def report: Stream[F, Unit] =
          for {
            _ <- Stream.eval(Logger[F].info("Starting metadata repoter"))
            _ <- Stream.fixedDelay[F](config.interval)
            _ <- submit
          } yield ()

        def submit: Stream[F, Unit] =
          for {
            snapshot <- Stream.eval(MetadataEventsRef.snapshot(observedRef))
            _ <- Stream
                   .emits[F, MetadataEvent](snapshot.eventsToEntities.keySet.toSeq)
                   .evalMap[F, Unit](reporter.report(snapshot.periodStart, snapshot.periodEnd)(_, snapshot.eventsToEntities))
          } yield ()

        def observe(event: EnrichedEvent): F[Unit] =
          observedRef.eventsToEntities.update(recalculate(_, event))
      }
    }

  def noop[F[_]: Async]: Metadata[F] =
    new Metadata[F] {
      def report: Stream[F, Unit] = Stream.never[F]
      def submit: Stream[F, Unit] = Stream.never[F]
      def observe(event: EnrichedEvent): F[Unit] = Applicative[F].unit
    }

  trait MetadataReporter[F[_]] {
    def report(
      periodStart: Instant,
      periodEnd: Instant
    )(
      snapshot: MetadataEvent,
      eventsToEntities: EventsToEntities
    ): F[Unit]
  }

  case class HttpMetadataReporter[F[_]: ConcurrentEffect: Timer](
    config: MetadataConfig,
    appName: String,
    client: Client[F]
  ) extends MetadataReporter[F] {
    def initTracker(
      config: MetadataConfig,
      appName: String,
      client: Client[F]
    ): Resource[F, Tracker[F]] =
      for {
        emitter <- Http4sEmitter.build(
                     Emitter.EndpointParams(
                       config.endpoint.host.map(_.toString()).getOrElse("localhost"),
                       config.endpoint.port,
                       https = config.endpoint.scheme.map(_ == Uri.Scheme.https).getOrElse(false)
                     ),
                     client,
                     retryPolicy = Emitter.RetryPolicy.MaxAttempts(10),
                     callback = Some(emitterCallback _)
                   )
      } yield new Tracker(NonEmptyList.of(emitter), "tracker-metadata", appName)

    def report(
      periodStart: Instant,
      periodEnd: Instant
    )(
      event: MetadataEvent,
      eventsToEntities: EventsToEntities
    ): F[Unit] =
      initTracker(config, appName, client).use { t =>
        Logger[F].info(s"Tracking observed event ${event.schema.toSchemaUri}") >>
          t.trackSelfDescribingEvent(
            mkWebhookEvent(config.organizationId, config.pipelineId, periodStart, periodEnd, event),
            eventsToEntities.find(_._1 == event).map(_._2).toSeq.flatMap(mkWebhookContexts)
          ) >> t.flushEmitters()
      }

    private def emitterCallback(
      params: Emitter.EndpointParams,
      req: Emitter.Request,
      res: Emitter.Result
    ): F[Unit] =
      res match {
        case Emitter.Result.Success(_) =>
          Logger[F].info(s"Metadata successfully sent to ${params.getGetUri}")
        case Emitter.Result.Failure(code) =>
          Logger[F].warn(s"Sending metadata got unexpected HTTP code $code from ${params.getUri}")
        case Emitter.Result.TrackerFailure(exception) =>
          Logger[F].warn(
            s"Metadata failed to reach ${params.getUri} with following exception $exception after ${req.attempt} attempts"
          )
        case Emitter.Result.RetriesExceeded(failure) =>
          Logger[F].error(s"Stopped trying to send metadata after following failure: $failure")
      }
  }

  /**
   * A metadata domain representation of an enriched event
   *
   * @param schema - schema key of given event
   * @param source - `app_id` for given event
   * @param tracker - `v_tracker` for given event
   */
  case class MetadataEvent(
    schema: SchemaKey,
    source: Option[String],
    tracker: Option[String]
  )
  object MetadataEvent {
    def apply(event: EnrichedEvent): MetadataEvent =
      MetadataEvent(
        SchemaKey(
          Option(event.event_vendor).getOrElse("unknown-vendor"),
          Option(event.event_name).getOrElse("unknown-name"),
          Option(event.event_format).getOrElse("unknown-format"),
          Option(event.event_version).toRight("unknown-version").flatMap(SchemaVer.parseFull).getOrElse(SchemaVer.Full(0, 0, 0))
        ),
        Option(event.app_id),
        Option(event.v_tracker)
      )
  }

  /**
   * An representation of observed metadata events and entites attached to them over a period of time
   *
   * @param eventsToEntities - mappings of entities observed (since `periodStart`) for given `MetadataEvent`s
   * @param periodStart - since when `eventsToEntities` are accumulated
   * @param periodEnd - until when `eventsToEntities` are accumulated
   */
  case class MetadataSnapshot(
    eventsToEntities: EventsToEntities,
    periodStart: Instant,
    periodEnd: Instant
  )

  /**
   * Internal state representation for current metadata period
   * @param eventsToEntities - mappings of entities observed (since `periodStart`) for given `MetadataEvent`s
   * @param periodStart - since when `eventsToEntities` are accumulated
   */
  case class MetadataEventsRef[F[_]: Sync](
    eventsToEntities: Ref[F, EventsToEntities],
    periodStart: Ref[F, Instant]
  )

  object MetadataEventsRef {
    def init[F[_]: Sync: Clock] =
      for {
        time <- Clock[F].instantNow
        eventsToEntities <- Ref.of[F, EventsToEntities](Map.empty)
        periodStart <- Ref.of[F, Instant](time)
      } yield MetadataEventsRef(eventsToEntities, periodStart)
    def snapshot[F[_]: Sync: Clock](ref: MetadataEventsRef[F]) =
      for {
        periodEnd <- Clock[F].instantNow
        eventsToEntities <- ref.eventsToEntities.getAndSet(Map.empty)
        periodStart <- ref.periodStart.getAndSet(periodEnd)
      } yield MetadataSnapshot(eventsToEntities, periodStart, periodEnd)
  }

  def unwrapEntities(event: EnrichedEvent): Set[SchemaKey] = {
    def unwrap(str: String) =
      decode[SelfDescribingData[Json]](str)
        .traverse(
          _.data
            .as[List[SelfDescribingData[Json]]]
            .traverse(_.map(_.schema))
            .flatMap(_.toList)
        )
        .flatMap(_.toList)
        .toSet

    unwrap(event.contexts) ++ unwrap(event.derived_contexts)
  }

  def schema(event: EnrichedEvent): SchemaKey =
    SchemaKey(
      Option(event.event_vendor).getOrElse("unknown-vendor"),
      Option(event.event_name).getOrElse("unknown-name"),
      Option(event.event_format).getOrElse("unknown-format"),
      SchemaVer.parseFull(event.event_version).getOrElse(SchemaVer.Full(0, 0, 0))
    )

  def recalculate(previous: EventsToEntities, event: EnrichedEvent): EventsToEntities =
    previous ++ Map(MetadataEvent(event) -> unwrapEntities(event))

  def mkWebhookEvent(
    organizationId: UUID,
    pipelineId: UUID,
    periodStart: Instant,
    periodEnd: Instant,
    event: MetadataEvent
  ): SelfDescribingData[Json] =
    SelfDescribingData(
      SchemaKey("com.snowplowanalytics.console", "observed_event", "jsonschema", SchemaVer.Full(4, 0, 0)),
      Json.obj(
        "organizationId" -> organizationId.asJson,
        "pipelineId" -> pipelineId.asJson,
        "eventVendor" -> event.schema.vendor.asJson,
        "eventName" -> event.schema.name.asJson,
        "eventVersion" -> event.schema.version.asString.asJson,
        "source" -> event.source.getOrElse("unknown-source").asJson,
        "tracker" -> event.tracker.getOrElse("unknown-tracker").asJson,
        "periodStart" -> periodStart.asJson,
        "periodEnd" -> periodEnd.asJson
      )
    )

  def mkWebhookContexts(entities: Set[SchemaKey]): Set[SelfDescribingData[Json]] =
    entities.map(entity =>
      SelfDescribingData[Json](
        SchemaKey("com.snowplowanalytics.console", "observed_entity", "jsonschema", SchemaVer.Full(4, 0, 0)),
        Json.obj("entityVendor" -> entity.vendor.asJson,
                 "entityName" -> entity.name.asJson,
                 "entityVersion" -> entity.version.asString.asJson
        )
      )
    )
}
