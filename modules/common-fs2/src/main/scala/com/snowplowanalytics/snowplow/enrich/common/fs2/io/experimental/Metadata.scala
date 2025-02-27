/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.io.experimental

import java.time.Instant
import java.util.UUID

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.implicits._
import cats.Applicative
import cats.data.NonEmptyList
import cats.kernel.Semigroup
import cats.effect.kernel.{Async, Clock, Ref, Resource, Spawn, Sync}
import cats.effect.std.Random
import fs2.Stream
import io.circe.Json
import io.circe.parser._
import io.circe.syntax._
import org.http4s.Uri
import org.http4s.client.Client

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.snowplow.scalatracker.{Emitter, Tracker}
import com.snowplowanalytics.snowplow.scalatracker.emitters.http4s.{Http4sEmitter, ceTracking}

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
  def observe(events: List[EnrichedEvent]): F[Unit]
}

object Metadata {
  type Aggregates = Map[MetadataEvent, EntitiesAndCount]
  case class EntitiesAndCount(entities: Set[SchemaKey], count: Int)

  implicit private def entitiesAndCountSemigroup: Semigroup[EntitiesAndCount] =
    new Semigroup[EntitiesAndCount] {
      override def combine(x: EntitiesAndCount, y: EntitiesAndCount): EntitiesAndCount =
        EntitiesAndCount(
          x.entities |+| y.entities,
          x.count + y.count
        )
    }

  case class EventSpecInfo(
    schemaVendor: String,
    schemaName: String,
    field: String
  )
  private val eventSpecInfo = EventSpecInfo("com.snowplowanalytics.snowplow", "event_specification", "id")

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def build[F[_]: Async](
    config: MetadataConfig,
    reporter: MetadataReporter[F]
  ): F[Metadata[F]] =
    MetadataEventsRef.init[F].map { observedRef =>
      new Metadata[F] {
        def report: Stream[F, Unit] =
          for {
            _ <- Stream.eval(Logger[F].info("Starting metadata reporter"))
            _ <- Stream.bracket(Sync[F].unit)(_ => submit(reporter, observedRef))
            _ <- Stream.fixedDelay[F](config.interval)
            _ <- Stream.eval(submit(reporter, observedRef))
          } yield ()

        def observe(events: List[EnrichedEvent]): F[Unit] =
          observedRef.aggregates.update(recalculate(_, events))
      }
    }

  def noop[F[_]: Spawn]: Metadata[F] =
    new Metadata[F] {
      def report: Stream[F, Unit] = Stream.never[F]
      def observe(events: List[EnrichedEvent]): F[Unit] = Applicative[F].unit
    }

  private def submit[F[_]: Sync](reporter: MetadataReporter[F], ref: MetadataEventsRef[F]): F[Unit] =
    for {
      snapshot <- MetadataEventsRef.snapshot(ref)
      _ <- snapshot.aggregates.toList.traverse {
             case (event, entitiesAndCount) =>
               reporter.report(snapshot.periodStart, snapshot.periodEnd, event, entitiesAndCount)
           }
      _ <- reporter.flush()
    } yield ()

  trait MetadataReporter[F[_]] {
    def report(
      periodStart: Instant,
      periodEnd: Instant,
      event: MetadataEvent,
      entitiesAndCount: EntitiesAndCount
    ): F[Unit]

    def flush(): F[Unit]
  }

  case class HttpMetadataReporter[F[_]: Sync](
    config: MetadataConfig,
    tracker: Tracker[F]
  ) extends MetadataReporter[F] {

    def report(
      periodStart: Instant,
      periodEnd: Instant,
      event: MetadataEvent,
      entitiesAndCount: EntitiesAndCount
    ): F[Unit] =
      Logger[F].debug(s"Tracking observed event ${event.schema.toSchemaUri}") >>
        tracker.trackSelfDescribingEvent(
          mkWebhookEvent(config.organizationId, config.pipelineId, periodStart, periodEnd, event, entitiesAndCount.count),
          mkWebhookContexts(entitiesAndCount.entities).toSeq
        )

    def flush(): F[Unit] = tracker.flushEmitters()
  }

  object HttpMetadataReporter {
    def resource[F[_]: Async](
      config: MetadataConfig,
      appName: String,
      client: Client[F]
    ): Resource[F, HttpMetadataReporter[F]] =
      initTracker(config, appName, client).map(t => HttpMetadataReporter(config, t))

    private def initTracker[F[_]: Async](
      config: MetadataConfig,
      appName: String,
      client: Client[F]
    ): Resource[F, Tracker[F]] =
      for {
        implicit0(random: Random[F]) <- Resource.eval(Random.scalaUtilRandom[F])
        emitter <- Http4sEmitter.build(
                     Emitter.EndpointParams(
                       config.endpoint.host.map(_.toString()).getOrElse("localhost"),
                       config.endpoint.port,
                       https = config.endpoint.scheme.map(_ == Uri.Scheme.https).getOrElse(false)
                     ),
                     client,
                     bufferConfig = Emitter.BufferConfig.PayloadSize(100000),
                     retryPolicy = Emitter.RetryPolicy.MaxAttempts(10),
                     callback = Some(emitterCallback[F](_, _, _))
                   )
      } yield new Tracker(NonEmptyList.of(emitter), "tracker-metadata", appName)

    private def emitterCallback[F[_]: Sync](
      params: Emitter.EndpointParams,
      req: Emitter.Request,
      res: Emitter.Result
    ): F[Unit] =
      res match {
        case Emitter.Result.Success(_) =>
          Logger[F].debug(s"Metadata successfully sent to ${params.getUri}")
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
   * @param platform - The platform the app runs on for given event (`platform` field)
   * @param scenarioId - Identifier for the tracking scenario the event is being tracked for
   */
  case class MetadataEvent(
    schema: SchemaKey,
    source: Option[String],
    tracker: Option[String],
    platform: Option[String],
    scenarioId: Option[String]
  )
  object MetadataEvent {
    def apply(event: EnrichedEvent, scenarioId: Option[String]): MetadataEvent =
      MetadataEvent(
        SchemaKey(
          Option(event.event_vendor).getOrElse("unknown-vendor"),
          Option(event.event_name).getOrElse("unknown-name"),
          Option(event.event_format).getOrElse("unknown-format"),
          Option(event.event_version).toRight("unknown-version").flatMap(SchemaVer.parseFull).getOrElse(SchemaVer.Full(0, 0, 0))
        ),
        Option(event.app_id),
        Option(event.v_tracker),
        Option(event.platform),
        scenarioId
      )
  }

  /**
   * An representation of observed metadata events and entites attached to them over a period of time
   *
   * @param aggregates - mappings of entities observed (since `periodStart`) for given `MetadataEvent`s
   * @param periodStart - since when `aggregates` are accumulated
   * @param periodEnd - until when `aggregates` are accumulated
   */
  case class MetadataSnapshot(
    aggregates: Aggregates,
    periodStart: Instant,
    periodEnd: Instant
  )

  /**
   * Internal state representation for current metadata period
   * @param aggregates - mappings of entities observed (since `periodStart`) for given `MetadataEvent`s
   * @param periodStart - since when `aggregates` are accumulated
   */
  case class MetadataEventsRef[F[_]](
    aggregates: Ref[F, Aggregates],
    periodStart: Ref[F, Instant]
  )

  object MetadataEventsRef {
    def init[F[_]: Sync]: F[MetadataEventsRef[F]] =
      for {
        time <- Clock[F].realTimeInstant
        aggregates <- Ref.of[F, Aggregates](Map.empty)
        periodStart <- Ref.of[F, Instant](time)
      } yield MetadataEventsRef(aggregates, periodStart)
    def snapshot[F[_]: Sync](ref: MetadataEventsRef[F]): F[MetadataSnapshot] =
      for {
        periodEnd <- Clock[F].realTimeInstant
        aggregates <- ref.aggregates.getAndSet(Map.empty)
        periodStart <- ref.periodStart.getAndSet(periodEnd)
      } yield MetadataSnapshot(aggregates, periodStart, periodEnd)
  }

  def unwrapEntities(event: EnrichedEvent): (Set[SchemaKey], Option[String]) = {
    case class Entities(schemas: Set[SchemaKey], scenarioId: Option[String])

    def unwrap(str: String): Entities = {
      val sdjs = decode[SelfDescribingData[Json]](str)
        .traverse(
          _.data
            .as[List[SelfDescribingData[Json]]]
            .sequence
            .flatMap(_.toList)
        )
        .flatMap(_.toList)
        .toSet

      val schemas = sdjs.map(_.schema)

      val scenarioId = sdjs.collectFirst {
        case sdj if sdj.schema.vendor == eventSpecInfo.schemaVendor && sdj.schema.name == eventSpecInfo.schemaName =>
          sdj.data.hcursor.downField(eventSpecInfo.field).as[String] match {
            case Right(scenarioId) =>
              Some(scenarioId)
            case _ =>
              None
          }
      }.flatten

      Entities(schemas, scenarioId)
    }

    val entities = unwrap(event.contexts)
    val schemas = entities.schemas ++ unwrap(event.derived_contexts).schemas

    (schemas, entities.scenarioId)
  }

  def schema(event: EnrichedEvent): SchemaKey =
    SchemaKey(
      Option(event.event_vendor).getOrElse("unknown-vendor"),
      Option(event.event_name).getOrElse("unknown-name"),
      Option(event.event_format).getOrElse("unknown-format"),
      SchemaVer.parseFull(event.event_version).getOrElse(SchemaVer.Full(0, 0, 0))
    )

  def recalculate(previous: Aggregates, events: List[EnrichedEvent]): Aggregates =
    previous |+| events.map { e =>
      val (entities, scenarioId) = unwrapEntities(e)
      Map(MetadataEvent(e, scenarioId) -> EntitiesAndCount(entities, 1))
    }.combineAll

  def mkWebhookEvent(
    organizationId: UUID,
    pipelineId: UUID,
    periodStart: Instant,
    periodEnd: Instant,
    event: MetadataEvent,
    count: Int
  ): SelfDescribingData[Json] =
    SelfDescribingData(
      SchemaKey("com.snowplowanalytics.console", "observed_event", "jsonschema", SchemaVer.Full(6, 0, 1)),
      Json.obj(
        "organizationId" -> organizationId.asJson,
        "pipelineId" -> pipelineId.asJson,
        "eventVendor" -> event.schema.vendor.asJson,
        "eventName" -> event.schema.name.asJson,
        "eventVersion" -> event.schema.version.asString.asJson,
        "source" -> event.source.getOrElse("unknown-source").asJson,
        "tracker" -> event.tracker.getOrElse("unknown-tracker").asJson,
        "platform" -> event.platform.getOrElse("unknown-platform").asJson,
        "scenario_id" -> event.scenarioId.asJson,
        "eventVolume" -> Json.fromInt(count),
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
