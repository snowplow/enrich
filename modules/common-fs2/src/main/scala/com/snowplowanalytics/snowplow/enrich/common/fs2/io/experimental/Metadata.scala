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
import java.util.{Base64, UUID}
import java.nio.charset.StandardCharsets

import scala.concurrent.duration._

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.implicits._
import cats.{Applicative, Show}
import cats.kernel.Semigroup
import cats.effect.kernel.{Async, Clock, Ref, Spawn, Sync}
import cats.effect.implicits._
import fs2.Stream
import io.circe.Json
import io.circe.syntax._
import org.http4s.{MediaType, Method, Request, Status, Uri}
import org.http4s.client.Client
import org.http4s.headers.`Content-Type`
import retry.{RetryDetails, RetryPolicies, retryingOnFailuresAndAllErrors}

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.enrich.common.enrichments.{MiscEnrichments => ME}
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
    interval: FiniteDuration,
    reporter: MetadataReporter[F]
  ): F[Metadata[F]] =
    MetadataEventsRef.init[F].map { observedRef =>
      new Metadata[F] {
        def report: Stream[F, Unit] =
          for {
            _ <- Stream.eval(Logger[F].info("Starting metadata reporter"))
            _ <- Stream.bracket(Sync[F].unit)(_ => submit(reporter, observedRef))
            _ <- Stream.fixedDelay[F](interval)
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
      _ <- reporter.report(snapshot.periodStart, snapshot.periodEnd, snapshot.aggregates)
    } yield ()

  trait MetadataReporter[F[_]] {
    def report(
      periodStart: Instant,
      periodEnd: Instant,
      aggregates: Aggregates
    ): F[Unit]
  }

  case class HttpMetadataReporter[F[_]: Async](
    endpoint: Uri,
    organizationId: UUID,
    pipelineId: UUID,
    client: Client[F],
    appId: String,
    maxBodySize: Int
  ) extends MetadataReporter[F] {

    def report(
      periodStart: Instant,
      periodEnd: Instant,
      aggregates: Aggregates
    ): F[Unit] =
      Logger[F].debug(s"Sending ${aggregates.size} metadata events") >>
        batchUp(
          aggregates,
          appId,
          organizationId,
          pipelineId,
          periodStart,
          periodEnd,
          maxBodySize
        )
          .parTraverse_ { body =>
            val request = Request[F](method = Method.POST, uri = endpoint)
              .withEntity(body)
              .withContentType(`Content-Type`(MediaType.application.json))

            sendWithRetries(client, request)
          }

    implicit def showRetryDetails: Show[RetryDetails] =
      Show {
        case RetryDetails.GivingUp(totalRetries, totalDelay) =>
          s"Giving up on retrying, total retries: $totalRetries, total delay: ${totalDelay.toSeconds} seconds"
        case RetryDetails.WillDelayAndRetry(nextDelay, retriesSoFar, cumulativeDelay) =>
          s"Will retry in ${nextDelay.toMillis} milliseconds, retries so far: $retriesSoFar, total delay so far: ${cumulativeDelay.toMillis} milliseconds"
      }

    def sendWithRetries(client: Client[F], request: Request[F]): F[Unit] = {
      val policy = RetryPolicies.fibonacciBackoff[F](100.millis).join(RetryPolicies.limitRetries(10))
      def wasSuccessful(status: Status) = Sync[F].pure(status.isSuccess)
      def onFailure(status: Status, retryDetails: RetryDetails) =
        Logger[F].warn(show"Got error code ${status.code} when sending metadata. $retryDetails")
      def onError(err: Throwable, retryDetails: RetryDetails) =
        Logger[F].error(err)(show"Error when sending metadata. $retryDetails")

      retryingOnFailuresAndAllErrors(policy, wasSuccessful, onFailure, onError)(client.status(request)).void
      // If the 10th attempt ends up in an exception, then the exception gets raised up and crashes the app.
      // We don't want that. We want to drop the request and keep going.
      .voidError
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

  private[experimental] def unwrapEntities(event: EnrichedEvent): (Set[SchemaKey], Option[String]) = {
    case class Entities(schemas: Set[SchemaKey], scenarioId: Option[String])

    def unwrap(sdjs: List[SelfDescribingData[Json]]): Entities = {
      val schemas = sdjs.map(_.schema).toSet

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

  def recalculate(previous: Aggregates, events: List[EnrichedEvent]): Aggregates =
    previous |+| events.map { e =>
      val (entities, scenarioId) = unwrapEntities(e)
      Map(MetadataEvent(e, scenarioId) -> EntitiesAndCount(entities, 1))
    }.combineAll

  // It is safe to put the strings in the JSON directly because they have been created by circe from `Json`.
  // Special characters have been escaped.
  private def mkPayloadData(jsonEscapedEvents: List[String]): String =
    s"""{"schema":"iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4","data":[${jsonEscapedEvents.mkString(",")}]}"""

  private def mkMetadataEvent(
    appId: String,
    organizationId: UUID,
    pipelineId: UUID,
    periodStart: Instant,
    periodEnd: Instant,
    event: MetadataEvent,
    entities: Seq[SchemaKey],
    count: Int
  ): Json = {
    val encoder = Base64.getEncoder

    val observedEvent = mkObservedEvent(organizationId, pipelineId, periodStart, periodEnd, event, count)
    val uePr = SelfDescribingData(ME.UnstructEventSchema, observedEvent.normalize).asString
    val uePx = new String(encoder.encode(uePr.getBytes(StandardCharsets.UTF_8)))

    val observedEntities = mkObservedEntities(entities)
    val co = SelfDescribingData(ME.ContextsSchema, Json.fromValues(observedEntities.map(_.normalize))).asString
    val cx = new String(encoder.encode(co.getBytes(StandardCharsets.UTF_8)))

    Json.obj(
      "aid" -> appId.asJson,
      "e" -> "ue".asJson,
      "ue_px" -> uePx.asJson,
      "cx" -> cx.asJson
    )
  }

  private[experimental] def mkObservedEvent(
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

  private[experimental] def mkObservedEntities(entities: Seq[SchemaKey]): Seq[SelfDescribingData[Json]] =
    entities.map(entity =>
      SelfDescribingData[Json](
        SchemaKey("com.snowplowanalytics.console", "observed_entity", "jsonschema", SchemaVer.Full(4, 0, 0)),
        Json.obj("entityVendor" -> entity.vendor.asJson,
                 "entityName" -> entity.name.asJson,
                 "entityVersion" -> entity.version.asString.asJson
        )
      )
    )

  // Returns a list of bodies ready to be put inside HTTP requests,
  // where each body contains multiple metadata events
  private[experimental] def batchUp(
    aggregates: Aggregates,
    appId: String,
    organizationId: UUID,
    pipelineId: UUID,
    periodStart: Instant,
    periodEnd: Instant,
    maxBytes: Int
  ): List[String] = {
    val stringified = aggregates
      .map {
        case (event, entitiesAndCount) =>
          mkMetadataEvent(
            appId,
            organizationId,
            pipelineId,
            periodStart,
            periodEnd,
            event,
            entitiesAndCount.entities.toSeq,
            entitiesAndCount.count
          ).noSpaces
      }

    case class Body(weight: Int, events: List[String])

    stringified
      .foldLeft(List.empty[Body]) {
        case (bodies, event) =>
          bodies match {
            case Nil =>
              List(Body(event.length, List(event)))
            case head :: tail if head.weight + event.length < maxBytes =>
              Body(head.weight + event.length, event :: head.events) :: tail
            case other =>
              Body(event.length, List(event)) :: other
          }

      }
      .map(body => mkPayloadData(body.events))
  }
}
