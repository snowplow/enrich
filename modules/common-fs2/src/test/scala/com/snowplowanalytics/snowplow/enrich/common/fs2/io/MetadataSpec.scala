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

import java.util.UUID
import java.time.Instant
import scala.concurrent.duration._

import cats.effect.IO
import cats.Applicative
import cats.effect.kernel.Ref
import cats.effect.testing.specs2.CatsEffect
import org.http4s.Uri

import org.specs2.mutable.Specification

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Metadata => MetadataConfig}
import Metadata.{EntitiesAndCount, MetadataEvent, MetadataReporter}

class MetadataSpec extends Specification with CatsEffect {
  case class Report(
    periodStart: Instant,
    periodEnd: Instant,
    event: MetadataEvent,
    entitiesAndCount: EntitiesAndCount
  )
  case class TestReporter[F[_]: Applicative](state: Ref[F, List[Report]]) extends MetadataReporter[F] {

    def report(
      periodStart: Instant,
      periodEnd: Instant,
      event: MetadataEvent,
      entitiesAndCount: EntitiesAndCount
    ): F[Unit] =
      state.update(
        _ :+ Report(periodStart, periodEnd, event, entitiesAndCount)
      )

    def flush(): F[Unit] = Applicative[F].unit
  }

  "Metadata" should {

    "report observed events and entities" in {
      val event = MetadataSpec.enriched
      event.contexts = """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[
             {"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"39a9934a-ddd3-4581-a4ea-d0ba20e63b92"}},
             {"schema":"iglu:org.w3/PerformanceTiming/jsonschema/1-0-0","data":{"navigationStart":1581931694397,"unloadEventStart":1581931696046,"unloadEventEnd":1581931694764,"redirectStart":0,"redirectEnd":0,"fetchStart":1581931694397,"domainLookupStart":1581931694440,"domainLookupEnd":1581931694513,"connectStart":1581931694513,"connectEnd":1581931694665,"secureConnectionStart":1581931694572,"requestStart":1581931694665,"responseStart":1581931694750,"responseEnd":1581931694750,"domLoading":1581931694762,"domInteractive":1581931695963,"domContentLoadedEventStart":1581931696039,"domContentLoadedEventEnd":1581931696039,"domComplete":0,"loadEventStart":0,"loadEventEnd":0}}
           ]}"""

      val config = MetadataConfig(
        Uri.unsafeFromString("https://localhost:443"),
        50.millis,
        UUID.fromString("dfc1aef8-2656-492b-b5ba-c77702f850bc"),
        UUID.fromString("8c121fdd-dc8c-4cdc-bad1-3cefbe2b01ff")
      )

      for {
        state <- Ref.of[IO, List[Report]](List.empty)
        system <- Metadata.build[IO](config, TestReporter(state))
        _ <- system.observe(List(event))
        _ <- system.report.take(1).compile.drain
        res <- state.get
        report = res.head
      } yield {
        report.event should beEqualTo(MetadataSpec.expectedMetadaEvent)

        report.entitiesAndCount.entities should containTheSameElementsAs(
          Seq(
            SchemaKey("com.snowplowanalytics.snowplow", "web_page", "jsonschema", SchemaVer.Full(1, 0, 0)),
            SchemaKey("org.w3", "PerformanceTiming", "jsonschema", SchemaVer.Full(1, 0, 0))
          )
        )

        report.entitiesAndCount.count should beEqualTo(1)
      }
    }

    "get entities in event's contexts and find scenarioId if present" in {
      val event = new EnrichedEvent()
      event.contexts = """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[
             {"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"39a9934a-ddd3-4581-a4ea-d0ba20e63b92"}},
             {"schema":"iglu:org.w3/PerformanceTiming/jsonschema/1-0-0","data":{"navigationStart":1581931694397,"unloadEventStart":1581931696046,"unloadEventEnd":1581931694764,"redirectStart":0,"redirectEnd":0,"fetchStart":1581931694397,"domainLookupStart":1581931694440,"domainLookupEnd":1581931694513,"connectStart":1581931694513,"connectEnd":1581931694665,"secureConnectionStart":1581931694572,"requestStart":1581931694665,"responseStart":1581931694750,"responseEnd":1581931694750,"domLoading":1581931694762,"domInteractive":1581931695963,"domContentLoadedEventStart":1581931696039,"domContentLoadedEventEnd":1581931696039,"domComplete":0,"loadEventStart":0,"loadEventEnd":0}},
             {"schema": "iglu:com.snowplowanalytics.snowplow/event_specification/jsonschema/1-0-0", "data": {"id": "scenario_id"}}
           ]}"""
      val expectedEntitites =
        Seq(
          SchemaKey("com.snowplowanalytics.snowplow", "web_page", "jsonschema", SchemaVer.Full(1, 0, 0)),
          SchemaKey("org.w3", "PerformanceTiming", "jsonschema", SchemaVer.Full(1, 0, 0)),
          SchemaKey("com.snowplowanalytics.snowplow", "event_specification", "jsonschema", SchemaVer.Full(1, 0, 0))
        )
      val expectedScenarioId = Some("scenario_id")

      val (actualEntities, actualScenarioId) = Metadata.unwrapEntities(event)

      actualEntities should containTheSameElementsAs(expectedEntitites)
      actualScenarioId should beEqualTo(expectedScenarioId)
    }

    "recalculate event aggregates" should {

      "add metadata event to empty state" in {
        val enriched = MetadataSpec.enriched
        Metadata.recalculate(Map.empty, List(enriched)) should containTheSameElementsAs(
          Seq(MetadataEvent(enriched, None) -> EntitiesAndCount(Set.empty, 1))
        )
      }

      "add new metadata event to non-empty state" in {
        val enriched = MetadataSpec.enriched
        val other = MetadataSpec.enriched
        val v1_0_1 = SchemaVer.Full(1, 0, 1)
        other.event_version = v1_0_1.asString
        val previous = Map(MetadataEvent(enriched, None) -> EntitiesAndCount(Set.empty[SchemaKey], 1))
        Metadata.recalculate(previous, List(other)) should containTheSameElementsAs(
          previous.toSeq ++ Seq(MetadataEvent(other, None) -> EntitiesAndCount(Set.empty[SchemaKey], 1))
        )
      }

      "add new entity to metadata event that already has an entity" in {
        val enriched = MetadataSpec.enriched
        enriched.contexts =
          """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"39a9934a-ddd3-4581-a4ea-d0ba20e63b92"}},{"schema":"iglu:org.w3/PerformanceTiming/jsonschema/1-0-0","data":{"navigationStart":1581931694397,"unloadEventStart":1581931696046,"unloadEventEnd":1581931694764,"redirectStart":0,"redirectEnd":0,"fetchStart":1581931694397,"domainLookupStart":1581931694440,"domainLookupEnd":1581931694513,"connectStart":1581931694513,"connectEnd":1581931694665,"secureConnectionStart":1581931694572,"requestStart":1581931694665,"responseStart":1581931694750,"responseEnd":1581931694750,"domLoading":1581931694762,"domInteractive":1581931695963,"domContentLoadedEventStart":1581931696039,"domContentLoadedEventEnd":1581931696039,"domComplete":0,"loadEventStart":0,"loadEventEnd":0}}]}"""
        val entities = Set(
          SchemaKey("com.snowplowanalytics.snowplow", "web_page", "jsonschema", SchemaVer.Full(1, 0, 0)),
          SchemaKey("org.w3", "PerformanceTiming", "jsonschema", SchemaVer.Full(1, 0, 0))
        )
        val enrichedBis = MetadataSpec.enriched
        enrichedBis.contexts =
          """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-1","data":{"id":"39a9934a-ddd3-4581-a4ea-d0ba20e63b92"}}]}"""
        val entityBis = SchemaKey("com.snowplowanalytics.snowplow", "web_page", "jsonschema", SchemaVer.Full(1, 0, 1))
        val previous = Map(MetadataEvent(enriched, None) -> EntitiesAndCount(entities, 1))
        Metadata.recalculate(previous, List(enrichedBis)) should containTheSameElementsAs(
          Seq(MetadataEvent(enriched, None) -> EntitiesAndCount(entities + entityBis, 2))
        )
      }

      "add several entities from several events to an existing metadata event" in {
        val enriched = MetadataSpec.enriched
        enriched.contexts =
          """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"39a9934a-ddd3-4581-a4ea-d0ba20e63b92"}},{"schema":"iglu:org.w3/PerformanceTiming/jsonschema/1-0-0","data":{"navigationStart":1581931694397,"unloadEventStart":1581931696046,"unloadEventEnd":1581931694764,"redirectStart":0,"redirectEnd":0,"fetchStart":1581931694397,"domainLookupStart":1581931694440,"domainLookupEnd":1581931694513,"connectStart":1581931694513,"connectEnd":1581931694665,"secureConnectionStart":1581931694572,"requestStart":1581931694665,"responseStart":1581931694750,"responseEnd":1581931694750,"domLoading":1581931694762,"domInteractive":1581931695963,"domContentLoadedEventStart":1581931696039,"domContentLoadedEventEnd":1581931696039,"domComplete":0,"loadEventStart":0,"loadEventEnd":0}}]}"""
        val entities = Set(
          SchemaKey("com.snowplowanalytics.snowplow", "web_page", "jsonschema", SchemaVer.Full(1, 0, 0)),
          SchemaKey("org.w3", "PerformanceTiming", "jsonschema", SchemaVer.Full(1, 0, 0))
        )
        val enrichedBis = MetadataSpec.enriched
        enrichedBis.contexts =
          """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-1","data":{"id":"39a9934a-ddd3-4581-a4ea-d0ba20e63b92"}}]}"""
        val entityBis = SchemaKey("com.snowplowanalytics.snowplow", "web_page", "jsonschema", SchemaVer.Full(1, 0, 1))
        val enrichedTer = MetadataSpec.enriched
        enrichedTer.contexts =
          """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-2","data":{"id":"39a9934a-ddd3-4581-a4ea-d0ba20e63b92"}}]}"""
        val entityTer = SchemaKey("com.snowplowanalytics.snowplow", "web_page", "jsonschema", SchemaVer.Full(1, 0, 2))
        val previous = Map(MetadataEvent(enriched, None) -> EntitiesAndCount(entities, 1))
        Metadata.recalculate(previous, List(enrichedBis, enrichedTer)) should containTheSameElementsAs(
          Seq(MetadataEvent(enriched, None) -> EntitiesAndCount(entities + entityBis + entityTer, 3))
        )
      }
    }

    "put scenario_id in the JSON if defined" in {
      val json = Metadata
        .mkWebhookEvent(
          UUID.randomUUID(),
          UUID.randomUUID(),
          Instant.now(),
          Instant.now(),
          MetadataEvent(
            SchemaKey("com.snowplowanalytics.snowplow", "whatever", "jsonschema", SchemaVer.Full(1, 0, 2)),
            None,
            None,
            None,
            Some("hello")
          ),
          42
        )
        .toString
      json.contains("\"scenario_id\" : \"hello\",") must beTrue
    }

    "put null as scenario_id in the JSON if not defined" in {
      val json = Metadata
        .mkWebhookEvent(
          UUID.randomUUID(),
          UUID.randomUUID(),
          Instant.now(),
          Instant.now(),
          MetadataEvent(
            SchemaKey("com.snowplowanalytics.snowplow", "whatever", "jsonschema", SchemaVer.Full(1, 0, 2)),
            None,
            None,
            None,
            None
          ),
          42
        )
        .toString
      json.contains("\"scenario_id\" : null,") must beTrue
    }
  }
}

object MetadataSpec {
  val eventVendor = "com.acme"
  val eventName = "example"
  val eventFormat = "jsonschema"
  val eventVersion = SchemaVer.Full(1, 0, 0)
  val appId = "app123"
  val tracker = "js-tracker-3.0.0"
  val platform = "web"

  def enriched = {
    val enriched = new EnrichedEvent()
    enriched.event_vendor = eventVendor
    enriched.event_name = eventName
    enriched.event_format = eventFormat
    enriched.event_version = eventVersion.asString
    enriched.app_id = appId
    enriched.v_tracker = tracker
    enriched.platform = platform
    enriched
  }

  val eventSchema = SchemaKey(eventVendor, eventName, eventFormat, eventVersion)

  val expectedMetadaEvent = MetadataEvent(
    eventSchema,
    Some(appId),
    Some(tracker),
    Some(platform),
    None
  )
}
