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

import org.specs2.mutable.Specification

import cats.Applicative

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.testing.specs2.CatsEffect

import io.circe.parser.parse

import org.http4s.Uri

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Metadata => MetadataConfig}

import Metadata.{Aggregates, EntitiesAndCount, MetadataEvent, MetadataReporter}

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers

class MetadataSpec extends Specification with CatsEffect {
  case class Report(
    periodStart: Instant,
    periodEnd: Instant,
    aggregates: Aggregates
  )
  case class TestReporter[F[_]: Applicative](state: Ref[F, List[Report]]) extends MetadataReporter[F] {

    def report(
      periodStart: Instant,
      periodEnd: Instant,
      aggregates: Aggregates
    ): F[Unit] =
      state.update(
        _ :+ Report(periodStart, periodEnd, aggregates)
      )
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
        UUID.fromString("8c121fdd-dc8c-4cdc-bad1-3cefbe2b01ff"),
        150000
      )

      for {
        state <- Ref.of[IO, List[Report]](List.empty)
        system <- Metadata.build[IO](config.interval, TestReporter(state))
        _ <- system.observe(List(event))
        _ <- system.report.take(1).compile.drain
        res <- state.get
        report = res.head
      } yield report.aggregates should beEqualTo(
        Map(
          MetadataSpec.expectedMetadaEvent -> EntitiesAndCount(
            Set(
              SchemaKey("com.snowplowanalytics.snowplow", "web_page", "jsonschema", SchemaVer.Full(1, 0, 0)),
              SchemaKey("org.w3", "PerformanceTiming", "jsonschema", SchemaVer.Full(1, 0, 0))
            ),
            1
          )
        )
      )
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
        .mkObservedEvent(
          UUID.randomUUID(),
          UUID.randomUUID(),
          SpecHelpers.etlTstamp,
          SpecHelpers.etlTstamp,
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
        .mkObservedEvent(
          UUID.randomUUID(),
          UUID.randomUUID(),
          SpecHelpers.etlTstamp,
          SpecHelpers.etlTstamp,
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

    "batch up multiple metadata events inside an HTTP body" in {
      "respecting maxBodySize" in {
        def metadataEvent(i: Int) =
          MetadataEvent(
            MetadataSpec.eventSchema.copy(version = SchemaVer.Full(1, 0, i)),
            Some("app"),
            Some("js tracker"),
            Some("web"),
            Some("scenario")
          )
        val aggregates = Map(
          metadataEvent(1) -> EntitiesAndCount(Set(MetadataSpec.eventSchema), 1),
          metadataEvent(2) -> EntitiesAndCount(Set(MetadataSpec.eventSchema), 2),
          metadataEvent(3) -> EntitiesAndCount(Set(MetadataSpec.eventSchema), 3)
        )

        val appId = "test"
        val organizationId = UUID.fromString("cc6151b4-30bb-4fd9-b2ff-3ac4285eda45")
        val pipelineId = UUID.fromString("9b5afd1e-7c30-4669-87b4-26898f601cc7")
        val periodStart = Instant.ofEpochSecond(123456789)
        val periodEnd = Instant.ofEpochSecond(234567890)

        Metadata.batchUp(aggregates, appId, organizationId, pipelineId, periodStart, periodEnd, 1500) must beEqualTo(
          List(
            """{"schema":"iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4","data":[{"aid":"test","e":"ue","ue_px":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5jb25zb2xlL29ic2VydmVkX2V2ZW50L2pzb25zY2hlbWEvNi0wLTEiLCJkYXRhIjp7Im9yZ2FuaXphdGlvbklkIjoiY2M2MTUxYjQtMzBiYi00ZmQ5LWIyZmYtM2FjNDI4NWVkYTQ1IiwicGlwZWxpbmVJZCI6IjliNWFmZDFlLTdjMzAtNDY2OS04N2I0LTI2ODk4ZjYwMWNjNyIsImV2ZW50VmVuZG9yIjoiY29tLmFjbWUiLCJldmVudE5hbWUiOiJleGFtcGxlIiwiZXZlbnRWZXJzaW9uIjoiMS0wLTMiLCJzb3VyY2UiOiJhcHAiLCJ0cmFja2VyIjoianMgdHJhY2tlciIsInBsYXRmb3JtIjoid2ViIiwic2NlbmFyaW9faWQiOiJzY2VuYXJpbyIsImV2ZW50Vm9sdW1lIjozLCJwZXJpb2RTdGFydCI6IjE5NzMtMTEtMjlUMjE6MzM6MDlaIiwicGVyaW9kRW5kIjoiMTk3Ny0wNi0wN1QyMTo0NDo1MFoifX19","cx":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6W3sic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuY29uc29sZS9vYnNlcnZlZF9lbnRpdHkvanNvbnNjaGVtYS80LTAtMCIsImRhdGEiOnsiZW50aXR5VmVuZG9yIjoiY29tLmFjbWUiLCJlbnRpdHlOYW1lIjoiZXhhbXBsZSIsImVudGl0eVZlcnNpb24iOiIxLTAtMCJ9fV19"}]}""",
            """{"schema":"iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4","data":[{"aid":"test","e":"ue","ue_px":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5jb25zb2xlL29ic2VydmVkX2V2ZW50L2pzb25zY2hlbWEvNi0wLTEiLCJkYXRhIjp7Im9yZ2FuaXphdGlvbklkIjoiY2M2MTUxYjQtMzBiYi00ZmQ5LWIyZmYtM2FjNDI4NWVkYTQ1IiwicGlwZWxpbmVJZCI6IjliNWFmZDFlLTdjMzAtNDY2OS04N2I0LTI2ODk4ZjYwMWNjNyIsImV2ZW50VmVuZG9yIjoiY29tLmFjbWUiLCJldmVudE5hbWUiOiJleGFtcGxlIiwiZXZlbnRWZXJzaW9uIjoiMS0wLTIiLCJzb3VyY2UiOiJhcHAiLCJ0cmFja2VyIjoianMgdHJhY2tlciIsInBsYXRmb3JtIjoid2ViIiwic2NlbmFyaW9faWQiOiJzY2VuYXJpbyIsImV2ZW50Vm9sdW1lIjoyLCJwZXJpb2RTdGFydCI6IjE5NzMtMTEtMjlUMjE6MzM6MDlaIiwicGVyaW9kRW5kIjoiMTk3Ny0wNi0wN1QyMTo0NDo1MFoifX19","cx":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6W3sic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuY29uc29sZS9vYnNlcnZlZF9lbnRpdHkvanNvbnNjaGVtYS80LTAtMCIsImRhdGEiOnsiZW50aXR5VmVuZG9yIjoiY29tLmFjbWUiLCJlbnRpdHlOYW1lIjoiZXhhbXBsZSIsImVudGl0eVZlcnNpb24iOiIxLTAtMCJ9fV19"}]}""",
            """{"schema":"iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4","data":[{"aid":"test","e":"ue","ue_px":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5jb25zb2xlL29ic2VydmVkX2V2ZW50L2pzb25zY2hlbWEvNi0wLTEiLCJkYXRhIjp7Im9yZ2FuaXphdGlvbklkIjoiY2M2MTUxYjQtMzBiYi00ZmQ5LWIyZmYtM2FjNDI4NWVkYTQ1IiwicGlwZWxpbmVJZCI6IjliNWFmZDFlLTdjMzAtNDY2OS04N2I0LTI2ODk4ZjYwMWNjNyIsImV2ZW50VmVuZG9yIjoiY29tLmFjbWUiLCJldmVudE5hbWUiOiJleGFtcGxlIiwiZXZlbnRWZXJzaW9uIjoiMS0wLTEiLCJzb3VyY2UiOiJhcHAiLCJ0cmFja2VyIjoianMgdHJhY2tlciIsInBsYXRmb3JtIjoid2ViIiwic2NlbmFyaW9faWQiOiJzY2VuYXJpbyIsImV2ZW50Vm9sdW1lIjoxLCJwZXJpb2RTdGFydCI6IjE5NzMtMTEtMjlUMjE6MzM6MDlaIiwicGVyaW9kRW5kIjoiMTk3Ny0wNi0wN1QyMTo0NDo1MFoifX19","cx":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6W3sic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuY29uc29sZS9vYnNlcnZlZF9lbnRpdHkvanNvbnNjaGVtYS80LTAtMCIsImRhdGEiOnsiZW50aXR5VmVuZG9yIjoiY29tLmFjbWUiLCJlbnRpdHlOYW1lIjoiZXhhbXBsZSIsImVudGl0eVZlcnNpb24iOiIxLTAtMCJ9fV19"}]}"""
          )
        )
        Metadata.batchUp(aggregates, appId, organizationId, pipelineId, periodStart, periodEnd, 3000) must beEqualTo(
          List(
            """{"schema":"iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4","data":[{"aid":"test","e":"ue","ue_px":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5jb25zb2xlL29ic2VydmVkX2V2ZW50L2pzb25zY2hlbWEvNi0wLTEiLCJkYXRhIjp7Im9yZ2FuaXphdGlvbklkIjoiY2M2MTUxYjQtMzBiYi00ZmQ5LWIyZmYtM2FjNDI4NWVkYTQ1IiwicGlwZWxpbmVJZCI6IjliNWFmZDFlLTdjMzAtNDY2OS04N2I0LTI2ODk4ZjYwMWNjNyIsImV2ZW50VmVuZG9yIjoiY29tLmFjbWUiLCJldmVudE5hbWUiOiJleGFtcGxlIiwiZXZlbnRWZXJzaW9uIjoiMS0wLTMiLCJzb3VyY2UiOiJhcHAiLCJ0cmFja2VyIjoianMgdHJhY2tlciIsInBsYXRmb3JtIjoid2ViIiwic2NlbmFyaW9faWQiOiJzY2VuYXJpbyIsImV2ZW50Vm9sdW1lIjozLCJwZXJpb2RTdGFydCI6IjE5NzMtMTEtMjlUMjE6MzM6MDlaIiwicGVyaW9kRW5kIjoiMTk3Ny0wNi0wN1QyMTo0NDo1MFoifX19","cx":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6W3sic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuY29uc29sZS9vYnNlcnZlZF9lbnRpdHkvanNvbnNjaGVtYS80LTAtMCIsImRhdGEiOnsiZW50aXR5VmVuZG9yIjoiY29tLmFjbWUiLCJlbnRpdHlOYW1lIjoiZXhhbXBsZSIsImVudGl0eVZlcnNpb24iOiIxLTAtMCJ9fV19"}]}""",
            """{"schema":"iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4","data":[{"aid":"test","e":"ue","ue_px":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5jb25zb2xlL29ic2VydmVkX2V2ZW50L2pzb25zY2hlbWEvNi0wLTEiLCJkYXRhIjp7Im9yZ2FuaXphdGlvbklkIjoiY2M2MTUxYjQtMzBiYi00ZmQ5LWIyZmYtM2FjNDI4NWVkYTQ1IiwicGlwZWxpbmVJZCI6IjliNWFmZDFlLTdjMzAtNDY2OS04N2I0LTI2ODk4ZjYwMWNjNyIsImV2ZW50VmVuZG9yIjoiY29tLmFjbWUiLCJldmVudE5hbWUiOiJleGFtcGxlIiwiZXZlbnRWZXJzaW9uIjoiMS0wLTIiLCJzb3VyY2UiOiJhcHAiLCJ0cmFja2VyIjoianMgdHJhY2tlciIsInBsYXRmb3JtIjoid2ViIiwic2NlbmFyaW9faWQiOiJzY2VuYXJpbyIsImV2ZW50Vm9sdW1lIjoyLCJwZXJpb2RTdGFydCI6IjE5NzMtMTEtMjlUMjE6MzM6MDlaIiwicGVyaW9kRW5kIjoiMTk3Ny0wNi0wN1QyMTo0NDo1MFoifX19","cx":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6W3sic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuY29uc29sZS9vYnNlcnZlZF9lbnRpdHkvanNvbnNjaGVtYS80LTAtMCIsImRhdGEiOnsiZW50aXR5VmVuZG9yIjoiY29tLmFjbWUiLCJlbnRpdHlOYW1lIjoiZXhhbXBsZSIsImVudGl0eVZlcnNpb24iOiIxLTAtMCJ9fV19"},{"aid":"test","e":"ue","ue_px":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5jb25zb2xlL29ic2VydmVkX2V2ZW50L2pzb25zY2hlbWEvNi0wLTEiLCJkYXRhIjp7Im9yZ2FuaXphdGlvbklkIjoiY2M2MTUxYjQtMzBiYi00ZmQ5LWIyZmYtM2FjNDI4NWVkYTQ1IiwicGlwZWxpbmVJZCI6IjliNWFmZDFlLTdjMzAtNDY2OS04N2I0LTI2ODk4ZjYwMWNjNyIsImV2ZW50VmVuZG9yIjoiY29tLmFjbWUiLCJldmVudE5hbWUiOiJleGFtcGxlIiwiZXZlbnRWZXJzaW9uIjoiMS0wLTEiLCJzb3VyY2UiOiJhcHAiLCJ0cmFja2VyIjoianMgdHJhY2tlciIsInBsYXRmb3JtIjoid2ViIiwic2NlbmFyaW9faWQiOiJzY2VuYXJpbyIsImV2ZW50Vm9sdW1lIjoxLCJwZXJpb2RTdGFydCI6IjE5NzMtMTEtMjlUMjE6MzM6MDlaIiwicGVyaW9kRW5kIjoiMTk3Ny0wNi0wN1QyMTo0NDo1MFoifX19","cx":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6W3sic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuY29uc29sZS9vYnNlcnZlZF9lbnRpdHkvanNvbnNjaGVtYS80LTAtMCIsImRhdGEiOnsiZW50aXR5VmVuZG9yIjoiY29tLmFjbWUiLCJlbnRpdHlOYW1lIjoiZXhhbXBsZSIsImVudGl0eVZlcnNpb24iOiIxLTAtMCJ9fV19"}]}"""
          )
        )
        Metadata.batchUp(aggregates, appId, organizationId, pipelineId, periodStart, periodEnd, 4500) must beEqualTo(
          List(
            """{"schema":"iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4","data":[{"aid":"test","e":"ue","ue_px":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5jb25zb2xlL29ic2VydmVkX2V2ZW50L2pzb25zY2hlbWEvNi0wLTEiLCJkYXRhIjp7Im9yZ2FuaXphdGlvbklkIjoiY2M2MTUxYjQtMzBiYi00ZmQ5LWIyZmYtM2FjNDI4NWVkYTQ1IiwicGlwZWxpbmVJZCI6IjliNWFmZDFlLTdjMzAtNDY2OS04N2I0LTI2ODk4ZjYwMWNjNyIsImV2ZW50VmVuZG9yIjoiY29tLmFjbWUiLCJldmVudE5hbWUiOiJleGFtcGxlIiwiZXZlbnRWZXJzaW9uIjoiMS0wLTMiLCJzb3VyY2UiOiJhcHAiLCJ0cmFja2VyIjoianMgdHJhY2tlciIsInBsYXRmb3JtIjoid2ViIiwic2NlbmFyaW9faWQiOiJzY2VuYXJpbyIsImV2ZW50Vm9sdW1lIjozLCJwZXJpb2RTdGFydCI6IjE5NzMtMTEtMjlUMjE6MzM6MDlaIiwicGVyaW9kRW5kIjoiMTk3Ny0wNi0wN1QyMTo0NDo1MFoifX19","cx":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6W3sic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuY29uc29sZS9vYnNlcnZlZF9lbnRpdHkvanNvbnNjaGVtYS80LTAtMCIsImRhdGEiOnsiZW50aXR5VmVuZG9yIjoiY29tLmFjbWUiLCJlbnRpdHlOYW1lIjoiZXhhbXBsZSIsImVudGl0eVZlcnNpb24iOiIxLTAtMCJ9fV19"},{"aid":"test","e":"ue","ue_px":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5jb25zb2xlL29ic2VydmVkX2V2ZW50L2pzb25zY2hlbWEvNi0wLTEiLCJkYXRhIjp7Im9yZ2FuaXphdGlvbklkIjoiY2M2MTUxYjQtMzBiYi00ZmQ5LWIyZmYtM2FjNDI4NWVkYTQ1IiwicGlwZWxpbmVJZCI6IjliNWFmZDFlLTdjMzAtNDY2OS04N2I0LTI2ODk4ZjYwMWNjNyIsImV2ZW50VmVuZG9yIjoiY29tLmFjbWUiLCJldmVudE5hbWUiOiJleGFtcGxlIiwiZXZlbnRWZXJzaW9uIjoiMS0wLTIiLCJzb3VyY2UiOiJhcHAiLCJ0cmFja2VyIjoianMgdHJhY2tlciIsInBsYXRmb3JtIjoid2ViIiwic2NlbmFyaW9faWQiOiJzY2VuYXJpbyIsImV2ZW50Vm9sdW1lIjoyLCJwZXJpb2RTdGFydCI6IjE5NzMtMTEtMjlUMjE6MzM6MDlaIiwicGVyaW9kRW5kIjoiMTk3Ny0wNi0wN1QyMTo0NDo1MFoifX19","cx":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6W3sic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuY29uc29sZS9vYnNlcnZlZF9lbnRpdHkvanNvbnNjaGVtYS80LTAtMCIsImRhdGEiOnsiZW50aXR5VmVuZG9yIjoiY29tLmFjbWUiLCJlbnRpdHlOYW1lIjoiZXhhbXBsZSIsImVudGl0eVZlcnNpb24iOiIxLTAtMCJ9fV19"},{"aid":"test","e":"ue","ue_px":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5jb25zb2xlL29ic2VydmVkX2V2ZW50L2pzb25zY2hlbWEvNi0wLTEiLCJkYXRhIjp7Im9yZ2FuaXphdGlvbklkIjoiY2M2MTUxYjQtMzBiYi00ZmQ5LWIyZmYtM2FjNDI4NWVkYTQ1IiwicGlwZWxpbmVJZCI6IjliNWFmZDFlLTdjMzAtNDY2OS04N2I0LTI2ODk4ZjYwMWNjNyIsImV2ZW50VmVuZG9yIjoiY29tLmFjbWUiLCJldmVudE5hbWUiOiJleGFtcGxlIiwiZXZlbnRWZXJzaW9uIjoiMS0wLTEiLCJzb3VyY2UiOiJhcHAiLCJ0cmFja2VyIjoianMgdHJhY2tlciIsInBsYXRmb3JtIjoid2ViIiwic2NlbmFyaW9faWQiOiJzY2VuYXJpbyIsImV2ZW50Vm9sdW1lIjoxLCJwZXJpb2RTdGFydCI6IjE5NzMtMTEtMjlUMjE6MzM6MDlaIiwicGVyaW9kRW5kIjoiMTk3Ny0wNi0wN1QyMTo0NDo1MFoifX19","cx":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6W3sic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuY29uc29sZS9vYnNlcnZlZF9lbnRpdHkvanNvbnNjaGVtYS80LTAtMCIsImRhdGEiOnsiZW50aXR5VmVuZG9yIjoiY29tLmFjbWUiLCJlbnRpdHlOYW1lIjoiZXhhbXBsZSIsImVudGl0eVZlcnNpb24iOiIxLTAtMCJ9fV19"}]}"""
          )
        )
      }

      "creating a valid JSON that handles special characters" in {
        def metadataEvent(i: Int) =
          MetadataEvent(
            MetadataSpec.eventSchema.copy(version = SchemaVer.Full(1, 0, i)),
            Some("app{,\"}:;[']"),
            Some("tracker{,\"}:;[']"),
            Some("web"),
            Some("scenario{,\"}:;[']")
          )
        val aggregates = Map(
          metadataEvent(1) -> EntitiesAndCount(Set(MetadataSpec.eventSchema), 1),
          metadataEvent(2) -> EntitiesAndCount(Set(MetadataSpec.eventSchema), 2)
        )

        val appId = "test{,\"}:;[']"
        val organizationId = UUID.fromString("cc6151b4-30bb-4fd9-b2ff-3ac4285eda45")
        val pipelineId = UUID.fromString("9b5afd1e-7c30-4669-87b4-26898f601cc7")
        val periodStart = Instant.ofEpochSecond(123456789)
        val periodEnd = Instant.ofEpochSecond(234567890)

        Metadata.batchUp(aggregates, appId, organizationId, pipelineId, periodStart, periodEnd, 1500).map { body =>
          parse(body) must beRight
        }
      }
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
