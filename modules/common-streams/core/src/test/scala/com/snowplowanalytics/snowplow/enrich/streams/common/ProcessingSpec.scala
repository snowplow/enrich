/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.streams.common

import java.util.{Base64, UUID}
import java.time.Instant
import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.duration._

import org.specs2.Specification

import io.circe.parser

import cats.implicits._

import cats.effect.IO

import fs2.Chunk

import cats.implicits._

import cats.effect.testkit.TestControl

import cats.effect.testing.specs2.CatsEffect

import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.streams.TokenedEvents

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.{IpLookupsEnrichment, JavascriptScriptEnrichment}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest.ApiRequestEnrichment

import MockEnvironment._
import EventUtils._

class ProcessingSpec extends Specification with CatsEffect {
  import ProcessingSpec._

  def is = s2"""
  Enrich should:
    Process the events, update the metrics and checkpoint the input $e1
    Emit failed events and bad rows for invalid events $e2
    Emit bad rows for malformed events $e3
    Allow Javascript enrichment to drop events $e4
    Enrich with IP lookups and API enrichments $e5
  """

  def e1 = {
    val eventId = UUID.randomUUID

    val input = List(GoodBatch(List.fill(2)(pageView(eventId))))

    val expectedEnriched =
      List.fill(2)(
        Enriched(
          expectedPageView(eventId),
          None,
          Map("app_id" -> "test_app")
        )
      )

    val expectedMetadata = {
      val entities = Metadata.EntitiesAndCount(Set.empty, 2)
      Map(expectedPageViewMetadata -> entities)
    }

    val io = runTest(etlTstamp, input) {
      case (input, control) =>
        for {
          _ <- Processing.stream(control.environment).compile.drain
          state <- control.state.get
          tokens = input.map(_.ack)
        } yield state should beEqualTo(
          Vector(
            Action.AddedRawCountMetric(2),
            Action.AddedMetadata(expectedMetadata),
            Action.SentToEnriched(expectedEnriched),
            Action.AddedEnrichedCountMetric(2),
            Action.SetE2ELatencyMetric(Duration(5, MINUTES)),
            Action.Checkpointed(tokens)
          )
        )
    }

    TestControl.executeEmbed(io)
  }

  def e2 = {
    val eventId = UUID.randomUUID

    val input = List(GoodBatch(List(invalidAddToCart(eventId))))

    val expectedFailed =
      List(
        Failed(
          expectedFailedSV(eventId),
          None,
          Map.empty
        )
      )

    val expectedBad =
      List(
        Bad(
          expectedBadSV(eventId),
          None,
          Map.empty
        )
      )

    val io = runTest(etlTstamp, input) {
      case (input, control) =>
        for {
          _ <- Processing.stream(control.environment).compile.drain
          state <- control.state.get
          tokens = input.map(_.ack)
        } yield state should beEqualTo(
          Vector(
            Action.AddedRawCountMetric(1),
            Action.SetE2ELatencyMetric(Duration(5, MINUTES)),
            Action.SentToFailed(expectedFailed),
            Action.AddedFailedCountMetric(1),
            Action.SentToBad(expectedBad),
            Action.AddedBadCountMetric(1),
            Action.Checkpointed(tokens)
          )
        )
    }

    TestControl.executeEmbed(io)
  }

  def e3 = {
    val input = List(BadBatch(Chunk("nonsense", "nonsense2")))

    val expectedBad =
      List(
        Bad(
          expectedBadCPF("bm9uc2Vuc2U="),
          None,
          Map.empty
        ),
        Bad(
          expectedBadCPF("bm9uc2Vuc2Uy"),
          None,
          Map.empty
        )
      )

    val io = runTest(etlTstamp, input) {
      case (input, control) =>
        for {
          _ <- Processing.stream(control.environment).compile.drain
          state <- control.state.get
          tokens = input.map(_.ack)
        } yield state should beEqualTo(
          Vector(
            Action.AddedRawCountMetric(2),
            Action.SentToBad(expectedBad),
            Action.AddedBadCountMetric(2),
            Action.Checkpointed(tokens)
          )
        )
    }

    TestControl.executeEmbed(io)
  }

  def e4 = {
    val eventId = UUID.randomUUID

    val input = List(GoodBatch(List(pageView(eventId), pageView(eventId, appID = "drop"))))

    val expectedEnriched =
      List(
        Enriched(
          expectedPageView(eventId),
          None,
          Map("app_id" -> "test_app")
        )
      )

    val expectedMetadata = {
      val entities = Metadata.EntitiesAndCount(Set.empty, 1)
      Map(expectedPageViewMetadata -> entities)
    }

    val script = """
      function process(event, params) {
        if(event.app_id == "drop") {
          event.drop();
        } else {
          return [ ];
        }
      }""".getBytes(UTF_8)
    val config = parser
      .parse(s"""{
      "parameters": {
        "script": "${Base64.getEncoder.encodeToString(script)}"
      }
    }""")
      .toOption
      .get
    val schemaKey = SchemaKey(
      "com.snowplowanalytics.snowplow",
      "javascript_script_config",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )
    val jsEnrichConf =
      JavascriptScriptEnrichment.parse(config, schemaKey).toOption.get

    val io = runTest(etlTstamp, input, List(jsEnrichConf)) {
      case (input, control) =>
        for {
          _ <- Processing.stream(control.environment).compile.drain
          state <- control.state.get
          tokens = input.map(_.ack)
        } yield state should beEqualTo(
          Vector(
            Action.AddedRawCountMetric(2),
            Action.AddedDroppedCountMetric(1),
            Action.AddedMetadata(expectedMetadata),
            Action.SentToEnriched(expectedEnriched),
            Action.AddedEnrichedCountMetric(1),
            Action.SetE2ELatencyMetric(Duration(5, MINUTES)),
            Action.Checkpointed(tokens)
          )
        )
    }

    TestControl.executeEmbed(io)
  }

  def e5 = {
    val eventId = UUID.randomUUID

    val quantity = 2

    val input = List(GoodBatch(List(pageView(eventId, tiQuantity = Some(quantity)))))

    val expectedContext = addToCartSDD(quantity)

    def expectedEnriched(etlTstamp: Option[Instant]) =
      List(
        Enriched(
          expectedPageView(eventId).copy(
            etl_tstamp = etlTstamp,
            geo_country = Some("CN"),
            geo_region = Some("22"),
            geo_city = Some("Fuyu"),
            geo_latitude = Some(43.88),
            geo_longitude = Some(125.3228),
            geo_region_name = Some("Jilin Sheng"),
            geo_timezone = Some("Asia/Harbin"),
            ti_quantity = Some(quantity),
            derived_contexts = SnowplowEvent.Contexts(List(expectedContext))
          ),
          None,
          Map("app_id" -> "test_app")
        )
      )

    val expectedMetadata = {
      val entities = Metadata.EntitiesAndCount(Set(expectedContext.schema), 1)
      Map(expectedPageViewMetadata -> entities)
    }

    val ipLookupsConf = IpLookupsEnrichment
      .parse(
        parser
          .parse(s"""
        {
          "name": "ip_lookups",
          "vendor": "com.snowplowanalytics.snowplow",
          "enabled": true,
          "parameters": {
            "geo": {
              "database": "GeoIP2-City.mmdb",
              "uri": "http://localhost:${HttpServer.port}/maxmind"
            }
          }
        }""")
          .toOption
          .get,
        SchemaKey(
          "com.snowplowanalytics.snowplow",
          "ip_lookups",
          "jsonschema",
          SchemaVer.Full(2, 0, 0)
        ),
        false
      )
      .toOption
      .get

    val apiRequestConf = ApiRequestEnrichment
      .parse(
        parser
          .parse(s"""
        {
          "vendor": "com.snowplowanalytics.snowplow.enrichments",
          "name": "api_request_enrichment_config",
          "enabled": true,
          "parameters": {
            "inputs": [
            {
              "key": "quantity",
              "pojo": {
                "field": "ti_quantity"
              }
            }
            ],
            "api": {
              "http": {
                "method": "GET",
                "uri": "http://localhost:${HttpServer.port}/enrichment/api/{{quantity}}?format=json",
                "timeout": 2000,
                "authentication": {}
              }
            },
            "outputs": [
            {
              "schema": "iglu:com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0",
              "json": {
                "jsonPath": "$$.record"
              }
            }
            ],
            "cache": {
              "size": 3000,
              "ttl": 60
            },
            "ignoreOnError": false 
          }
        }""")
          .toOption
          .get,
        SchemaKey(
          "com.snowplowanalytics.snowplow.enrichments",
          "api_request_enrichment_config",
          "jsonschema",
          SchemaVer.Full(1, 0, 2)
        ),
        false
      )
      .toOption
      .get

    val io = runTest(Instant.EPOCH, input, List(apiRequestConf, ipLookupsConf)) {
      case (input, control) =>
        for {
          _ <- Processing.stream(control.environment).compile.drain
          state <- control.state.get
          tokens = input.map(_.ack)
          expectedEtlTstamp = state.collectFirst {
                                case Action.SentToEnriched(List(enriched)) =>
                                  enriched.event.etl_tstamp
                              }.flatten
          expectedE2ELatency = state.collectFirst {
                                 case Action.SetE2ELatencyMetric(latency) => latency
                               }.get
        } yield state should beEqualTo(
          Vector(
            Action.AddedRawCountMetric(1),
            Action.AddedMetadata(expectedMetadata),
            Action.SentToEnriched(expectedEnriched(expectedEtlTstamp)),
            Action.AddedEnrichedCountMetric(1),
            Action.SetE2ELatencyMetric(expectedE2ELatency),
            Action.Checkpointed(tokens)
          )
        )
    }

    // This is not executed inside TestControl.executeEmbed because timeouts are immediate
    // and there is very unstable behavior with Ember client
    HttpServer.resource.use(_ => io)
  }
}

object ProcessingSpec {
  def runTest[A](
    etlTstamp: Instant,
    input: List[TestBatch],
    enrichmentsConfs: List[EnrichmentConf] = Nil,
    mocks: Mocks = Mocks.default
  )(
    f: (List[TokenedEvents], MockEnvironment) => IO[A]
  ): IO[A] =
    IO.sleep(etlTstamp.toEpochMilli.millis) >> input.traverse(_.tokened).flatMap { events =>
      MockEnvironment.build(events, enrichmentsConfs, mocks).use { control =>
        f(events, control)
      }
    }
}
