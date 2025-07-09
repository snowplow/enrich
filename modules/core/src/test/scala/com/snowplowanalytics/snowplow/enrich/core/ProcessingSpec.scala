/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.core

import java.util.{Base64, UUID}
import java.time.Instant
import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.duration._

import org.specs2.Specification

import io.circe.parser

import cats.effect.IO

import fs2.{Chunk, Stream}

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
    Enrich with API enrichment $e5
    Crash if JS script is invalid and exitOnJsCompileError is true $e6
    Emit failed events and bad rows if JS script is invalid and exitOnJsCompileError is false $e7
    Refresh IP lookups assets $e8
  """

  def e1 = {
    val eventId = UUID.randomUUID

    val input = GoodBatch(List.fill(2)(pageView(eventId)))

    val expectedEnriched =
      List.fill(2)(
        Enriched(
          expectedPageView(eventId),
          None,
          Map("app_id" -> "test_app")
        )
      )

    val expectedMetadata = {
      val entities = Metadata.EntitiesAndCount(expectedPageViewMetadataEntities, 2)
      Map(expectedPageViewMetadata -> entities)
    }

    val io =
      for {
        token <- IO.unique
        inputStream = mkGoodStream((input, token))
        assertion <- runTest(etlTstamp, inputStream) {
                       case control =>
                         for {
                           _ <- Processing.stream(control.environment).compile.drain
                           state <- control.state.get
                         } yield state should beEqualTo(
                           Vector(
                             Action.AddedRawCountMetric(2),
                             Action.AddedMetadata(expectedMetadata),
                             Action.SentToEnriched(expectedEnriched),
                             Action.AddedEnrichedCountMetric(2),
                             Action.SetE2ELatencyMetric(Duration(etlLatency.toMinutes, MINUTES)),
                             Action.Checkpointed(List(token))
                           )
                         )
                     }
      } yield assertion

    TestControl.executeEmbed(io)
  }

  def e2 = {
    val eventId = UUID.randomUUID

    val input = GoodBatch(List(invalidAddToCart(eventId)))

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

    val io =
      for {
        token <- IO.unique
        inputStream = mkGoodStream((input, token))
        assertion <- runTest(etlTstamp, inputStream) {
                       case control =>
                         for {
                           _ <- Processing.stream(control.environment).compile.drain
                           state <- control.state.get
                         } yield state should beOneOf(
                           sinkActionsPermutations(
                             Vector(
                               Action.AddedRawCountMetric(1),
                               Action.SentToFailed(expectedFailed),
                               Action.AddedFailedCountMetric(1),
                               Action.SentToBad(expectedBad),
                               Action.AddedBadCountMetric(1),
                               Action.SetE2ELatencyMetric(Duration(etlLatency.toMinutes, MINUTES)),
                               Action.Checkpointed(List(token))
                             ),
                             1,
                             5
                           ): _*
                         )
                     }
      } yield assertion

    TestControl.executeEmbed(io)
  }

  def e3 = {
    val input = BadBatch(Chunk("nonsense", "nonsense2"))

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

    val io =
      for {
        token <- IO.unique
        inputStream = mkBadStream((input, token))
        assertion <- runTest(etlTstamp, inputStream) {
                       case control =>
                         for {
                           _ <- Processing.stream(control.environment).compile.drain
                           state <- control.state.get
                         } yield state should beEqualTo(
                           Vector(
                             Action.AddedRawCountMetric(2),
                             Action.SentToBad(expectedBad),
                             Action.AddedBadCountMetric(2),
                             Action.Checkpointed(List(token))
                           )
                         )
                     }
      } yield assertion

    TestControl.executeEmbed(io)
  }

  def e4 = {
    val eventId = UUID.randomUUID

    val input = GoodBatch(List(pageView(eventId), pageView(eventId, appID = "drop")))

    val expectedEnriched =
      List(
        Enriched(
          expectedPageView(eventId),
          None,
          Map("app_id" -> "test_app")
        )
      )

    val expectedMetadata = {
      val entities = Metadata.EntitiesAndCount(expectedPageViewMetadataEntities, 1)
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

    val io =
      for {
        token <- IO.unique
        inputStream = mkGoodStream((input, token))
        assertion <- runTest(etlTstamp, inputStream, List(jsEnrichConf)) {
                       case control =>
                         for {
                           _ <- Processing.stream(control.environment).compile.drain
                           state <- control.state.get
                         } yield state should beEqualTo(
                           Vector(
                             Action.AddedRawCountMetric(2),
                             Action.AddedDroppedCountMetric(1),
                             Action.AddedMetadata(expectedMetadata),
                             Action.SentToEnriched(expectedEnriched),
                             Action.AddedEnrichedCountMetric(1),
                             Action.SetE2ELatencyMetric(Duration(etlLatency.toMinutes, MINUTES)),
                             Action.Checkpointed(List(token))
                           )
                         )
                     }
      } yield assertion

    TestControl.executeEmbed(io)
  }

  def e5 = {
    val eventId = UUID.randomUUID

    val quantity = 2

    val input = GoodBatch(List(pageView(eventId, tiQuantity = Some(quantity))))

    val expectedContext = addToCartSDD(quantity)

    def expectedEnriched =
      List(
        Enriched(
          expectedPageView(eventId).copy(
            ti_quantity = Some(quantity),
            derived_contexts = SnowplowEvent.Contexts(List(expectedIdentityContext(eventId, etlTstamp), expectedContext))
          ),
          None,
          Map("app_id" -> "test_app")
        )
      )

    val expectedMetadata = {
      val entities = Metadata.EntitiesAndCount((expectedPageViewMetadataEntities + expectedContext.schema), 1)
      Map(expectedPageViewMetadata -> entities)
    }

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
                "uri": "${MockEnvironment.mockHttpServerUri}/enrichment/api/{{quantity}}?format=json",
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

    val io =
      for {
        token <- IO.unique
        inputStream = mkGoodStream((input, token))
        assertion <- runTest(etlTstamp, inputStream, List(apiRequestConf)) {
                       case control =>
                         for {
                           _ <- Processing.stream(control.environment).compile.drain
                           state <- control.state.get
                         } yield state should beEqualTo(
                           Vector(
                             Action.AddedRawCountMetric(1),
                             Action.AddedMetadata(expectedMetadata),
                             Action.SentToEnriched(expectedEnriched),
                             Action.AddedEnrichedCountMetric(1),
                             Action.SetE2ELatencyMetric(Duration(etlLatency.toMinutes, MINUTES)),
                             Action.Checkpointed(List(token))
                           )
                         )
                     }
      } yield assertion

    TestControl.executeEmbed(io)
  }

  def e6 = {
    val script = """
      function process(event, params) {
          return [ ;
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

    val io = runTest(etlTstamp, Stream.empty, List(jsEnrichConf), exitOnJsCompileError = true) {
      case control => Processing.stream(control.environment).compile.drain
    }.attempt.map {
      case Left(err) if err.getMessage.contains("Can't build enrichments registry: Error compiling JavaScript function") => ok
      case Left(_) => ko("Environment crashed with unexpected error")
      case Right(_) => ko("Environment didn't crash")
    }

    TestControl.executeEmbed(io)
  }

  def e7 = {
    val eventId = UUID.randomUUID

    val input = GoodBatch(List(pageView(eventId)))

    val expectedFailed =
      List(
        Failed(
          expectedFailedJavascript(eventId),
          None,
          Map("app_id" -> "test_app")
        )
      )

    val expectedBad =
      List(
        Bad(
          expectedBadJavascript(eventId),
          None,
          Map.empty
        )
      )

    val script = """
      function process(event, params) {
          return [ ;
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

    val io =
      for {
        token <- IO.unique
        inputStream = mkGoodStream((input, token))
        assertion <- runTest(etlTstamp, inputStream, List(jsEnrichConf), exitOnJsCompileError = false) {
                       case control =>
                         for {
                           _ <- Processing.stream(control.environment).compile.drain
                           state <- control.state.get
                         } yield state should beOneOf(
                           sinkActionsPermutations(
                             Vector(
                               Action.AddedRawCountMetric(1),
                               Action.SentToFailed(expectedFailed),
                               Action.AddedFailedCountMetric(1),
                               Action.SentToBad(expectedBad),
                               Action.AddedBadCountMetric(1),
                               Action.SetE2ELatencyMetric(Duration(etlLatency.toMinutes, MINUTES)),
                               Action.Checkpointed(List(token))
                             ),
                             1,
                             5
                           ): _*
                         )
                     }
      } yield assertion

    TestControl.executeEmbed(io)
  }

  def e8 = {
    val eventId = UUID.randomUUID

    val input = GoodBatch(List(pageView(eventId)))

    def expectedEnriched(etlTstamp: Instant, city: String) =
      List(
        Enriched(
          expectedPageView(eventId).copy(
            etl_tstamp = Some(etlTstamp),
            geo_country = Some("CN"),
            geo_region = Some("22"),
            geo_city = Some(city),
            geo_latitude = Some(43.88),
            geo_longitude = Some(125.3228),
            geo_region_name = Some("Jilin Sheng"),
            geo_timezone = Some("Asia/Harbin"),
            derived_contexts = SnowplowEvent.Contexts(Nil)
          ),
          None,
          Map("app_id" -> "test_app")
        )
      )

    val expectedMetadata = Map(Metadata.MetadataEvent(None, Some("test_app"), None, None, None) -> Metadata.EntitiesAndCount(Set(), 1))

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
              "uri": "${MockEnvironment.mockHttpServerUri}/maxmind"
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

    val io =
      for {
        token1 <- IO.unique
        token2 <- IO.unique
        inputStream = mkGoodStream((input, token1), (input, token2)).spaced(4.seconds)
        assertion <- runTest(Instant.EPOCH, inputStream, List(ipLookupsConf)) {
                       case control =>
                         val environment = control.environment.copy(identity = None)
                         for {
                           _ <- Processing.stream(environment).compile.drain
                           state <- control.state.get
                           etlTstamps = state.collect { case Action.SentToEnriched(List(enriched)) => enriched.event.etl_tstamp.get }
                           etlLatencies = state.collect { case Action.SetE2ELatencyMetric(latency) => latency }
                         } yield state should beEqualTo(
                           Vector(
                             Action.AddedRawCountMetric(1),
                             Action.AddedMetadata(expectedMetadata),
                             Action.SentToEnriched(expectedEnriched(etlTstamps.head, "Fuyu")),
                             Action.AddedEnrichedCountMetric(1),
                             Action.SetE2ELatencyMetric(Duration(etlLatencies.head.toMicros, MICROSECONDS)),
                             Action.Checkpointed(List(token1)),
                             Action.AddedRawCountMetric(1),
                             Action.AddedMetadata(expectedMetadata),
                             Action.SentToEnriched(expectedEnriched(etlTstamps.last, "Changchun")),
                             Action.AddedEnrichedCountMetric(1),
                             Action.SetE2ELatencyMetric(Duration(etlLatencies.last.toMicros, MICROSECONDS)),
                             Action.Checkpointed(List(token2))
                           )
                         )
                     }
      } yield assertion

    // Can't get executed with TestControl because of Async.evalOn in IP lookups enrichment
    io
  }
}

object ProcessingSpec {
  def runTest[A](
    etlTstamp: Instant,
    input: Stream[IO, TokenedEvents],
    enrichmentsConfs: List[EnrichmentConf] = Nil,
    mocks: Mocks = Mocks.default,
    exitOnJsCompileError: Boolean = true
  )(
    f: MockEnvironment => IO[A]
  ): IO[A] =
    IO.sleep(etlTstamp.toEpochMilli.millis) >>
      MockEnvironment.build(input, enrichmentsConfs, mocks, exitOnJsCompileError).use { env =>
        f(env)
      }

  /**
   * Since sink actions are run parallel, exact order of sink actions can't be known.
   * To be able to test the results in the tests, permutations of actions are created here.
   */
  def sinkActionsPermutations(
    actions: Vector[MockEnvironment.Action],
    sinkActionStart: Int,
    sinkActionFinish: Int
  ): Vector[Vector[MockEnvironment.Action]] = {
    val sublist = actions.slice(sinkActionStart, sinkActionFinish)
    val perms = sublist.grouped(2).toVector.permutations.map(_.flatten).toVector
    perms.map(v => actions.take(sinkActionStart) ++ v ++ actions.drop(sinkActionFinish))
  }

}
