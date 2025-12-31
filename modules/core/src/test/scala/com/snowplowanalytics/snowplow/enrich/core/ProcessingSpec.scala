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

import com.snowplowanalytics.iglu.client.resolver.registries.{Registry, RegistryError, RegistryLookup}
import com.snowplowanalytics.iglu.core.SchemaList

import io.circe.Json

import com.snowplowanalytics.snowplow.streams.TokenedEvents

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.{IpLookupsEnrichment, JavascriptScriptEnrichment}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest.ApiRequestEnrichment
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.utils.IgluUtils
import com.snowplowanalytics.snowplow.enrich.core.CompressionTestUtils._

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
    Process compressed GZIP input with multiple batched payloads $e9
    Process compressed ZSTD input with multiple batched payloads $e10
    Process mixed compressed and uncompressed inputs $e11
    Process mixed inputs with different compression types $e12
    Split compressed batch when exceeding maxBytesInBatch limit $e13
    Emit bad rows for compressed payloads exceeding maxBytesSinglePayload limit $e14
    Crash with IgluSystemError when Iglu Server is unavailable during schema validation $e15
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
          expectedBadCPF("bm9uc2Vuc2Uy"),
          None,
          Map.empty
        ),
        Bad(
          expectedBadCPF("bm9uc2Vuc2U="),
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
            derived_contexts = SnowplowEvent.Contexts(List(expectedContext))
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

  // Tests e9-e12: Compressed input processing tests

  def e9 = {
    val eventId1 = UUID.randomUUID
    val eventId2 = UUID.randomUUID
    val eventId3 = UUID.randomUUID

    val input = CompressedBatch(List(pageView(eventId1), pageView(eventId2), pageView(eventId3)), CompressionType.GZIP)

    val expectedEnriched =
      List(
        Enriched(
          expectedPageView(eventId1),
          None,
          Map("app_id" -> "test_app")
        ),
        Enriched(
          expectedPageView(eventId2),
          None,
          Map("app_id" -> "test_app")
        ),
        Enriched(
          expectedPageView(eventId3),
          None,
          Map("app_id" -> "test_app")
        )
      )

    val expectedMetadata = {
      val entities = Metadata.EntitiesAndCount(expectedPageViewMetadataEntities, 3)
      Map(expectedPageViewMetadata -> entities)
    }

    val io =
      for {
        token <- IO.unique
        inputStream = mkCompressedStream((input, token))
        assertion <- runTest(etlTstamp, inputStream) {
                       case control =>
                         for {
                           _ <- Processing.stream(control.environment).compile.drain
                           state <- control.state.get
                         } yield state should beEqualTo(
                           Vector(
                             Action.AddedRawCountMetric(3),
                             Action.AddedMetadata(expectedMetadata),
                             Action.SentToEnriched(expectedEnriched),
                             Action.AddedEnrichedCountMetric(3),
                             Action.SetE2ELatencyMetric(Duration(etlLatency.toMinutes, MINUTES)),
                             Action.Checkpointed(List(token))
                           )
                         )
                     }
      } yield assertion

    TestControl.executeEmbed(io)
  }

  def e10 = {
    val eventId1 = UUID.randomUUID
    val eventId2 = UUID.randomUUID

    val input = CompressedBatch(List(pageView(eventId1), pageView(eventId2)), CompressionType.ZSTD)

    val expectedEnriched =
      List(
        Enriched(
          expectedPageView(eventId1),
          None,
          Map("app_id" -> "test_app")
        ),
        Enriched(
          expectedPageView(eventId2),
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
        inputStream = mkCompressedStream((input, token))
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

  def e11 = {
    val eventId1 = UUID.randomUUID
    val eventId2 = UUID.randomUUID
    val eventId3 = UUID.randomUUID
    val eventId4 = UUID.randomUUID

    val uncompressedBatch = GoodBatch(List(pageView(eventId1)))
    val compressedBatch = CompressedBatch(List(pageView(eventId2), pageView(eventId3)), CompressionType.GZIP)
    val uncompressedBatch2 = GoodBatch(List(pageView(eventId4)))

    val io =
      for {
        token1 <- IO.unique
        token2 <- IO.unique
        token3 <- IO.unique
        uncompressedStream1 = mkGoodStream((uncompressedBatch, token1))
        compressedStream = mkCompressedStream((compressedBatch, token2))
        uncompressedStream2 = mkGoodStream((uncompressedBatch2, token3))
        inputStream = uncompressedStream1 ++ compressedStream ++ uncompressedStream2
        assertion <- runTest(etlTstamp, inputStream) {
                       case control =>
                         for {
                           _ <- Processing.stream(control.environment).compile.drain
                           state <- control.state.get
                         } yield {
                           val rawCountMetrics = state.collect { case Action.AddedRawCountMetric(count) => count }
                           val enrichedCountMetrics = state.collect { case Action.AddedEnrichedCountMetric(count) => count }
                           val checkpointedTokens = state.collect { case Action.Checkpointed(tokens) => tokens }.flatten.toList
                           val totalEnriched = state.collect { case Action.SentToEnriched(enriched) => enriched }.flatten.toList

                           rawCountMetrics.sum should beEqualTo(4)
                           enrichedCountMetrics.sum should beEqualTo(4)
                           totalEnriched.length should beEqualTo(4)
                           checkpointedTokens should containTheSameElementsAs(List(token1, token2, token3))
                         }
                     }
      } yield assertion

    TestControl.executeEmbed(io)
  }

  def e12 = {
    val eventId1 = UUID.randomUUID
    val eventId2 = UUID.randomUUID
    val eventId3 = UUID.randomUUID
    val eventId4 = UUID.randomUUID
    val eventId5 = UUID.randomUUID

    val compressedBatch1 = CompressedBatch(List(pageView(eventId1), pageView(eventId2)), CompressionType.GZIP)
    val uncompressedBatch = GoodBatch(List(pageView(eventId3)))
    val compressedBatch2 = CompressedBatch(List(pageView(eventId4), pageView(eventId5)), CompressionType.ZSTD)

    val expectedEnriched =
      List(
        Enriched(
          expectedPageView(eventId1),
          None,
          Map("app_id" -> "test_app")
        ),
        Enriched(
          expectedPageView(eventId2),
          None,
          Map("app_id" -> "test_app")
        ),
        Enriched(
          expectedPageView(eventId3),
          None,
          Map("app_id" -> "test_app")
        ),
        Enriched(
          expectedPageView(eventId4),
          None,
          Map("app_id" -> "test_app")
        ),
        Enriched(
          expectedPageView(eventId5),
          None,
          Map("app_id" -> "test_app")
        )
      )

    val io =
      for {
        token1 <- IO.unique
        token2 <- IO.unique
        token3 <- IO.unique
        compressedStream1 = mkCompressedStream((compressedBatch1, token1))
        uncompressedStream = mkGoodStream((uncompressedBatch, token2))
        compressedStream2 = mkCompressedStream((compressedBatch2, token3))
        inputStream = compressedStream1 ++ uncompressedStream ++ compressedStream2
        assertion <- runTest(etlTstamp, inputStream) {
                       case control =>
                         for {
                           _ <- Processing.stream(control.environment).compile.drain
                           state <- control.state.get
                         } yield {
                           val rawCountMetrics = state.collect { case Action.AddedRawCountMetric(count) => count }
                           val enrichedCountMetrics = state.collect { case Action.AddedEnrichedCountMetric(count) => count }
                           val checkpointedTokens = state.collect { case Action.Checkpointed(tokens) => tokens }.flatten.toList
                           val totalEnriched = state.collect { case Action.SentToEnriched(enriched) => enriched }.flatten.toList

                           rawCountMetrics.sum should beEqualTo(5) // 2+1+2 = 5
                           enrichedCountMetrics.sum should beEqualTo(5) // 2+1+2 = 5
                           totalEnriched.length should beEqualTo(5)
                           totalEnriched should containTheSameElementsAs(expectedEnriched)
                           checkpointedTokens should containTheSameElementsAs(List(token1, token2, token3))
                         }
                     }
      } yield assertion

    TestControl.executeEmbed(io)
  }

  def e13 = {
    val eventId1 = UUID.randomUUID
    val eventId2 = UUID.randomUUID

    val largePayload1 = pageView(eventId1).copy(
      querystring = pageView(eventId1).querystring ++ List.fill(100)(
        new org.apache.http.message.BasicNameValuePair("large_param", "x" * 50)
      )
    )
    val largePayload2 = pageView(eventId2).copy(
      querystring = pageView(eventId2).querystring ++ List.fill(100)(
        new org.apache.http.message.BasicNameValuePair("large_param", "y" * 50)
      )
    )

    val input = CompressedBatch(
      List(largePayload1, largePayload2),
      CompressionType.GZIP
    )

    val expectedEnriched =
      List(
        Enriched(
          expectedPageView(eventId1),
          None,
          Map("app_id" -> "test_app")
        ),
        Enriched(
          expectedPageView(eventId2),
          None,
          Map("app_id" -> "test_app")
        )
      )

    val expectedMetadata = {
      val entities = Metadata.EntitiesAndCount(expectedPageViewMetadataEntities, 1)
      Map(expectedPageViewMetadata -> entities)
    }

    val io =
      for {
        token <- IO.unique
        inputStream = mkCompressedStream((input, token))
        // Use very small maxBytesInBatch to force splitting of the decompressed payloads
        smallBatchConfig = Config.Decompression(maxBytesSinglePayload = 10000000, maxBytesInBatch = 1000)
        assertion <- runTest(etlTstamp, inputStream, decompressionConfig = smallBatchConfig) {
                       case control =>
                         for {
                           _ <- Processing.stream(control.environment).compile.drain
                           state <- control.state.get
                         } yield state should beEqualTo(
                           Vector(
                             Action.AddedRawCountMetric(1), // First payload processed
                             Action.AddedMetadata(expectedMetadata),
                             Action.SentToEnriched(List(expectedEnriched.head)),
                             Action.AddedEnrichedCountMetric(1),
                             Action.SetE2ELatencyMetric(Duration(etlLatency.toMicros, MICROSECONDS)),
                             Action.AddedRawCountMetric(1), // Second payload processed
                             Action.AddedMetadata(expectedMetadata),
                             Action.SentToEnriched(List(expectedEnriched(1))),
                             Action.AddedEnrichedCountMetric(1),
                             Action.SetE2ELatencyMetric(Duration(etlLatency.toMicros, MICROSECONDS)),
                             Action.Checkpointed(List(token)) // Token attached to final result
                           )
                         )
                     }
      } yield assertion

    TestControl.executeEmbed(io)
  }

  def e14 = {
    // Test: Verify that compressed payloads exceeding maxBytesSinglePayload limit generate size violation bad rows
    // Key insight: The system checks the RAW payload size (what would be decompressed) against the limit,
    // NOT the compressed batch size. This prevents memory exhaustion from decompressing huge payloads.

    val eventId = UUID.randomUUID

    // Create a simple payload (just x=y parameter). Despite being simple, the raw Thrift serialization
    // produces ~222 bytes due to all the metadata, headers, and field structures.
    val oversizedPayload = pageView(eventId).copy(
      querystring = List(new org.apache.http.message.BasicNameValuePair("x", "y"))
    )

    val input = CompressedBatch(List(oversizedPayload), CompressionType.GZIP)

    // Set a very small limit (50 bytes) that the raw payload will exceed
    val maxSize = 50

    // Generate the compressed batch and base64 payload that will be stored in the bad row
    val compressedBytes = createCompressedStream(List(oversizedPayload).map(_.toRaw), CompressionType.GZIP)
    val base64Payload = java.util.Base64.getEncoder.encodeToString(compressedBytes.array())

    // The decompressor reads the 4-byte size from the compressed stream header and compares it to maxSize.
    // This size represents the raw payload that would be decompressed (~222 bytes), not the compressed
    // batch size (~187 bytes). If rawSize > maxSize, it creates RecordTooBig BEFORE actually decompressing.
    val actualCompressedSize = oversizedPayload.toRaw.length // ~222 bytes

    // Expected behavior: The system should create a SizeViolation bad row containing:
    // - maxSize (50): The configured limit
    // - actualCompressedSize (~222): The raw payload size that would exceed the limit
    // - Descriptive message about the size violation
    // - base64Payload: The original compressed batch for debugging
    val expectedBad =
      List(
        Bad(
          expectedBadSizeViolation(
            maxSize,
            actualCompressedSize,
            s"Collector payload will exceed maximum allowed size of $maxSize after Gzip decompression",
            base64Payload
          ),
          None,
          Map.empty
        )
      )

    val io =
      for {
        token <- IO.unique
        inputStream = mkCompressedStream((input, token))
        restrictiveConfig = Config.Decompression(maxBytesSinglePayload = maxSize, maxBytesInBatch = 10000000)
        assertion <- runTest(etlTstamp, inputStream, decompressionConfig = restrictiveConfig) {
                       case control =>
                         for {
                           _ <- Processing.stream(control.environment).compile.drain
                           state <- control.state.get
                         } yield state should beEqualTo(
                           Vector(
                             Action.AddedRawCountMetric(1), // Oversized payload processed
                             Action.SentToBad(expectedBad), // Size violation bad row created
                             Action.AddedBadCountMetric(1), // Bad count metric updated
                             Action.Checkpointed(List(token)) // Token attached to final result
                           )
                         )
                     }
      } yield assertion

    TestControl.executeEmbed(io)
  }

  def e15 = {
    // Test: Verify that IgluSystemError bubbles up when Iglu Server is unavailable
    // This ensures the application crashes rather than creating bad rows for infrastructure issues
    val eventId = UUID.randomUUID

    // Use an event with an unstruct event that requires schema validation
    val input = GoodBatch(List(invalidAddToCart(eventId)))

    // Create a RegistryLookup that simulates server unavailability by returning RepoFailure
    val failingRegistryLookup: RegistryLookup[IO] = new RegistryLookup[IO] {
      def lookup(registry: Registry, schemaKey: SchemaKey): IO[Either[RegistryError, Json]] =
        IO.pure(Left(RegistryError.RepoFailure("Simulated Iglu Server unavailability")))

      def list(
        registry: Registry,
        vendor: String,
        name: String,
        model: Int
      ): IO[Either[RegistryError, SchemaList]] =
        IO.pure(Left(RegistryError.RepoFailure("Simulated Iglu Server unavailability")))
    }

    val io =
      for {
        token <- IO.unique
        inputStream = mkGoodStream((input, token))
        result <- runTest(etlTstamp, inputStream, registryLookup = failingRegistryLookup)(control =>
                    Processing.stream(control.environment).compile.drain
                  ).attempt
      } yield result must beLeft(haveClass[IgluUtils.IgluSystemError])

    TestControl.executeEmbed(io)
  }

}

object ProcessingSpec {
  def runTest[A](
    etlTstamp: Instant,
    input: Stream[IO, TokenedEvents],
    enrichmentsConfs: List[EnrichmentConf] = Nil,
    mocks: Mocks = Mocks.default,
    exitOnJsCompileError: Boolean = true,
    decompressionConfig: Config.Decompression = Config.Decompression(10000000, 10000000),
    registryLookup: RegistryLookup[IO] = SpecHelpers.registryLookup
  )(
    f: MockEnvironment => IO[A]
  ): IO[A] =
    IO.sleep(etlTstamp.toEpochMilli.millis) >>
      MockEnvironment
        .build(
          input,
          enrichmentsConfs,
          mocks,
          exitOnJsCompileError,
          decompressionConfig,
          registryLookup
        )
        .use { env =>
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
