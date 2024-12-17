/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.enrichments

import java.util.Base64

import org.apache.http.message.BasicNameValuePair

import cats.implicits._

import cats.effect.testing.specs2.CatsEffect

import org.specs2.mutable.Specification

import fs2.Stream

import io.circe.Json
import io.circe.literal._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.Contexts

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.ApiRequestConf
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest.{
  Authentication,
  Cache,
  HttpApi,
  Input,
  JsonOutput,
  Output => RegistryOutput
}

import com.snowplowanalytics.snowplow.enrich.common.fs2.enrichments.ApiRequestEnrichmentSpec.unstructEvent
import com.snowplowanalytics.snowplow.enrich.common.fs2.EnrichSpec
import com.snowplowanalytics.snowplow.enrich.common.fs2.test._
import com.snowplowanalytics.snowplow.badrows.BadRow

class ApiRequestEnrichmentSpec extends Specification with CatsEffect {

  sequential

  "ApiRequestEnrichment" should {
    "add a derived context" in {
      val event =
        json"""{
          "schema": "iglu:com.acme/test/jsonschema/1-0-1",
          "data": {"path": {"id": 3}}
        }"""
      val payload = EnrichSpec.collectorPayload.copy(
        querystring = new BasicNameValuePair("ue_px", unstructEvent(event)) :: EnrichSpec.collectorPayload.querystring
      )
      val input = Stream(payload.toRaw)

      /** Schemas defined at [[SchemaRegistry]] */
      val enrichment = ApiRequestConf(
        SchemaKey("com.acme", "enrichment", "jsonschema", SchemaVer.Full(1, 0, 0)),
        List(Input.Json("key1", "unstruct_event", SchemaCriterion("com.acme", "test", "jsonschema", 1), "$.path.id")),
        HttpApi("GET", "http://localhost:8080/enrichment/api/{{key1}}", 2000, Authentication(None)),
        List(RegistryOutput("iglu:com.acme/output/jsonschema/1-0-0", Some(JsonOutput("$")))),
        Cache(1, 1000),
        ignoreOnError = false
      )

      val expected = Contexts(
        List(
          SelfDescribingData(
            SchemaKey("com.acme", "output", "jsonschema", SchemaVer.Full(1, 0, 0)),
            json"""{"output": "3"}"""
          )
        )
      )

      val testWithHttp = HttpServer.resource *> TestEnvironment.make(input, List(enrichment))
      testWithHttp.use { test =>
        test.run().map {
          case (bad, pii, good, incomplete) =>
            bad must beEmpty
            pii must beEmpty
            incomplete must beEmpty
            good.map(_.derived_contexts) must contain(exactly(expected))
        }
      }
    }

    "generate bad rows if API server is not available" in {
      val nbEvents = 1000
      val input = Stream((1 to nbEvents).toList: _*)
        .map { i =>
          json"""{
            "schema": "iglu:com.acme/test/jsonschema/1-0-1",
            "data": {"path": {"id": $i}}
          }"""
        }
        .map { ue =>
          EnrichSpec.collectorPayload.copy(
            querystring = new BasicNameValuePair("ue_px", unstructEvent(ue)) :: EnrichSpec.collectorPayload.querystring
          )
        }
        .map(_.toRaw)

      val enrichment = ApiRequestConf(
        SchemaKey("com.acme", "enrichment", "jsonschema", SchemaVer.Full(1, 0, 0)),
        List(Input.Json("key1", "unstruct_event", SchemaCriterion("com.acme", "test", "jsonschema", 1), "$.path.id")),
        HttpApi("GET", "http://foo.bar.unassigned/{{key1}}", 2000, Authentication(None)),
        List(RegistryOutput("iglu:com.acme/output/jsonschema/1-0-0", Some(JsonOutput("$")))),
        Cache(1, 1000),
        ignoreOnError = false
      )

      TestEnvironment.make(input, List(enrichment)).use { test =>
        test.run().map {
          case (bad, pii, good, incomplete) =>
            good must beEmpty
            pii must beEmpty
            incomplete must haveSize(nbEvents)
            bad.collect { case ef: BadRow.EnrichmentFailures => ef } must haveSize(nbEvents)
        }
      }
    }
  }
}

object ApiRequestEnrichmentSpec {
  private val encoder = Base64.getEncoder

  def encode(json: Json): String =
    new String(encoder.encode(json.noSpaces.getBytes))

  def unstructEvent(json: Json): String =
    encode(json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":$json}""")
}
