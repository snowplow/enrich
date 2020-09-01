/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.fs2.enrichments

import java.util.Base64

import scala.concurrent.duration._

import org.apache.http.message.BasicNameValuePair

import cats.implicits._

import cats.effect.IO
import cats.effect.testing.specs2.CatsIO

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
  Output
}

import com.snowplowanalytics.snowplow.enrich.fs2.enrichments.ApiRequestEnrichmentSpec.unstructEvent
import com.snowplowanalytics.snowplow.enrich.fs2.{EnrichSpec, Payload}
import com.snowplowanalytics.snowplow.enrich.fs2.test._

import org.specs2.mutable.Specification

class ApiRequestEnrichmentSpec extends Specification with CatsIO {

  sequential

  "ApiRequestEnrichment" should {
    "add a derived context" in {
      val event =
        json"""{
          "schema": "iglu:com.acme/test/jsonschema/1-0-1",
          "data": {"path": {"id": 3}}
        }"""
      val payload = EnrichSpec.colllectorPayload.copy(
        querystring = new BasicNameValuePair("ue_px", unstructEvent(event)) :: EnrichSpec.colllectorPayload.querystring
      )
      val input = Stream(Payload(payload.toRaw, IO.unit))

      /** Schemas defined at [[SchemaRegistry]] */
      val enrichment = ApiRequestConf(
        SchemaKey("com.acme", "enrichment", "jsonschema", SchemaVer.Full(1, 0, 0)),
        List(Input.Json("key1", "unstruct_event", SchemaCriterion("com.acme", "test", "jsonschema", 1), "$.path.id")),
        HttpApi("GET", "http://localhost:8080/enrichment/api/{{key1}}", 2000, Authentication(None)),
        List(Output("iglu:com.acme/output/jsonschema/1-0-0", Some(JsonOutput("$")))),
        Cache(1, 1000)
      )

      val expected = Contexts(
        List(
          SelfDescribingData(
            SchemaKey("com.acme", "output", "jsonschema", SchemaVer.Full(1, 0, 0)),
            json"""{"output": "3"}"""
          )
        )
      )

      val testWithHttp = HttpServer.resource(4.seconds) *> TestEnvironment.make(input, List(enrichment))
      testWithHttp.use { test =>
        test.run().map { events =>
          events must beLike {
            case List(Right(event)) =>
              event.derived_contexts must beEqualTo(expected)
            case other => ko(s"Expected one enriched event, got $other")
          }
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
