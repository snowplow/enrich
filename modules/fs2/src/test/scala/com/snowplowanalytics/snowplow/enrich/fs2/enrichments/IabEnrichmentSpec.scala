/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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

import java.net.URI

import scala.concurrent.duration._

import cats.syntax.apply._
import cats.syntax.option._

import cats.effect.IO

import io.circe.literal._

import fs2.Stream

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.Contexts
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.fs2.{EnrichSpec, Output, Payload}
import com.snowplowanalytics.snowplow.enrich.fs2.test.{HttpServer, TestEnvironment}

import org.specs2.mutable.Specification
import cats.effect.testing.specs2.CatsIO

class IabEnrichmentSpec extends Specification with CatsIO {

  sequential

  "IabEnrichment" should {
    "recognize a robot by IP address" in {
      val payload = EnrichSpec.collectorPayload.copy(
        context = EnrichSpec.collectorPayload.context.copy(
          useragent = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0".some
        )
      )
      val input = Stream(Payload(payload.toRaw, IO.unit))
      val expected = Contexts(
        List(
          SelfDescribingData(
            SchemaKey("com.iab.snowplow", "spiders_and_robots", "jsonschema", SchemaVer.Full(1, 0, 0)),
            json"""{"spiderOrRobot":true,"category":"SPIDER_OR_ROBOT","reason":"FAILED_IP_EXCLUDE","primaryImpact":"UNKNOWN"}"""
          )
        )
      )
      val testWithHttp = HttpServer.resource(6.seconds) *> TestEnvironment.make(input, List(IabEnrichmentSpec.enrichmentConf))
      testWithHttp.use { test =>
        test.run().map {
          case List(Output.Good(event)) =>
            event.derived_contexts must beEqualTo(expected)
          case other =>
            ko(s"Expected one valid event, got $other")
        }
      }
    }

    "refresh assets" in {
      val payload = EnrichSpec.collectorPayload.copy(
        context = EnrichSpec.collectorPayload.context.copy(
          useragent = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0".some
        )
      )
      val input = Stream(Payload(payload.toRaw, IO.unit)) ++ Stream.sleep_(2.seconds) ++ Stream(Payload(payload.toRaw, IO.unit))

      val expectedOne = Contexts(
        List(
          SelfDescribingData(
            SchemaKey("com.iab.snowplow", "spiders_and_robots", "jsonschema", SchemaVer.Full(1, 0, 0)),
            json"""{"spiderOrRobot":true,"category":"SPIDER_OR_ROBOT","reason":"FAILED_IP_EXCLUDE","primaryImpact":"UNKNOWN"}"""
          )
        )
      )
      val expectedTwo = Contexts(
        List(
          SelfDescribingData(
            SchemaKey("com.iab.snowplow", "spiders_and_robots", "jsonschema", SchemaVer.Full(1, 0, 0)),
            json"""{"spiderOrRobot":false,"category":"BROWSER","reason":"PASSED_ALL","primaryImpact":"NONE"}"""
          )
        )
      )

      val testWithHttp = HttpServer.resource(6.seconds) *> TestEnvironment.make(input, List(IabEnrichmentSpec.enrichmentConf))
      testWithHttp.use { test =>
        test.run(_.copy(assetsUpdatePeriod = Some(1800.millis))).map {
          case List(Output.Good(eventOne), Output.Good(eventTwo)) =>
            List(eventOne.derived_contexts, eventTwo.derived_contexts) must containTheSameElementsAs(List(expectedOne, expectedTwo))
          case other =>
            ko(s"Expected two valid events, got $other")
        }
      }
    }
  }
}

object IabEnrichmentSpec {
  val enrichmentConf = EnrichmentConf.IabConf(
    SchemaKey("com.acme", "enrichment", "jsonschema", SchemaVer.Full(1, 0, 0)),
    (URI.create("http://localhost:8080/iab/ip"), "ip"),
    (URI.create("http://localhost:8080/iab/exclude"), "exclude"),
    (URI.create("http://localhost:8080/iab/include"), "include")
  )
}
