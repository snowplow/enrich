/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.enrichments

import java.net.URI

import scala.concurrent.duration._

import cats.syntax.apply._
import cats.syntax.option._

import cats.effect.IO

import cats.effect.testing.specs2.CatsEffect

import io.circe.literal._

import fs2.Stream

import org.specs2.mutable.Specification

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.Contexts

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf

import com.snowplowanalytics.snowplow.enrich.common.fs2.EnrichSpec
import com.snowplowanalytics.snowplow.enrich.common.fs2.test.{HttpServer, TestEnvironment}

class IabEnrichmentSpec extends Specification with CatsEffect {

  sequential

  "IabEnrichment" should {
    "recognize a robot by IP address" in {
      val payload = EnrichSpec.collectorPayload.copy(
        context = EnrichSpec.collectorPayload.context.copy(
          useragent = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0".some
        )
      )
      val input = Stream(payload.toRaw)
      val expected = Contexts(
        List(
          SelfDescribingData(
            SchemaKey("com.iab.snowplow", "spiders_and_robots", "jsonschema", SchemaVer.Full(1, 0, 0)),
            json"""{"spiderOrRobot":true,"category":"SPIDER_OR_ROBOT","reason":"FAILED_IP_EXCLUDE","primaryImpact":"UNKNOWN"}"""
          )
        )
      )
      val testWithHttp = HttpServer.resource *> TestEnvironment.make(input, List(IabEnrichmentSpec.enrichmentConf))
      testWithHttp.use { test =>
        test.run().map {
          case (bad, pii, good, incomplete) =>
            (bad must be empty)
            (pii must be empty)
            (incomplete must be empty)
            good.map(_.derived_contexts) must contain(exactly(expected))
        }
      }
    }

    "refresh assets" in {
      val payload = EnrichSpec.collectorPayload.copy(
        context = EnrichSpec.collectorPayload.context.copy(
          useragent = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0".some
        )
      )
      val input = Stream(payload.toRaw) ++ Stream.sleep_[IO](2.seconds) ++ Stream(payload.toRaw)

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

      val testWithHttp = HttpServer.resource *> TestEnvironment.make(input, List(IabEnrichmentSpec.enrichmentConf))
      testWithHttp.use { test =>
        test.run(_.copy(assetsUpdatePeriod = Some(1800.millis))).map {
          case (bad, pii, good, incomplete) =>
            (bad must be empty)
            (pii must be empty)
            (incomplete must be empty)
            good.map(_.derived_contexts) must contain(exactly(expectedOne, expectedTwo))
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
