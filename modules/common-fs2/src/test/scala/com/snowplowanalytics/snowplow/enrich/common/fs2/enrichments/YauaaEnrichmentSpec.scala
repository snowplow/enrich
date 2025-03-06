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

import cats.implicits._

import cats.effect.testing.specs2.CatsEffect

import fs2.Stream

import io.circe.literal._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.Contexts

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.YauaaConf

import com.snowplowanalytics.snowplow.enrich.common.fs2.EnrichSpec
import com.snowplowanalytics.snowplow.enrich.common.fs2.test._

import org.specs2.mutable.Specification

class YauaaEnrichmentSpec extends Specification with CatsEffect {

  sequential

  "YauaaEnrichment" should {
    "add a derived context" in {
      val payload = EnrichSpec.collectorPayload.copy(
        context = EnrichSpec.collectorPayload.context.copy(
          useragent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:81.0) Gecko/20100101 Firefox/81.0".some
        )
      )
      val input = Stream(payload.toRaw)

      /** Schemas defined at [[SchemaRegistry]] */
      val enrichment = YauaaConf(
        SchemaKey("com.acme", "enrichment", "jsonschema", SchemaVer.Full(1, 0, 0)),
        Some(1)
      )

      val expected = Contexts(
        List(
          SelfDescribingData(
            SchemaKey("nl.basjes", "yauaa_context", "jsonschema", SchemaVer.Full(1, 0, 5)),
            json"""{
          "deviceBrand" : "Apple",
          "deviceName" : "Apple Macintosh",
          "operatingSystemVersionMajor" : "10.14",
          "layoutEngineNameVersion" : "Gecko 81.0",
          "operatingSystemNameVersion" : "Mac OS 10.14",
          "agentInformationEmail": "Unknown",
          "networkType": "Unknown",
          "layoutEngineBuild" : "20100101",
          "webviewAppNameVersion": "Unknown ??",
          "webviewAppNameVersionMajor": "Unknown ??",
          "layoutEngineNameVersionMajor" : "Gecko 81",
          "operatingSystemName" : "Mac OS",
          "agentVersionMajor" : "81",
          "layoutEngineVersionMajor" : "81",
          "webviewAppName": "Unknown",
          "deviceClass" : "Desktop",
          "agentNameVersionMajor" : "Firefox 81",
          "operatingSystemNameVersionMajor" : "Mac OS 10.14",
          "deviceCpuBits" : "64",
          "webviewAppVersionMajor": "??",
          "operatingSystemClass" : "Desktop",
          "webviewAppVersion": "??",
          "layoutEngineName" : "Gecko",
          "agentName" : "Firefox",
          "agentVersion" : "81.0",
          "layoutEngineClass" : "Browser",
          "agentNameVersion" : "Firefox 81.0",
          "operatingSystemVersion" : "10.14",
          "deviceCpu" : "Intel",
          "agentClass" : "Browser",
          "layoutEngineVersion" : "81.0"
        }"""
          )
        )
      )

      TestEnvironment.make(input, List(enrichment)).use { test =>
        test.run().map {
          case (bad, pii, good, incomplete) =>
            (bad must be empty)
            (pii must be empty)
            (incomplete must be empty)
            good.map(_.derived_contexts) must contain(exactly(expected))
        }
      }
    }
  }
}
