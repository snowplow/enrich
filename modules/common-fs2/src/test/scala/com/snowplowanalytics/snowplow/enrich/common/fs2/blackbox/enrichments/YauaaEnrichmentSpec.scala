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
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.enrichments

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsEffect

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class YauaaEnrichmentSpec extends Specification with CatsEffect {
  "enrichWith" should {
    "enrich with YauaaEnrichment" in {
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/i",
        querystring = "e=pp".some,
        userAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36".some,
        headers = List(
          """Sec-CH-UA: "Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"""",
          """Sec-CH-UA-Arch: "x86"""",
          """Sec-CH-UA-Bitness: "64"""",
          """Sec-CH-UA-Full-Version: "106.0.5249.119"""",
          """Sec-CH-UA-Full-Version-List: "Chromium";v="106.0.5249.119", "Google Chrome";v="106.0.5249.119", "Not;A=Brand";v="99.0.0.0"""",
          """Sec-CH-UA-Mobile: ?0""",
          """Sec-CH-UA-Model: """"",
          """Sec-CH-UA-Platform: "Linux"""",
          """Sec-CH-ua-Platform-version: "6.0.1"""",
          """Sec-CH-UA-WoW64: ?0"""
        )
      )
      val expected = Map(
        "event_vendor" -> "com.snowplowanalytics.snowplow",
        "event_name" -> "page_ping",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event" -> "page_ping",
        "derived_contexts" -> json"""{
          "schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
          "data":[{
            "schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-5",
            "data": {
              "deviceBrand":"Unknown",
              "deviceName":"Linux Desktop",
              "operatingSystemVersionMajor":"6",
              "layoutEngineNameVersion":"Blink 106.0",
              "operatingSystemNameVersion":"Linux 6.0.1",
              "agentInformationEmail": "Unknown",
              "networkType": "Unknown",
              "operatingSystemVersionBuild":"??",
              "webviewAppNameVersion": "Unknown ??",
              "webviewAppNameVersionMajor": "Unknown ??",
              "layoutEngineNameVersionMajor":"Blink 106",
              "operatingSystemName":"Linux",
              "agentVersionMajor":"106",
              "layoutEngineVersionMajor":"106",
              "webviewAppName": "Unknown",
              "deviceClass":"Desktop",
              "agentNameVersionMajor":"Chrome 106",
              "operatingSystemNameVersionMajor":"Linux 6",
              "deviceCpuBits":"64",
              "webviewAppVersionMajor": "??",
              "operatingSystemClass":"Desktop",
              "webviewAppVersion": "??",
              "layoutEngineName":"Blink",
              "agentName":"Chrome",
              "agentVersion":"106.0.5249.119",
              "layoutEngineClass":"Browser",
              "agentNameVersion":"Chrome 106.0.5249.119",
              "operatingSystemVersion":"6.0.1",
              "deviceCpu":"Intel x86_64",
              "agentClass":"Browser",
              "layoutEngineVersion":"106.0",
              "agentInformationUrl": "Unknown",
              "deviceFirmwareVersion": "??",
              "deviceVersion": "??"
            }
          }]
        }""".noSpaces
      )
      BlackBoxTesting.runTest(input, expected, Some(YauaaEnrichmentSpec.conf))
    }
  }
}

object YauaaEnrichmentSpec {
  val conf = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow.enrichments/yauaa_enrichment_config/jsonschema/1-0-0",
      "data": {
        "enabled": true,
        "vendor": "com.snowplowanalytics.snowplow.enrichments",
        "name": "yauaa_enrichment_config"
      }
    }
  """
}
