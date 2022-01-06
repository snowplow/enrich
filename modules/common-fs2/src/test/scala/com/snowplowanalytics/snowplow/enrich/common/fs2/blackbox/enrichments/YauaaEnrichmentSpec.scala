/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.enrichments

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsIO

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class YauaaEnrichmentSpec extends Specification with CatsIO {
  "enrichWith" should {
    "enrich with YauaaEnrichment" in {
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/i",
        querystring = "e=pp".some,
        userAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0".some
      )
      val expected = Map(
        "event_vendor" -> "com.snowplowanalytics.snowplow",
        "event_name" -> "page_ping",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event" -> "page_ping",
        "derived_contexts" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-3","data":{"deviceBrand":"Unknown","deviceName":"Desktop","operatingSystemVersionMajor":"7","layoutEngineNameVersion":"Gecko 12.0","operatingSystemNameVersion":"Windows 7","layoutEngineBuild":"20100101","layoutEngineNameVersionMajor":"Gecko 12","operatingSystemName":"Windows NT","agentVersionMajor":"12","layoutEngineVersionMajor":"12","deviceClass":"Desktop","agentNameVersionMajor":"Firefox 12","operatingSystemNameVersionMajor":"Windows 7","deviceCpuBits":"64","operatingSystemClass":"Desktop","layoutEngineName":"Gecko","agentName":"Firefox","agentVersion":"12.0","layoutEngineClass":"Browser","agentNameVersion":"Firefox 12.0","operatingSystemVersion":"7","deviceCpu":"Intel x86_64","agentClass":"Browser","layoutEngineVersion":"12.0"}}]}""".noSpaces
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
