/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.enrichments

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsEffect

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class RefererParserEnrichmentSpec extends Specification with CatsEffect {
  "enrichWith" should {
    "enrich with RefererParserEnrichment" in {
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/i",
        querystring =
          "e=pp&refr=http%3A%2F%2Fwww.google.com%2Fsearch%3Fq%3D%250Agateway%2509oracle%2509cards%2509denise%2509linn%26hl%3Den%26client%3Dsafari".some
      )
      val expected = Map(
        "event_vendor" -> "com.snowplowanalytics.snowplow",
        "event_name" -> "page_ping",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event" -> "page_ping",
        "refr_medium" -> "search",
        "refr_urlhost" -> "www.google.com",
        "refr_urlscheme" -> "http",
        "refr_urlquery" -> "q=%0Agateway%09oracle%09cards%09denise%09linn&hl=en&client=safari",
        "refr_dvce_tstamp" -> "",
        "refr_term" -> "gateway    oracle    cards    denise    linn",
        "refr_urlfragment" -> "",
        "refr_domain_userid" -> "",
        "refr_urlport" -> "80",
        "refr_urlpath" -> "/search",
        "refr_source" -> "Google"
      )
      BlackBoxTesting.runTest(input, expected, Some(RefererParserEnrichmentSpec.conf))
    }
  }
}

object RefererParserEnrichmentSpec {
  val conf = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/referer_parser/jsonschema/2-0-0",
      "data": {
        "name": "referer_parser",
        "vendor": "com.snowplowanalytics.snowplow",
        "enabled": true,
        "parameters": {
          "internalDomains": [ "www.subdomain1.snowplowanalytics.com" ],
          "database": "referers-test.json",
          "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/referer-parser"
        }
      }
    }
    """
}
