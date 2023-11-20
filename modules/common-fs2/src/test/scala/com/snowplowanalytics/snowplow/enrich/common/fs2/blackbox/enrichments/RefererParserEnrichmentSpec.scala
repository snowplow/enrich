/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.enrichments

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsIO

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class RefererParserEnrichmentSpec extends Specification with CatsIO {
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
