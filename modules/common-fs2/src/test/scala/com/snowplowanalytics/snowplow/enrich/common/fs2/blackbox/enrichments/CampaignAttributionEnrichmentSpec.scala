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

class CampaignAttributionEnrichmentSpec extends Specification with CatsIO {
  "enrichWith" should {
    "enrich with CampaignAttributionEnrichment" in {
      val input = BlackBoxTesting.buildCollectorPayload(
        refererUri = "http://pb.com/?utm_source=GoogleSearch&utm_medium=cpc&utm_term=pb&utm_content=39&cid=tna&gclid=CI6".some,
        path = "/i",
        querystring = "e=pp".some
      )
      val expected = Map(
        "mkt_content" -> "39",
        "mkt_clickid" -> "CI6",
        "mkt_term" -> "pb",
        "mkt_campaign" -> "tna",
        "mkt_source" -> "GoogleSearch",
        "event_vendor" -> "com.snowplowanalytics.snowplow",
        "event_name" -> "page_ping",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event" -> "page_ping"
      )
      BlackBoxTesting.runTest(input, expected, Some(CampaignAttributionEnrichmentSpec.conf))
    }
  }
}

object CampaignAttributionEnrichmentSpec {
  val conf = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/campaign_attribution/jsonschema/1-0-1",
      "data": {
        "vendor": "com.snowplowanalytics.snowplow",
        "name": "campaign_attribution",
        "enabled": true,
        "parameters": {
          "mapping": "static",
          "fields": {
            "mktMedium": ["utm_medium", "medium"],
            "mktSource": ["utm_source", "source"],
            "mktTerm": ["utm_term", "legacy_term"],
            "mktContent": ["utm_content"],
            "mktCampaign": ["utm_campaign", "cid", "legacy_campaign"]
          }
        }
      }
    }
    """
}
