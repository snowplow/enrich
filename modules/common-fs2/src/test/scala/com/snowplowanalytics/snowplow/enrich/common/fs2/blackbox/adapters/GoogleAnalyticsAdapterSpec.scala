/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.adapters

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsIO

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class GoogleAnalyticsAdapterSpec extends Specification with CatsIO {
  "enrichWith" should {
    "enrich with GoogleAnalyticsAdapter" in {
      val body = "t=pageview&dh=host&dp=path"
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/com.google.analytics/v1",
        body = body.some
      )
      val expected = Map(
        "v_tracker" -> "com.google.analytics.measurement-protocol-v1",
        "event_vendor" -> "com.google.analytics.measurement-protocol",
        "event_name" -> "page_view",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event" -> "unstruct",
        "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0","data":{"documentHostName":"host","documentPath":"path"}}}""".noSpaces
      )
      BlackBoxTesting.runTest(input, expected)
    }
  }
}
