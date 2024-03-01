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
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.adapters

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsEffect

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class GoogleAnalyticsAdapterSpec extends Specification with CatsEffect {
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
