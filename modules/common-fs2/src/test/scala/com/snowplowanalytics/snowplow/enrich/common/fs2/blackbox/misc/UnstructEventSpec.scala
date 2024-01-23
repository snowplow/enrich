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
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.misc

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsEffect

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class UnstructEventSpec extends Specification with CatsEffect {
  "enrichWith" should {
    "enrich unstruct events" in {
      val querystring =
        "e=ue&ue_pr=%7B%22schema%22%3A%22iglu%3Acom.snowplowanalytics.snowplow%2Funstruct_event%2Fjsonschema%2F1-0-0%22%2C%22data%22%3A%7B%22schema%22%3A%22iglu%3Acom.snowplowanalytics.snowplow.input-adapters%2Fsegment_webhook_config%2Fjsonschema%2F1-0-0%22%2C%22data%22%3A%7B%22vendor%22%3A%22%CE%A7%CE%B1%CF%81%CE%B9%CF%84%CE%AF%CE%BD%CE%B7%20NEW%20Unicode%20test%22%2C%22name%22%3A%22alex%2Btest%40snowplowanalytics.com%22%2C%22parameters%22%3A%7B%22mappings%22%3A%7B%22eventsPerMonth%22%3A%22%3C%201%20million%22%2C%22serviceType%22%3A%22unsure%22%7D%7D%7D%7D%7D&evn=com.acme"
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/ice.png",
        querystring = querystring.some
      )
      val expected = Map(
        "event_vendor" -> "com.snowplowanalytics.snowplow.input-adapters",
        "event_name" -> "segment_webhook_config",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event" -> "unstruct",
        "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow.input-adapters/segment_webhook_config/jsonschema/1-0-0","data":{"vendor":"Χαριτίνη NEW Unicode test","name":"alex+test@snowplowanalytics.com","parameters":{"mappings":{"eventsPerMonth":"< 1 million","serviceType":"unsure"}}}}}""".noSpaces
      )
      BlackBoxTesting.runTest(input, expected)
    }
  }
}
