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

class MandrillAdapterSpec extends Specification with CatsEffect {
  "enrichWith" should {
    "enrich with MandrillAdapter" in {
      val body =
        "mandrill_events=%5B%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%20%20%22event%22%3A%20%22send%22%2C%0A%20%20%20%20%20%20%20%20%22msg%22%3A%20%7B%0A%20%20%20%20%20%20%20%20%20%20%20%20%22ts%22%3A%201365109999%2C%0A%20%20%20%20%20%20%20%20%20%20%20%20%22subject%22%3A%20%22This%20an%20example%20webhook%20message%22%2C%0A%20%20%20%20%20%20%20%20%20%20%20%20%22email%22%3A%20%22example.webhook%40mandrillapp.com%22%2C%0A%20%20%20%20%20%20%20%20%20%20%20%20%22sender%22%3A%20%22example.sender%40mandrillapp.com%22%2C%0A%20%20%20%20%20%20%20%20%20%20%20%20%22tags%22%3A%20%5B%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%22webhook-example%22%0A%20%20%20%20%20%20%20%20%20%20%20%20%5D%2C%0A%20%20%20%20%20%20%20%20%20%20%20%20%22opens%22%3A%20%5B%5D%2C%0A%20%20%20%20%20%20%20%20%20%20%20%20%22clicks%22%3A%20%5B%5D%2C%0A%20%20%20%20%20%20%20%20%20%20%20%20%22state%22%3A%20%22sent%22%2C%0A%20%20%20%20%20%20%20%20%20%20%20%20%22metadata%22%3A%20%7B%0A%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%20%22user_id%22%3A%20111%0A%20%20%20%20%20%20%20%20%20%20%20%20%7D%2C%0A%20%20%20%20%20%20%20%20%20%20%20%20%22_id%22%3A%20%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa%22%2C%0A%20%20%20%20%20%20%20%20%20%20%20%20%22_version%22%3A%20%22exampleaaaaaaaaaaaaaaa%22%0A%20%20%20%20%20%20%20%20%7D%2C%0A%20%20%20%20%20%20%20%20%22_id%22%3A%20%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa%22%2C%0A%20%20%20%20%20%20%20%20%22ts%22%3A%201415692035%0A%20%20%20%20%7D%0A%5D"
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/com.mandrill/v1",
        body = body.some,
        contentType = "application/x-www-form-urlencoded".some
      )
      val expected = Map(
        "v_tracker" -> "com.mandrill-v1",
        "event_vendor" -> "com.mandrill",
        "event_name" -> "message_sent",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-1",
        "event" -> "unstruct",
        "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mandrill/message_sent/jsonschema/1-0-1","data":{"msg":{"_version":"exampleaaaaaaaaaaaaaaa","subject":"This an example webhook message","email":"example.webhook@mandrillapp.com","state":"sent","_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa","tags":["webhook-example"],"ts":"2013-04-04T21:13:19.000Z","clicks":[],"metadata":{"user_id":111},"sender":"example.sender@mandrillapp.com","opens":[]},"_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa","ts":"2014-11-11T07:47:15.000Z"}}}""".noSpaces
      )
      BlackBoxTesting.runTest(input, expected)
    }
  }
}
