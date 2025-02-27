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
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.adapters

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsEffect

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class MailchimpAdapterSpec extends Specification with CatsEffect {
  "enrichWith" should {
    "enrich with MailchimpAdapter" in {
      val body =
        "type=subscribe&fired_at=2014-11-04+09%3A42%3A31&data%5Bid%5D=e7c77d3852&data%5Bemail%5D=agentsmith%40snowplowtest.com&data%5Bemail_type%5D=html&data%5Bip_opt%5D=82.225.169.220&data%5Bweb_id%5D=210833825&data%5Bmerges%5D%5BEMAIL%5D=agentsmith%40snowplowtest.com&data%5Bmerges%5D%5BFNAME%5D=Agent&data%5Bmerges%5D%5BLNAME%5D=Smith&data%5Blist_id%5D=f1243a3b12"
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/com.mailchimp/v1",
        body = body.some,
        contentType = "application/x-www-form-urlencoded".some
      )
      val expected = Map(
        "v_tracker" -> "com.mailchimp-v1",
        "event_vendor" -> "com.mailchimp",
        "event_name" -> "subscribe",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event" -> "unstruct",
        "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mailchimp/subscribe/jsonschema/1-0-0","data":{"data":{"merges":{"EMAIL":"agentsmith@snowplowtest.com","FNAME":"Agent","LNAME":"Smith"},"web_id":"210833825","id":"e7c77d3852","email_type":"html","list_id":"f1243a3b12","email":"agentsmith@snowplowtest.com","ip_opt":"82.225.169.220"},"type":"subscribe","fired_at":"2014-11-04T09:42:31.000Z"}}}""".noSpaces
      )
      BlackBoxTesting.runTest(input, expected)
    }
  }
}
