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

import cats.effect.testing.specs2.CatsIO

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class PingdomAdapterSpec extends Specification with CatsIO {
  "enrichWith" should {
    "enrich with PingdomAdapter" in {
      val querystring =
        "p=srv&message=%7B%22check%22%3A%20%221421338%22%2C%20%22checkname%22%3A%20%22Webhooks_Test%22%2C%20%22host%22%3A%20%227eef51c2.ngrok.com%22%2C%20%22action%22%3A%20%22assign%22%2C%20%22incidentid%22%3A%203%2C%20%22description%22%3A%20%22down%22%7D"
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/com.pingdom/v1",
        querystring = querystring.some
      )
      val expected = Map(
        "v_tracker" -> "com.pingdom-v1",
        "event_vendor" -> "com.pingdom",
        "event_name" -> "incident_assign",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event" -> "unstruct",
        "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.pingdom/incident_assign/jsonschema/1-0-0","data":{"check":"1421338","checkname":"Webhooks_Test","host":"7eef51c2.ngrok.com","incidentid":3,"description":"down"}}}""".noSpaces
      )
      BlackBoxTesting.runTest(input, expected)
    }
  }
}
