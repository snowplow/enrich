/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.config

import java.util.Base64
import java.nio.charset.StandardCharsets

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsIO

class CliConfigSpec extends Specification with CatsIO {
  "parseHocon" should {
    "parse valid HOCON" in {
      val string = """
           "input": {
             "type": "PubSub"
              "subscription": "projects/test-project/subscriptions/collector-payloads-sub"
           }
          """.stripMargin
      val b64Encoded = Base64.getEncoder.encodeToString(string.getBytes(StandardCharsets.UTF_8))
      Base64Hocon.parseHocon(b64Encoded) must beRight
    }
  }
}
