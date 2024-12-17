/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.config

import java.util.Base64
import java.nio.charset.StandardCharsets

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsEffect

class CliConfigSpec extends Specification with CatsEffect {
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
