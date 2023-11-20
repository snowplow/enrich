/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import io.circe.literal._
import org.specs2.matcher.ValidatedMatchers
import org.specs2.mutable.Specification

class CirceUtilsSpec extends Specification with ValidatedMatchers {
  val testJson = json"""{
    "outer": "1",
    "inner": {
      "value": 2
      }
    }"""

  "Applying extractString" should {
    "successfully access an outer string field" in {
      val result = CirceUtils.extract[String](testJson, "outer")
      result must beValid("1")
    }
  }

  "Applying extractInt" should {
    "successfully access an inner string field" in {
      val result = CirceUtils.extract[Int](testJson, "inner", "value")
      result must beValid(2)
    }
  }

}
