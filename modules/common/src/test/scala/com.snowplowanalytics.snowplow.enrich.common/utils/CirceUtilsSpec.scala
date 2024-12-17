/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
