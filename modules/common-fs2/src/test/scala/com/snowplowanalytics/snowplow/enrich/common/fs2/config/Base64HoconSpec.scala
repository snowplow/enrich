/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.config

import java.util.Base64.getEncoder

import com.monovore.decline.Argument

import org.specs2.mutable.Specification

class Base64HoconSpec extends Specification {
  "Argument[Base64Hocon]" should {
    "parse a base64-encoded HOCON as a source" in {
      val inputStr = """input = {}"""
      val input = getEncoder.encodeToString(inputStr.getBytes())
      Argument[Base64Hocon].read(input).toEither must beRight
    }

    "not parse any other base-64 encoded string as a HOCON source" in {
      val inputStr = "xyz"
      val input = getEncoder.encodeToString(inputStr.getBytes())
      Argument[Base64Hocon].read(input).toEither must beLeft
    }
  }
}
