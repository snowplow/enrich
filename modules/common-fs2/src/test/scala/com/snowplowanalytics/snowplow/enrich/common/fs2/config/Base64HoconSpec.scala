/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
