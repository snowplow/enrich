/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import org.specs2.Specification
import org.specs2.matcher.DataTables

class ValidateAndReformatJsonSpec extends Specification with DataTables {
  def is = s2"""
  extracting and reformatting (where necessary) valid JSONs with work $e1
  extracting invalid JSONs should fail $e2
  """

  def e1 =
    "SPEC NAME" || "INPUT STR" | "EXPECTED" |
      "Empty JSON" !! "{}" ! "{}" |
      "Simple JSON #1" !! """{"key":"value"}""" ! """{"key":"value"}""" |
      "Simple JSON #2" !! """[1,2,3]""" ! """[1,2,3]""" |
      "Reformatted JSON #1" !! """{ "key" : 23 }""" ! """{"key":23}""" |
      "Reformatted JSON #2" !! """[1.00, 2.00, 3.00, 4.00]""" ! """[1.00,2.00,3.00,4.00]""" |
      "Reformatted JSON #3" !! """
      {
        "a": 23
      }""" ! """{"a":23}""" |> { (_, str, expected) =>
      JsonUtils.validateAndReformatJson(str) must beRight(expected)
    }

  def err1 = s"invalid json: exhausted input"
  def err2: (String, Int, Int) => String =
    (got, line, col) => s"invalid json: expected json value got '$got' (line $line, column $col)"
  def err3: (String, Int, Int) => String =
    (got, line, col) => s"""invalid json: expected " got '$got' (line $line, column $col)"""

  def e2 =
    "SPEC NAME" || "INPUT STR" | "EXPECTED" |
      "Empty string" !! "" ! err1 |
      "Double colons" !! """{"a"::2}""" ! err2(":2}", 1, 6) |
      "Random noise" !! "^45fj_" ! err2("^45fj_", 1, 1) |
      "Bad key" !! """{9:"a"}""" ! err3("""9:"a"}""", 1, 2) |> { (_, str, expected) =>
      JsonUtils.validateAndReformatJson(str) must beLeft(expected)
    }

}
