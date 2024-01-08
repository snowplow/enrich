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

package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest

import io.circe.Json
import io.circe.literal.JsonStringContext
import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers
import org.specs2.specification.core.SpecStructure

import com.snowplowanalytics.iglu.client.CirceValidator

import com.snowplowanalytics.snowplow.enrich.common.utils.JsonPath.query

class ValidatorSpec extends Specification with ValidatedMatchers {
  override def is: SpecStructure = s2"""
    validate integer field using a valid long value (maximum long)               $e1
    validate integer field using a valid long value (minimum long)               $e2
    validate number field using a positive float value                           $e3
    validate number field using a negative float value                           $e4
    validate number field using a negative double value                          $e5
    validate number field using a positive double value                          $e6
    invalidate integer field using a positive double value                       $e7
  """

  val schema =
    json"""{ "type": "object", "properties": { "orderID": { "type": "integer" }, "price": { "type": "number" } }, "additionalProperties": false }"""

  def e1 =
    query("$", json"""{"orderID": 9223372036854775807 }""")
      .flatMap(fb => CirceValidator.validate(fb.head, schema)) must beRight

  def e2 =
    query("$", json"""{"orderID": -9223372036854775808 }""")
      .flatMap(fb => CirceValidator.validate(fb.head, schema)) must beRight

  def e3 =
    query("$", json"""{"price": ${Json.fromFloatOrString(88.92f)} }""")
      .flatMap(fb => CirceValidator.validate(fb.head, schema)) must beRight

  def e4 =
    query("$", json"""{"price": ${Json.fromFloatOrString(-34345328.72f)} }""")
      .flatMap(fb => CirceValidator.validate(fb.head, schema)) must beRight

  def e5 =
    query("$", json"""{"price": ${Json.fromDoubleOrString(-34345488.72)} }""")
      .flatMap(fb => CirceValidator.validate(fb.head, schema)) must beRight

  def e6 =
    query("$", json"""{"price": ${Json.fromDoubleOrString(32488.72)} }""")
      .flatMap(fb => CirceValidator.validate(fb.head, schema)) must beRight

  def e7 =
    query("$", json"""{"orderID": ${Json.fromDoubleOrString(32488.72)} }""")
      .flatMap(fb => CirceValidator.validate(fb.head, schema)) must beLeft
}
