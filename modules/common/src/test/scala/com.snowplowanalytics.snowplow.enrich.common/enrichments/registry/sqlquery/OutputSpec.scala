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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery

import java.sql.Date

import io.circe._
import org.joda.time.DateTime
import org.specs2.Specification

class OutputSpec extends Specification {
  def is = s2"""
  Parse Integer without type hint        $e1
  Parse Double without type hint         $e2
  Handle null                            $e3
  Handle java.sql.Date as ISO8601 string $e4
  """

  def e1 =
    JsonOutput.getValue(1: Integer, "") must beEqualTo(Json.fromInt(1))

  def e2 =
    JsonOutput.getValue(32.2: java.lang.Double, "") must beEqualTo(Json.fromDoubleOrNull(32.2))

  def e3 =
    JsonOutput.getValue(null, "") must beEqualTo(Json.Null)

  def e4 = {
    val date = new Date(1465558727000L)
    JsonOutput.getValue(date, "java.sql.Date") must
      beEqualTo(Json.fromString(new DateTime(date).toString))
  }
}
