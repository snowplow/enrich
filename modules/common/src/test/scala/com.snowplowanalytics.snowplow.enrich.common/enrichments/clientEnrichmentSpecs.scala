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
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import cats.syntax.either._
import org.specs2.Specification
import org.specs2.matcher.DataTables

import com.snowplowanalytics.snowplow.enrich.common.utils.AtomicError

class ExtractViewDimensionsSpec extends Specification with DataTables {

  val FieldName = "res"
  def err(value: String): AtomicError =
    AtomicError.ParseError("""Does not conform to regex (\d+)x(\d+)""", FieldName, Option(value))

  def err2(value: String): AtomicError =
    AtomicError.ParseError("Could not be converted to java.lang.Integer s", FieldName, Option(value))

  def is = s2"""
  Extracting screen dimensions (viewports, screen resolution etc) with extractViewDimensions should work $e1"""

  def e1 =
    "SPEC NAME" || "INPUT VAL" | "EXPECTED OUTPUT" |
      "valid desktop" !! "1200x800" ! (1200, 800).asRight |
      "valid mobile" !! "76x128" ! (76, 128).asRight |
      "invalid empty" !! "" ! err("").asLeft |
      "invalid null" !! null ! err(null).asLeft |
      "invalid hex" !! "76xEE" ! err("76xEE").asLeft |
      "invalid negative" !! "1200x-17" ! err("1200x-17").asLeft |
      "Arabic number" !! "٤٥٦٧x680" ! err("٤٥٦٧x680").asLeft |
      "number > int #1" !! "760x3389336768" ! err2("760x3389336768").asLeft |
      "number > int #2" !! "9989336768x1200" ! err2("9989336768x1200").asLeft |> { (_, input, expected) =>
      ClientEnrichments.extractViewDimensions(FieldName, input) must_== expected
    }
}
