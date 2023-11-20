/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import cats.syntax.either._
import org.specs2.Specification
import org.specs2.matcher.DataTables

import com.snowplowanalytics.snowplow.badrows._

class ExtractViewDimensionsSpec extends Specification with DataTables {

  val FieldName = "res"
  def err: String => FailureDetails.EnrichmentFailure =
    input =>
      FailureDetails.EnrichmentFailure(
        None,
        FailureDetails.EnrichmentFailureMessage.InputData(
          FieldName,
          Option(input),
          """does not conform to regex (\d+)x(\d+)"""
        )
      )
  def err2: String => FailureDetails.EnrichmentFailure =
    input =>
      FailureDetails.EnrichmentFailure(
        None,
        FailureDetails.EnrichmentFailureMessage.InputData(
          FieldName,
          Option(input),
          "could not be converted to java.lang.Integer s"
        )
      )

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
