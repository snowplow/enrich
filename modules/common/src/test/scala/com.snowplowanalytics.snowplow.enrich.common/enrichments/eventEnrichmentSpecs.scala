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
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.specs2.Specification
import org.specs2.matcher.DataTables

class ExtractEventTypeSpec extends Specification with DataTables {
  def is = s2"""
  extractEventType should return the event name for any valid event code         $e1
  extractEventType should return a validation failure for any invalid event code $e2
  formatCollectorTstamp should validate collector timestamps                     $e3
  extractTimestamp should validate timestamps                                    $e4
  """

  val FieldName = "e"
  def err: String => AtomicError =
    input => AtomicError(FieldName, Option(input), "Not a valid event type")

  def e1 =
    "SPEC NAME" || "INPUT VAL" | "EXPECTED OUTPUT" |
      "transaction" !! "tr" ! "transaction" |
      "transaction item" !! "ti" ! "transaction_item" |
      "page view" !! "pv" ! "page_view" |
      "page ping" !! "pp" ! "page_ping" |
      "unstructured event" !! "ue" ! "unstruct" |
      "structured event" !! "se" ! "struct" |
      "structured event (legacy)" !! "ev" ! "struct" |
      "ad impression (legacy)" !! "ad" ! "ad_impression" |> { (_, input, expected) =>
      EventEnrichments.extractEventType(FieldName, input) must beRight(expected)
    }

  def e2 =
    "SPEC NAME" || "INPUT VAL" | "EXPECTED OUTPUT" |
      "null" !! null ! err(null) |
      "empty string" !! "" ! err("") |
      "unrecognized #1" !! "e" ! err("e") |
      "unrecognized #2" !! "evnt" ! err("evnt") |> { (_, input, expected) =>
      EventEnrichments.extractEventType(FieldName, input) must beLeft(expected)
    }

  val SeventiesTstamp = Some(new DateTime(0, DateTimeZone.UTC))
  val BCTstamp = SeventiesTstamp.map(_.minusYears(2000))
  val FarAwayTstamp = SeventiesTstamp.map(_.plusYears(10000))
  def e3 =
// format: off
    "SPEC NAME"          || "INPUT VAL"     | "EXPECTED OUTPUT" |
    "None"               !! None            ! AtomicError("collector_tstamp", None, "Field not set").asLeft |
    "Negative timestamp" !! BCTstamp        ! AtomicError("collector_tstamp", Some("-0030-01-01T00:00:00.000Z"),"Formatted as -0030-01-01 00:00:00.000 is not Redshift-compatible").asLeft |
    ">10k timestamp"     !! FarAwayTstamp   ! AtomicError("collector_tstamp", Some("11970-01-01T00:00:00.000Z"),"Formatted as 11970-01-01 00:00:00.000 is not Redshift-compatible").asLeft |
    "Valid timestamp"    !! SeventiesTstamp ! "1970-01-01 00:00:00.000".asRight |> {
// format: on
      (_, input, expected) =>
        EventEnrichments.formatCollectorTstamp(input) must_== expected
    }

  def e4 =
    "SPEC NAME" || "INPUT VAL" | "EXPECTED OUTPUT" |
      "Not long" !! (("f", "v")) ! AtomicError("f", Some("v"), "Not in the expected format: ms since epoch").asLeft |
      "Too long" !! (("f", "1111111111111111")) ! AtomicError("f",
                                                              Some("1111111111111111"),
                                                              "Formatting as 37179-09-17 07:18:31.111 is not Redshift-compatible"
      ).asLeft |
      "Valid ts" !! (("f", "1")) ! "1970-01-01 00:00:00.001".asRight |> { (_, input, expected) =>
      EventEnrichments.extractTimestamp(input._1, input._2) must_== expected
    }
}

class DerivedTimestampSpec extends Specification with DataTables {
  def is = s2"""
  getDerivedTimestamp should correctly calculate the derived timestamp $e1"""

  def e1 =
    "SPEC NAME" || "DVCE_CREATED_TSTAMP" | "DVCE_SENT_TSTAMP" | "COLLECTOR_TSTAMP" | "TRUE_TSTAMP" | "EXPECTED DERIVED_TSTAMP" |
      "No dvce_sent_tstamp" !! "2014-04-29 12:00:54.555" ! null ! "2014-04-29 09:00:54.000" ! null ! "2014-04-29 09:00:54.000" |
      "No dvce_created_tstamp" !! null ! null ! "2014-04-29 09:00:54.000" ! null ! "2014-04-29 09:00:54.000" |
      "No collector_tstamp" !! null ! null ! null ! null ! null |
      "dvce_sent_tstamp before dvce_created_tstamp" !! "2014-04-29 09:00:54.001" ! "2014-04-29 09:00:54.000" ! "2014-04-29 09:00:54.000" ! null ! "2014-04-29 09:00:54.000" |
      "dvce_sent_tstamp after dvce_created_tstamp" !! "2014-04-29 09:00:54.000" ! "2014-04-29 09:00:54.001" ! "2014-04-29 09:00:54.000" ! null ! "2014-04-29 09:00:53.999" |
      "true_tstamp override" !! "2014-04-29 09:00:54.001" ! "2014-04-29 09:00:54.000" ! "2014-04-29 09:00:54.000" ! "2000-01-01 00:00:00.000" ! "2000-01-01 00:00:00.000" |> {

      (_, created, sent, collected, truth, expected) =>
        EventEnrichments.getDerivedTimestamp(
          Option(sent),
          Option(created),
          Option(collected),
          Option(truth)
        ) must beRight(Option(expected))
    }
}
