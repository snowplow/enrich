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
package com.snowplowanalytics.snowplow.enrich.common.loaders

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant

import cats.data.ValidatedNel
import cats.syntax.option._
import cats.syntax.validated._
import org.specs2.mutable.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

object LoaderSpec {
  val processor = Processor("LoaderSpec", "v1")

  val loader = new Loader[String] {
    // Make our trait whole
    override def toCollectorPayload(line: String, processor: Processor): ValidatedNel[BadRow.CPFormatViolation, Option[CollectorPayload]] =
      BadRow
        .CPFormatViolation(
          processor,
          Failure.CPFormatViolation(
            Instant.now(),
            "test",
            FailureDetails.CPFormatViolationMessage.Fallback("FAIL")
          ),
          Payload.RawPayload(line)
        )
        .invalidNel
  }
}

class LoaderSpec extends Specification with DataTables with ValidatedMatchers {
  import LoaderSpec._

  "extractGetPayload" should {
    val Encoding = UTF_8
    // TODO: add more tests
    "return a Success-boxed NonEmptyList of NameValuePairs for a valid or empty querystring" in {

      "SPEC NAME" || "QUERYSTRING" | "EXP. NEL" |
        "Simple querystring #1" !! "e=pv&dtm=1376487150616&tid=483686".some ! toNameValuePairs(
          "e" -> "pv",
          "dtm" -> "1376487150616",
          "tid" -> "483686"
        ) |
        "Simple querystring #2" !! "page=Celestial%2520Tarot%2520-%2520Psychic%2520Bazaar&vp=1097x482&ds=1097x1973".some ! toNameValuePairs(
          "page" -> "Celestial%20Tarot%20-%20Psychic%20Bazaar",
          "vp" -> "1097x482",
          "ds" -> "1097x1973"
        ) |
        "Superfluous ? ends up in first param's name" !! "?e=pv&dtm=1376487150616&tid=483686".some ! toNameValuePairs(
          "?e" -> "pv",
          "dtm" -> "1376487150616",
          "tid" -> "483686"
        ) |
        "Empty querystring" !! None ! toNameValuePairs() |> { (_, qs, expected) =>
        loader.parseQuerystring(qs, Encoding) must beRight(expected)
      }
    }

    // TODO: test invalid querystrings
  }
}
