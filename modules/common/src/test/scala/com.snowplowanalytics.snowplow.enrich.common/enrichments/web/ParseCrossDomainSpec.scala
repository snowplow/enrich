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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.web

import cats.syntax.option._
import org.specs2.Specification
import org.specs2.matcher.DataTables

import com.snowplowanalytics.snowplow.badrows._

class ParseCrossDomainSpec extends Specification with DataTables {
  def is = s2"""
  parseCrossDomain should return None when the querystring is empty                             $e1
  parseCrossDomain should return None when the querystring contains no _sp parameter            $e2
  parseCrossDomain should return None when the querystring contains _sp parameter without value $e3
  parseCrossDomain should return a failure when the _sp timestamp is unparseable                $e4
  parseCrossDomain should successfully extract the domain user ID when available                $e5
  parseCrossDomain should successfully extract the domain user ID and timestamp when available  $e6
  parseCrossDomain should extract neither field from an empty _sp parameter                     $e7
  """

  def e1 =
    PageEnrichments.parseCrossDomain(Nil) must beRight((None, None))

  def e2 =
    PageEnrichments.parseCrossDomain(List(("foo" -> Some("bar")))) must beRight((None, None))

  def e3 =
    PageEnrichments.parseCrossDomain(List(("_sp" -> None))) must beRight((None, None))

  def e4 = {
    val expected = FailureDetails.EnrichmentFailure(
      None,
      FailureDetails.EnrichmentFailureMessage.InputData(
        "sp_dtm",
        "not-a-timestamp".some,
        "not in the expected format: ms since epoch"
      )
    )
    PageEnrichments.parseCrossDomain(List(("_sp" -> Some("abc.not-a-timestamp")))) must beLeft(expected)
  }

  def e5 =
    PageEnrichments.parseCrossDomain(List(("_sp" -> Some("abc")))) must beRight(("abc".some, None))

  def e6 =
    PageEnrichments.parseCrossDomain(List(("_sp" -> Some("abc.1426245561368")))) must beRight(
      ("abc".some, "2015-03-13 11:19:21.368".some)
    )

  def e7 =
    PageEnrichments.parseCrossDomain(List(("_sp" -> Some("")))) must beRight(None -> None)
}
