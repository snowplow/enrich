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

import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.CrossNavigationEnrichment

import org.specs2.Specification
import org.specs2.matcher.DataTables

class ParseCrossDomainSpec extends Specification with DataTables {
  def is = s2"""
  parseCrossDomain should return empty Map when the querystring is empty                             $e1
  parseCrossDomain should return empty Map when the querystring contains no _sp parameter            $e2
  parseCrossDomain should return empty Map when the querystring contains _sp parameter without value $e3
  parseCrossDomain should return a failure when the _sp timestamp is unparseable                $e4
  parseCrossDomain should successfully extract the domain user ID when available                $e5
  parseCrossDomain should successfully extract the domain user ID and timestamp when available  $e6
  parseCrossDomain should extract neither field from an empty _sp parameter                     $e7
  """

  def e1 =
    CrossNavigationEnrichment.parseCrossDomain(Nil).map(_.domainMap) must beRight(Map.empty[String, Option[String]])

  def e2 =
    CrossNavigationEnrichment.parseCrossDomain(List(("foo" -> Some("bar")))).map(_.domainMap) must beRight(
      Map.empty[String, Option[String]]
    )

  def e3 =
    CrossNavigationEnrichment.parseCrossDomain(List(("_sp" -> None))).map(_.domainMap) must beRight(Map.empty[String, Option[String]])

  def e4 = {
    val expected = FailureDetails.EnrichmentFailure(
      None,
      FailureDetails.EnrichmentFailureMessage.InputData(
        "sp_dtm",
        "not-a-timestamp".some,
        "not in the expected format: ms since epoch"
      )
    )
    CrossNavigationEnrichment.parseCrossDomain(List(("_sp" -> Some("abc.not-a-timestamp")))).map(_.domainMap) must beLeft(expected)
  }

  def e5 =
    CrossNavigationEnrichment.parseCrossDomain(List(("_sp" -> Some("abc")))).map(_.domainMap) must beRight(
      Map(
        "domain_user_id" -> "abc".some,
        "timestamp" -> None,
        "session_id" -> None,
        "user_id" -> None,
        "source_id" -> None,
        "source_platform" -> None,
        "reason" -> None
      )
    )

  def e6 =
    CrossNavigationEnrichment.parseCrossDomain(List(("_sp" -> Some("abc.1426245561368")))).map(_.domainMap) must beRight(
      Map(
        "domain_user_id" -> "abc".some,
        "timestamp" -> "2015-03-13 11:19:21.368".some,
        "session_id" -> None,
        "user_id" -> None,
        "source_id" -> None,
        "source_platform" -> None,
        "reason" -> None
      )
    )

  def e7 =
    CrossNavigationEnrichment.parseCrossDomain(List(("_sp" -> Some("")))).map(_.domainMap) must beRight(Map.empty[String, Option[String]])
}
