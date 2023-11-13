/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.common
package enrichments.web

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
      Map("domain_user_id" -> "abc".some, "timestamp" -> None)
    )

  def e6 =
    CrossNavigationEnrichment.parseCrossDomain(List(("_sp" -> Some("abc.1426245561368")))).map(_.domainMap) must beRight(
      Map("domain_user_id" -> "abc".some, "timestamp" -> "2015-03-13 11:19:21.368".some)
    )

  def e7 =
    CrossNavigationEnrichment.parseCrossDomain(List(("_sp" -> Some("")))).map(_.domainMap) must beRight(Map.empty[String, Option[String]])
}
