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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import cats.syntax.either._
import cats.syntax.option._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.badrows._

import io.circe.Json
import io.circe.syntax._

import org.specs2.mutable.Specification
import org.specs2.matcher.EitherMatchers

class CrossNavigationEnrichmentSpec extends Specification with EitherMatchers {
  import CrossNavigationEnrichment._

  val schemaKey = SchemaKey(
    CrossNavigationEnrichment.supportedSchema.vendor,
    CrossNavigationEnrichment.supportedSchema.name,
    CrossNavigationEnrichment.supportedSchema.format,
    SchemaVer.Full(1, 0, 0)
  )

  "makeCrossDomainMap" should {
    "return expected Map on original format" >> {
      val input = "abc.1697398398279"
      val expectedOut: Map[String, Option[String]] = Map(
        "domain_user_id" -> Some("abc"),
        "timestamp" -> Some("2023-10-15 19:33:18.279"),
        "session_id" -> None,
        "user_id" -> None,
        "source_id" -> None,
        "source_platform" -> None,
        "reason" -> None
      )
      val result = CrossDomainMap.makeCrossDomainMap(input).map(_.domainMap)
      result must beEqualTo(expectedOut.asRight)
    }

    "return expected Map on original format when missing timestamp" >> {
      val input = "abc"
      val expectedOut: Map[String, Option[String]] = Map(
        "domain_user_id" -> Some("abc"),
        "timestamp" -> None,
        "session_id" -> None,
        "user_id" -> None,
        "source_id" -> None,
        "source_platform" -> None,
        "reason" -> None
      )
      val result = CrossDomainMap.makeCrossDomainMap(input).map(_.domainMap)
      result must beEqualTo(expectedOut.asRight)
    }

    "return expected Map on original format when missing duid" >> {
      val input = ".1697398398279"
      val expectedOut: Map[String, Option[String]] = Map(
        "domain_user_id" -> None,
        "timestamp" -> Some("2023-10-15 19:33:18.279"),
        "session_id" -> None,
        "user_id" -> None,
        "source_id" -> None,
        "source_platform" -> None,
        "reason" -> None
      )
      val result = CrossDomainMap.makeCrossDomainMap(input).map(_.domainMap)
      result must beEqualTo(expectedOut.asRight)
    }

    "return expected Map on extended format" >> {
      val input = "abc.1697175843762.176ff68a-4769-4566-ad0e-3792c1c8148f.dGVzdGVy.c29tZVNvdXJjZUlk.web.dGVzdGluZ19yZWFzb24"
      val expectedOut: Map[String, Option[String]] = Map(
        "domain_user_id" -> Some("abc"),
        "timestamp" -> Some("2023-10-13 05:44:03.762"),
        "session_id" -> Some("176ff68a-4769-4566-ad0e-3792c1c8148f"),
        "user_id" -> Some("tester"),
        "source_id" -> Some("someSourceId"),
        "source_platform" -> Some("web"),
        "reason" -> Some("testing_reason")
      )
      val result = CrossDomainMap.makeCrossDomainMap(input).map(_.domainMap)
      result must beEqualTo(expectedOut.asRight)
    }

    "return expected Map on extended format when missing timestamp" >> {
      val input = "abc..176ff68a-4769-4566-ad0e-3792c1c8148f.dGVzdGVy.c29tZVNvdXJjZUlk.web.dGVzdGluZ19yZWFzb24"
      val expectedOut: Map[String, Option[String]] = Map(
        "domain_user_id" -> Some("abc"),
        "timestamp" -> None,
        "session_id" -> Some("176ff68a-4769-4566-ad0e-3792c1c8148f"),
        "user_id" -> Some("tester"),
        "source_id" -> Some("someSourceId"),
        "source_platform" -> Some("web"),
        "reason" -> Some("testing_reason")
      )
      val result = CrossDomainMap.makeCrossDomainMap(input).map(_.domainMap)
      result must beEqualTo(expectedOut.asRight)
    }

    "return expected Map on extended format when missing duid" >> {
      val input = "..176ff68a-4769-4566-ad0e-3792c1c8148f.dGVzdGVy.c29tZVNvdXJjZUlk.web.dGVzdGluZ19yZWFzb24"
      val expectedOut: Map[String, Option[String]] = Map(
        "domain_user_id" -> None,
        "timestamp" -> None,
        "session_id" -> Some("176ff68a-4769-4566-ad0e-3792c1c8148f"),
        "user_id" -> Some("tester"),
        "source_id" -> Some("someSourceId"),
        "source_platform" -> Some("web"),
        "reason" -> Some("testing_reason")
      )
      val result = CrossDomainMap.makeCrossDomainMap(input).map(_.domainMap)
      result must beEqualTo(expectedOut.asRight)
    }

    "handle variations of extended format 1" >> {
      val input = "abc.1697175843762..dGVzdGVy..web"
      val expectedOut: Map[String, Option[String]] = Map(
        "domain_user_id" -> Some("abc"),
        "timestamp" -> Some("2023-10-13 05:44:03.762"),
        "session_id" -> None,
        "user_id" -> Some("tester"),
        "source_id" -> None,
        "source_platform" -> Some("web"),
        "reason" -> None
      )
      val result = CrossDomainMap.makeCrossDomainMap(input).map(_.domainMap)
      result must beEqualTo(expectedOut.asRight)
    }

    "handle variations of extended format 2" >> {
      val input = "abc..176ff68a-4769-4566-ad0e-3792c1c8148f.."
      val expectedOut: Map[String, Option[String]] = Map(
        "domain_user_id" -> Some("abc"),
        "timestamp" -> None,
        "session_id" -> Some("176ff68a-4769-4566-ad0e-3792c1c8148f"),
        "user_id" -> None,
        "source_id" -> None,
        "source_platform" -> None,
        "reason" -> None
      )
      val result = CrossDomainMap.makeCrossDomainMap(input).map(_.domainMap)
      result must beEqualTo(expectedOut.asRight)
    }

    "handle variations of extended format 3" >> {
      val input = "abc.1697175843762....."
      val expectedOut: Map[String, Option[String]] = Map(
        "domain_user_id" -> Some("abc"),
        "timestamp" -> Some("2023-10-13 05:44:03.762"),
        "session_id" -> None,
        "user_id" -> None,
        "source_id" -> None,
        "source_platform" -> None,
        "reason" -> None
      )
      val result = CrossDomainMap.makeCrossDomainMap(input).map(_.domainMap)
      result must beEqualTo(expectedOut.asRight)
    }

    "return empty map on invalid format" >> {
      val input = "abc.1697175843762.176ff68a-4769-4566-ad0e-3792c1c8148f.dGVzdGVy.c29tZVNvdXJjZUlk.web.dGVzdGluZ19yZWFzb24...foo..bar..."
      val expectedOut = Map.empty[String, Option[String]]
      val result = CrossDomainMap.makeCrossDomainMap(input).map(_.domainMap)
      result must beEqualTo(expectedOut.asRight)
    }

    "return failure on invalid timestamp" >> {
      val input = "abc.not-timestamp.176ff68a-4769-4566-ad0e-3792c1c8148f."
      val expectedOut = FailureDetails.EnrichmentFailure(
        None,
        FailureDetails.EnrichmentFailureMessage.InputData(
          "sp_dtm",
          "not-timestamp".some,
          "Not in the expected format: ms since epoch"
        )
      )
      val result = CrossDomainMap.makeCrossDomainMap(input).map(_.domainMap)
      result must beEqualTo(expectedOut.asLeft)
    }

    "return failure on incompatible timestamp" >> {
      val input = "abc.1111111111111111.176ff68a-4769-4566-ad0e-3792c1c8148f."
      val expectedOut = FailureDetails.EnrichmentFailure(
        None,
        FailureDetails.EnrichmentFailureMessage.InputData(
          "sp_dtm",
          "1111111111111111".some,
          "Formatting as 37179-09-17 07:18:31.111 is not Redshift-compatible"
        )
      )
      val result = CrossDomainMap.makeCrossDomainMap(input).map(_.domainMap)
      result must beEqualTo(expectedOut.asLeft)
    }
  }

  "getCrossNavigationContext" should {
    "return Nil if input is empty Map" >> {
      val input = Map.empty[String, Option[String]]
      val expectedOut: List[SelfDescribingData[Json]] = Nil
      val result = CrossDomainMap(input).getCrossNavigationContext
      result must beEqualTo(expectedOut)
    }

    "return Nil if missing domain_user_id (required)" >> {
      val input: Map[String, Option[String]] = Map(
        "timestamp" -> Some("2023-10-13 05:44:03.762")
      )
      val expectedOut: List[SelfDescribingData[Json]] = Nil
      val result = CrossDomainMap(input).getCrossNavigationContext
      result must beEqualTo(expectedOut)
    }

    "return Nil if missing timestamp (required)" >> {
      val input: Map[String, Option[String]] = Map(
        "domain_user_id" -> Some("abcd")
      )
      val expectedOut: List[SelfDescribingData[Json]] = Nil
      val result = CrossDomainMap(input).getCrossNavigationContext
      result must beEqualTo(expectedOut)
    }

    "return List of SelfDescribingData" >> {
      val input: Map[String, Option[String]] = Map(
        "domain_user_id" -> Some("abc"),
        "timestamp" -> Some("2023-10-13 05:44:03.762"),
        "session_id" -> Some("176ff68a-4769-4566-ad0e-3792c1c8148f"),
        "user_id" -> Some("tester"),
        "source_id" -> Some("someSourceId"),
        "source_platform" -> Some("web"),
        "reason" -> Some("testing_reason")
      )
      val expectedOut: List[SelfDescribingData[Json]] = List(
        SelfDescribingData(
          CrossNavigationEnrichment.outputSchema,
          Map(
            "domain_user_id" -> Some("abc"),
            "timestamp" -> Some("2023-10-13T05:44:03.762Z"),
            "session_id" -> Some("176ff68a-4769-4566-ad0e-3792c1c8148f"),
            "user_id" -> Some("tester"),
            "source_id" -> Some("someSourceId"),
            "source_platform" -> Some("web"),
            "reason" -> Some("testing_reason")
          ).asJson
        )
      )
      val result = CrossDomainMap(input).getCrossNavigationContext
      result must beEqualTo(expectedOut)
    }
  }

  "addEnrichmentInfo" should {
    val cne = new CrossNavigationEnrichment(schemaKey)

    "add the cross-navigation enrichment info" >> {
      val input = FailureDetails.EnrichmentFailure(
        None,
        FailureDetails.EnrichmentFailureMessage.InputData(
          "some_field",
          Some("some_value"),
          "some error message"
        )
      )
      val expectedOut = FailureDetails.EnrichmentFailure(
        FailureDetails.EnrichmentInformation(schemaKey, "cross-navigation").some,
        FailureDetails.EnrichmentFailureMessage.InputData(
          "some_field",
          Some("some_value"),
          "some error message"
        )
      )
      val result = cne.addEnrichmentInfo(input)
      result must beEqualTo(expectedOut)
    }
  }
}
