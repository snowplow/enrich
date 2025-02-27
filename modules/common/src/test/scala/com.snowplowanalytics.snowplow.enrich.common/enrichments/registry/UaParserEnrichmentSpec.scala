/**
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import java.net.URI

import org.specs2.matcher.DataTables
import org.specs2.mutable.Specification

import cats.data.EitherT
import cats.implicits._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import cats.effect.testing.specs2.CatsEffect

import io.circe.literal._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.UaParserConf

class UaParserEnrichmentSpec extends Specification with DataTables with CatsEffect {

  val mobileSafariUserAgent =
    "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3"
  val mobileSafariJson =
    SelfDescribingData(
      SchemaKey(
        "com.snowplowanalytics.snowplow",
        "ua_parser_context",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ),
      json"""{"useragentFamily":"Mobile Safari","useragentMajor":"5","useragentMinor":"1","useragentPatch":null,"useragentVersion":"Mobile Safari 5.1","osFamily":"iOS","osMajor":"5","osMinor":"1","osPatch":"1","osPatchMinor":null,"osVersion":"iOS 5.1.1","deviceFamily":"iPhone"}"""
    )

  val safariUserAgent =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25"
  val safariJson =
    SelfDescribingData(
      SchemaKey(
        "com.snowplowanalytics.snowplow",
        "ua_parser_context",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ),
      json"""{"useragentFamily":"Safari","useragentMajor":"8","useragentMinor":"0","useragentPatch":null,"useragentVersion":"Safari 8.0","osFamily":"Mac OS X","osMajor":"10","osMinor":"10","osPatch":null,"osPatchMinor":null,"osVersion":"Mac OS X 10.10","deviceFamily":"Mac"}"""
    )

  // The URI is irrelevant here, but the local file name needs to point to our test resource
  val testRulefile = getClass.getResource("uap-test-rules.yml").toURI.getPath
  val customRules = (new URI("s3://private-bucket/files/uap-rules.yml"), testRulefile)
  val testAgentJson =
    SelfDescribingData(
      SchemaKey(
        "com.snowplowanalytics.snowplow",
        "ua_parser_context",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ),
      json"""{"useragentFamily":"UAP Test Family","useragentMajor":null,"useragentMinor":null,"useragentPatch":null,"useragentVersion":"UAP Test Family","osFamily":"UAP Test OS","osMajor":null,"osMinor":null,"osPatch":null,"osPatchMinor":null,"osVersion":"UAP Test OS","deviceFamily":"UAP Test Device"}"""
    )

  val schemaKey = SchemaKey("vendor", "name", "format", SchemaVer.Full(1, 0, 0))

  val badRulefile = (new URI("s3://private-bucket/files/uap-rules.yml"), "NotAFile")

  "useragent parser" should {
    "report initialization error" in {
      "Custom Rules" | "Input UserAgent" | "Parsed UserAgent" |
        Some(badRulefile) !! mobileSafariUserAgent !! "Failed to initialize ua parser" |> { (rules, input, errorPrefix) =>
        (for {
          c <- EitherT.rightT[IO, String](UaParserConf(schemaKey, rules))
          e <- c.enrichment[IO]
          res = e.extractUserAgent(input)
        } yield res).value
          .map(_ must beLeft.like {
            case a => a must startWith(errorPrefix)
          })
          .unsafeRunSync()
      }
    }

    "parse useragent according to configured rules" in {
      "Custom Rules" | "Input UserAgent" | "Parsed UserAgent" |
        None !! mobileSafariUserAgent !! mobileSafariJson |
        None !! safariUserAgent !! safariJson |
        Some(customRules) !! mobileSafariUserAgent !! testAgentJson |> { (rules, input, expected) =>
        (for {
          c <- EitherT.rightT[IO, String](UaParserConf(schemaKey, rules))
          e <- c.enrichment[IO]
          res <- EitherT(e.extractUserAgent(input).map(_.leftMap(_.toString())))
        } yield res).value.map(_ must beRight(expected)).unsafeRunSync()
      }
    }
  }
}
