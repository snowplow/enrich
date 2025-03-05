/*
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

import cats.data.EitherT
import cats.syntax.either._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import cats.effect.testing.specs2.CatsEffect

import io.circe.literal._

import org.specs2.Specification
import org.specs2.matcher.DataTables

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.refererparser._

/**
 * A small selection of tests partially borrowed from referer-parser.
 * This is a very imcomplete set - more a tripwire than an exhaustive test.
 */
class RefererParserEnrichmentSpec extends Specification with DataTables with CatsEffect {
  def is = s2"""
  parsing referer URIs should work                     $e1
  tabs and newlines in search terms should be replaced $e2
  """

  val PageHost = "www.snowplowanalytics.com"
  def e1 =
    "SPEC NAME" || "REFERER URI" | "REFERER" |
      "Google search" !! "http://www.google.com/search?q=gateway+oracle+cards+denise+linn&hl=en&client=safari" ! SearchReferer(
        Medium.Search,
        "Google",
        Some("gateway oracle cards denise linn")
      ) |
      "Facebook social" !! "http://www.facebook.com/l.php?u=http%3A%2F%2Fwww.psychicbazaar.com&h=yAQHZtXxS&s=1" ! SocialReferer(
        Medium.Social,
        "Facebook"
      ) |
      "Yahoo! Mail" !! "http://36ohk6dgmcd1n-c.c.yom.mail.yahoo.net/om/api/1.0/openmail.app.invoke/36ohk6dgmcd1n/11/1.0.35/us/en-US/view.html/0" ! EmailReferer(
        Medium.Email,
        "Yahoo! Mail"
      ) |
      "ChatGPT" !! "https://www.chatgpt.com" ! ChatbotReferer(
        Medium.Chatbot,
        "ChatGPT"
      ) |
      "Internal referer" !! "https://www.snowplowanalytics.com/account/profile" ! InternalReferer(
        Medium.Internal
      ) |
      "Custom referer" !! "https://www.internaldomain.com/path" ! InternalReferer(Medium.Internal) |
      "Unknown referer" !! "http://www.spyfu.com/domain.aspx?d=3897225171967988459" ! UnknownReferer(
        Medium.Unknown
      ) |> { (_, refererUri, referer) =>
      (for {
        c <- EitherT.fromEither[IO](
               RefererParserEnrichment
                 .parse(
                   json"""{
              "name": "referer_parser",
              "vendor": "com.snowplowanalytics.snowplow",
              "enabled": true,
              "parameters": {
                "internalDomains": [ "www.internaldomain.com" ],
                "uri": "http://snowplow.com",
                "database": "referer-tests.json"
              }
            }""",
                   SchemaKey(
                     "com.snowplowanalytics.snowplow",
                     "referer_parser",
                     "jsonschema",
                     SchemaVer.Full(2, 0, 0)
                   ),
                   true
                 )
                 .toEither
                 .leftMap(_.head)
             )
        e <- c.enrichment[IO]
        res = e.extractRefererDetails(new URI(refererUri), PageHost)
      } yield res).value
        .map(_ must beRight.like {
          case o => o must beSome(referer)
        })
        .unsafeRunSync()
    }

  def e2 =
    (for {
      c <- EitherT.fromEither[IO](
             RefererParserEnrichment
               .parse(
                 json"""{
            "name": "referer_parser",
            "vendor": "com.snowplowanalytics.snowplow",
            "enabled": true,
            "parameters": {
              "internalDomains": [],
              "uri": "http://snowplow.com",
              "database": "referer-tests.json"
            }
          }""",
                 SchemaKey(
                   "com.snowplowanalytics.snowplow",
                   "referer_parser",
                   "jsonschema",
                   SchemaVer.Full(2, 0, 0)
                 ),
                 true
               )
               .toEither
               .leftMap(_.head)
           )
      e <- c.enrichment[IO]
      res = e.extractRefererDetails(
              new URI(
                "http://www.google.com/search?q=%0Agateway%09oracle%09cards%09denise%09linn&hl=en&client=safari"
              ),
              PageHost
            )
    } yield res).value.map(_ must beRight.like {
      case o =>
        o must beSome(
          SearchReferer(
            Medium.Search,
            "Google",
            Some("gateway    oracle    cards    denise    linn")
          )
        )
    })
}
