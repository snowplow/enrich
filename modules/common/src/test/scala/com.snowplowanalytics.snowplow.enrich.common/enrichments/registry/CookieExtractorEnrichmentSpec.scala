/**
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 * | *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import io.circe.literal._

import org.specs2.Specification

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

class CookieExtractorEnrichmentSpec extends Specification {
  def is = s2"""
  returns an empty list when no cookie header                $e1
  returns an empty list when no cookie matches configuration $e2
  returns contexts for found cookies                         $e3
  """

  def e1 = {
    val actual = CookieExtractorEnrichment(List("cookieKey1")).extract(List("Content-Length: 348"))
    actual must_== Nil
  }

  def e2 = {
    val actual = CookieExtractorEnrichment(List("cookieKey1"))
      .extract(List("Cookie: not-interesting-cookie=1234;"))
    actual must_== Nil
  }

  def e3 = {
    val cookies = List("ck1", "=cv2", "ck3=", "ck4=cv4", "ck5=\"cv5\"")
    val cookieKeys = List("ck1", "", "ck3", "ck4", "ck5")

    val expected =
      List(
        SelfDescribingData(
          SchemaKey("org.ietf", "http_cookie", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"name":"ck1","value":null}"""
        ),
        SelfDescribingData(
          SchemaKey("org.ietf", "http_cookie", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"name":"","value":"cv2"}"""
        ),
        SelfDescribingData(
          SchemaKey("org.ietf", "http_cookie", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"name":"ck3","value":""}"""
        ),
        SelfDescribingData(
          SchemaKey("org.ietf", "http_cookie", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"name":"ck4","value":"cv4"}"""
        ),
        SelfDescribingData(
          SchemaKey("org.ietf", "http_cookie", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"name":"ck5","value":"cv5"}"""
        )
      )

    val actual = CookieExtractorEnrichment(cookieKeys)
      .extract(List("Cookie: " + cookies.mkString(";")))

    actual must beLike {
      case cookies @ _ :: _ :: _ :: _ :: _ :: Nil => cookies must_== expected
    }
  }
}
