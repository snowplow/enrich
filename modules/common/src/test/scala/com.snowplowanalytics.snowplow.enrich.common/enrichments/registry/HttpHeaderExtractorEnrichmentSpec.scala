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

import io.circe._
import io.circe.literal._

import org.specs2.Specification

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

class HttpHeaderExtractorEnrichmentSpec extends Specification {
  def is = s2"""
  returns X-Forwarded-For header             $e1
  returns Accept header after Regex matching $e2
  no headers                                 $e3
  excludes cookie in any case with ^(?!Cookie$$).*         $e4
  excludes cookie in any case with ^(?![Cc]ookie).*        $e5
  returns x-forwarded-for in any case with X-Forwarded-For $e6
  """

  private val schemaKey = SchemaKey("org.ietf", "http_header", "jsonschema", SchemaVer.Full(1, 0, 0))

  private val headers = List(
    "Cookie: sp=abc; session=123",
    "cookie: sp=abc; session=123",
    "COOKIE: sp=abc; session=123",
    "Host: example.com",
    "accept: text/html",
    "User-Agent: Mozilla/5.0"
  )

  private val expectedNonCookie = List(
    json"""{"name":"Host","value":"example.com"}""",
    json"""{"name":"accept","value":"text/html"}""",
    json"""{"name":"User-Agent","value":"Mozilla/5.0"}"""
  ).map(SelfDescribingData(schemaKey, _))

  def e1 = {
    val expected = List(
      SelfDescribingData(
        SchemaKey("org.ietf", "http_header", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"name":"X-Forwarded-For","value":"129.78.138.66, 129.78.64.103"}"""
      )
    )
    HttpHeaderExtractorEnrichment("X-Forwarded-For".r)
      .extract(List("X-Forwarded-For: 129.78.138.66, 129.78.64.103")) must_== expected
  }

  def e2 = {
    val expected = List(
      SelfDescribingData(
        SchemaKey("org.ietf", "http_header", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"name":"Accept","value":"text/html"}"""
      )
    )
    HttpHeaderExtractorEnrichment(".*".r).extract(List("Accept: text/html")) must_== expected
  }

  def e3 = {
    val expected = List.empty[Json]
    HttpHeaderExtractorEnrichment(".*".r).extract(Nil) must_== expected
  }

  def e4 = HttpHeaderExtractorEnrichment("^(?!Cookie$).*".r).extract(headers) must_== expectedNonCookie

  def e5 = HttpHeaderExtractorEnrichment("^(?![Cc]ookie).*".r).extract(headers) must_== expectedNonCookie

  def e6 = {
    val expected = List(
      json"""{"name":"X-Forwarded-For","value":"129.78.138.66"}""",
      json"""{"name":"x-forwarded-for","value":"129.78.64.103"}""",
      json"""{"name":"X-FORWARDED-FOR","value":"129.78.64.104"}"""
    ).map(SelfDescribingData(schemaKey, _))
    HttpHeaderExtractorEnrichment("X-Forwarded-For".r)
      .extract(
        List(
          "X-Forwarded-For: 129.78.138.66",
          "x-forwarded-for: 129.78.64.103",
          "X-FORWARDED-FOR: 129.78.64.104",
          "Host: example.com"
        )
      ) must_== expected
  }
}
