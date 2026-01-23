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
  """

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
}
