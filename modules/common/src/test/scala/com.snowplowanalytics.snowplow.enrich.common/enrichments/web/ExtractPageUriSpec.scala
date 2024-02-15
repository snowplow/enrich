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

import java.net.URI

import cats.syntax.option._
import org.specs2.Specification
import org.specs2.matcher.DataTables

class ExtractPageUriSpec extends Specification with DataTables {
  def is = s2"""
  extractPageUri should return a None when no page URI provided                             $e1
  extractPageUri should choose the URI from the tracker if it has one or two to choose from $e2
  extractPageUri will alas assume a browser-truncated page URL is a custom URL not an error $e3
  """

  // No URI
  def e1 =
    PageEnrichments.extractPageUri(None, None) must beRight[Option[URI]](None)

  // Valid URI combinations
  val originalUri = "http://www.mysite.com/shop/session/_internal/checkout"
  val customUri = "http://www.mysite.com/shop/checkout" // E.g. set by setCustomUrl in JS Tracker
  val originalURI = new URI(originalUri)
  val customURI = new URI(customUri)

  def e2 =
    "SPEC NAME" || "URI TAKEN FROM COLLECTOR'S REFERER" | "URI SENT BY TRACKER" | "EXPECTED URI" |
      "both URIs match (98% of the time)" !! originalUri.some ! originalUri.some ! originalURI.some |
      "tracker didn't send URI (e.g. No-JS Tracker)" !! originalUri.some ! None ! originalURI.some |
      "collector didn't record the referer (rare)" !! None ! originalUri.some ! originalURI.some |
      "collector and tracker URIs differ - use tracker" !! originalUri.some ! customUri.some ! customURI.some |> {

      (_, fromReferer, fromTracker, expected) =>
        PageEnrichments.extractPageUri(fromReferer, fromTracker) must beRight(expected)
    }

  // Truncate
  val truncatedUri = originalUri.take(originalUri.length - 5)
  val truncatedURI = new URI(truncatedUri)

  // See https://github.com/snowplow/snowplow/issues/268 for background behind this test
  def e3 =
    PageEnrichments.extractPageUri(originalUri.some, truncatedUri.some) must beRight(
      truncatedURI.some
    )
}
