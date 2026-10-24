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
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry.snowplow

import cats.data.NonEmptyList
import cats.syntax.option._

import cats.effect.unsafe.implicits.global
import cats.effect.testing.specs2.CatsEffect

import org.joda.time.DateTime

import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers

/**
 * Covers the `cx`/`co` handling in `RedirectAdapter.toRawEvents` when the request already carries
 * an event. A querystring key present with no `=` at all (a bare `&cx` or `&co`) parses to a `None`
 * value in `Adapter.toMap`; the adapter treats such a key as absent, so the redirect is still added
 * as a context and the request succeeds. An empty-valued `cx=` is not treated as absent: it reaches
 * `addToExistingCx` and is rejected as a bad row. All five shapes are reachable from a plain GET to
 * a redirect endpoint and are exercised nowhere else in the suite.
 */
class RedirectAdapterSpec extends Specification with ValidatedMatchers with CatsEffect {
  def is = s2"""
  RedirectAdapter.toRawEvents should succeed for a bare cx and no co                                $e1
  RedirectAdapter.toRawEvents should succeed for a bare co and no cx                                $e2
  RedirectAdapter.toRawEvents should succeed for both cx and co present bare                        $e3
  RedirectAdapter.toRawEvents should succeed for a bare cx and a co with a value                    $e4
  RedirectAdapter.toRawEvents should fail with a TrackerProtocolViolation for an empty-valued cx    $e5
  """

  val Tp2 = CollectorPayload.Api("com.snowplowanalytics.snowplow", "tp2")

  val source = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
  val context = CollectorPayload.Context(
    DateTime.parse("2013-08-29T00:18:48.000+00:00"),
    "37.157.33.123".some,
    None,
    None,
    Nil,
    None
  )

  val Uri = "https://example.com"

  private val UriRedirectContext =
    """{"schema":"iglu:com.snowplowanalytics.snowplow/uri_redirect/jsonschema/1-0-0","data":{"uri":"%s"}}"""
      .format(Uri)

  private val FreshCo =
    s"""{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[$UriRedirectContext]}"""

  // pair._2 == null represents a querystring key present with no `=` at all, e.g. a bare `&cx`
  private def parametersOf(pairs: (String, String)*) =
    RedirectAdapter
      .toRawEvents(
        SpecHelpers.collectorPayload(Tp2, SpecHelpers.toNameValuePairs(pairs: _*), None, None, source, context),
        SpecHelpers.client,
        SpecHelpers.registryLookup,
        SpecHelpers.DefaultMaxJsonDepth
      )
      .unsafeRunSync()
      .map(_.map(_.parameters))

  def e1 =
    parametersOf("u" -> Uri, "e" -> "pv", "cx" -> null) must beValid(
      NonEmptyList.one(
        Map(
          "e" -> Some("pv"),
          "cx" -> None,
          "co" -> Some(FreshCo),
          "tv" -> Some("r-tp2"),
          "p" -> Some("web")
        )
      )
    )

  def e2 =
    parametersOf("u" -> Uri, "e" -> "pv", "co" -> null) must beValid(
      NonEmptyList.one(
        Map(
          "e" -> Some("pv"),
          "co" -> Some(FreshCo),
          "tv" -> Some("r-tp2"),
          "p" -> Some("web")
        )
      )
    )

  def e3 =
    parametersOf("u" -> Uri, "e" -> "pv", "cx" -> null, "co" -> null) must beValid(
      NonEmptyList.one(
        Map(
          "e" -> Some("pv"),
          "cx" -> None,
          "co" -> Some(FreshCo),
          "tv" -> Some("r-tp2"),
          "p" -> Some("web")
        )
      )
    )

  def e4 = {
    val existingCo = """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[]}"""
    val mergedCo =
      s"""{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[$UriRedirectContext]}"""
    parametersOf("u" -> Uri, "e" -> "pv", "cx" -> null, "co" -> existingCo) must beValid(
      NonEmptyList.one(
        Map(
          "e" -> Some("pv"),
          "cx" -> None,
          "co" -> Some(mergedCo),
          "tv" -> Some("r-tp2"),
          "p" -> Some("web")
        )
      )
    )
  }

  // An empty-valued `cx` is not treated as absent (unlike an empty-valued `co`): it reaches
  // `addToExistingCx`, which fails to base64-decode-then-parse an empty string as JSON.
  def e5 =
    parametersOf("u" -> Uri, "e" -> "pv", "cx" -> "") must beInvalid(
      NonEmptyList.one(
        FailureDetails.TrackerProtocolViolation.NotJson("co|cx", "".some, "invalid json: exhausted input")
      )
    )
}
