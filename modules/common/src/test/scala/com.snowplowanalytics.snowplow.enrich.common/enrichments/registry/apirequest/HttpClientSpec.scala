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
package com.snowplowanalytics.snowplow.enrich.common.utils

import org.http4s.{BasicCredentials, Header, Headers}
import org.http4s.headers.Authorization
import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers
import org.specs2.mock.Mockito

class HttpClientSpec extends Specification with ValidatedMatchers with Mockito {
  def is = s2"""
  getHeaders returns Authorization, accept header and content-type headers if authUser or authPassword is defined $e1
  getHeaders does not return an Authorization header if authUser and authPassword are not defined  $e2
  """

  def e1 = {
    val headers = HttpClient.getHeaders(None, Some("2778e1d8-500b-4f9f-a14e-f68b6b4e7b9f"))
    val expected =
      Headers(Authorization(BasicCredentials("", "2778e1d8-500b-4f9f-a14e-f68b6b4e7b9f")),
              Header("content-type", "application/json"),
              Header("accept", "*/*")
      )
    headers must beEqualTo(expected)
  }

  def e2 = {
    val headers = HttpClient.getHeaders(None, None)
    val expected = Headers(Header("content-type", "application/json"), Header("accept", "*/*"))
    headers must beEqualTo(expected)
  }
}
