/*
 * Copyright (c) 2016-2020 Snowplow Analytics Ltd. All rights reserved.
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
package adapters
package registry

import cats.data.NonEmptyList
import cats.syntax.option._
import com.snowplowanalytics.snowplow.badrows._
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import loaders._
import utils.Clock._

import SpecHelpers._

class StatusGatorAdapterSpec extends Specification with DataTables with ValidatedMatchers {
  def is = s2"""
  toRawEvents must return a Success Nel if every event in the payload is successful          $e1
  toRawEvents must return a Nel Failure if the request body is missing                       $e2
  toRawEvents must return a Nel Failure if the content type is missing                       $e3
  toRawEvents must return a Nel Failure if the content type is incorrect                     $e4
  toRawEvents must return a Failure Nel if the event in the payload is incorrect             $e5
  toRawEvents must return a Failure String if the event string could not be parsed into JSON $e6
  """

  object Shared {
    val api = CollectorPayload.Api("com.statusgator", "v1")
    val cljSource = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
    val context = CollectorPayload.Context(
      DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    )
  }

  val ContentType = "application/x-www-form-urlencoded"

  def e1 = {
    val body =
      "service_name=CloudFlare&favicon_url=https%3A%2F%2Fdwxjd9cd6rwno.cloudfront.net%2Ffavicons%2Fcloudflare.ico&status_page_url=https%3A%2F%2Fwww.cloudflarestatus.com%2F&home_page_url=http%3A%2F%2Fwww.cloudflare.com&current_status=up&last_status=warn&occurred_at=2016-05-19T09%3A26%3A31%2B00%3A00"
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      ContentType.some,
      body.some,
      Shared.cljSource,
      Shared.context
    )
    val expectedJson =
      """|{
          |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
          |"data":{
            |"schema":"iglu:com.statusgator/status_change/jsonschema/1-0-0",
            |"data":{
              |"lastStatus":"warn",
              |"statusPageUrl":"https://www.cloudflarestatus.com/",
              |"serviceName":"CloudFlare",
              |"faviconUrl":"https://dwxjd9cd6rwno.cloudfront.net/favicons/cloudflare.ico",
              |"occurredAt":"2016-05-19T09:26:31+00:00",
              |"homePageUrl":"http://www.cloudflare.com",
              |"currentStatus":"up"
            |}
          |}
        |}""".stripMargin.replaceAll("[\n\r]", "")

    val expected = NonEmptyList.one(
      RawEvent(
        Shared.api,
        Map("tv" -> "com.statusgator-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson).toOpt,
        ContentType.some,
        Shared.cljSource,
        Shared.context
      )
    )
    StatusGatorAdapter.toRawEvents(payload, SpecHelpers.client) must beValid(expected)
  }

  def e2 = {
    val payload =
      CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    StatusGatorAdapter.toRawEvents(payload, SpecHelpers.client) must beInvalid(
      NonEmptyList.one(
        FailureDetails.AdapterFailure
          .InputData("body", None, "empty body: no events to process")
      )
    )
  }

  def e3 = {
    val body =
      "service_name=CloudFlare&favicon_url=https%3A%2F%2Fdwxjd9cd6rwno.cloudfront.net%2Ffavicons%2Fcloudflare.ico&status_page_url=https%3A%2F%2Fwww.cloudflarestatus.com%2F&home_page_url=http%3A%2F%2Fwww.cloudflare.com&current_status=up&last_status=warn&occurred_at=2016-05-19T09%3A26%3A31%2B00%3A00"
    val payload =
      CollectorPayload(Shared.api, Nil, None, body.some, Shared.cljSource, Shared.context)
    StatusGatorAdapter.toRawEvents(payload, SpecHelpers.client) must beInvalid(
      NonEmptyList.one(
        FailureDetails.AdapterFailure.InputData(
          "contentType",
          None,
          "no content type: expected application/x-www-form-urlencoded"
        )
      )
    )
  }

  def e4 = {
    val body =
      "service_name=CloudFlare&favicon_url=https%3A%2F%2Fdwxjd9cd6rwno.cloudfront.net%2Ffavicons%2Fcloudflare.ico&status_page_url=https%3A%2F%2Fwww.cloudflarestatus.com%2F&home_page_url=http%3A%2F%2Fwww.cloudflare.com&current_status=up&last_status=warn&occurred_at=2016-05-19T09%3A26%3A31%2B00%3A00"
    val ct = "application/json"
    val payload =
      CollectorPayload(Shared.api, Nil, ct.some, body.some, Shared.cljSource, Shared.context)
    StatusGatorAdapter.toRawEvents(payload, SpecHelpers.client) must beInvalid(
      NonEmptyList.one(
        FailureDetails.AdapterFailure.InputData(
          "contentType",
          "application/json".some,
          "expected application/x-www-form-urlencoded"
        )
      )
    )
  }

  def e5 = {
    val body = ""
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      ContentType.some,
      body.some,
      Shared.cljSource,
      Shared.context
    )
    val expected =
      NonEmptyList.one(
        FailureDetails.AdapterFailure
          .InputData("body", None, "empty body: no events to process")
      )
    StatusGatorAdapter.toRawEvents(payload, SpecHelpers.client) must beInvalid(expected)
  }

  def e6 = {
    val body =
      "{service_name=CloudFlare&favicon_url=https%3A%2F%2Fdwxjd9cd6rwno.cloudfront.net%2Ffavicons%2Fcloudflare.ico&status_page_url=https%3A%2F%2Fwww.cloudflarestatus.com%2F&home_page_url=http%3A%2F%2Fwww.cloudflare.com&current_status=up&last_status=warn&occurred_at=2016-05-19T09%3A26%3A31%2B00%3A00"
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      ContentType.some,
      body.some,
      Shared.cljSource,
      Shared.context
    )
    val expected = NonEmptyList.one(
      FailureDetails.AdapterFailure.InputData(
        "body",
        body.some,
        "could not parse body: Illegal character in query at index 18: http://localhost/?{service_name=CloudFlare&favicon_url=https%3A%2F%2Fdwxjd9cd6rwno.cloudfront.net%2Ffavicons%2Fcloudflare.ico&status_page_url=https%3A%2F%2Fwww.cloudflarestatus.com%2F&home_page_url=http%3A%2F%2Fwww.cloudflare.com&current_status=up&last_status=warn&occurred_at=2016-05-19T09%3A26%3A31%2B00%3A00"
      )
    )
    StatusGatorAdapter.toRawEvents(payload, SpecHelpers.client) must beInvalid(expected)
  }
}
