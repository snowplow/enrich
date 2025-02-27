/*
 * Copyright (c) 2016-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry

import cats.data.NonEmptyList
import cats.syntax.option._
import cats.effect.testing.specs2.CatsEffect
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class StatusGatorAdapterSpec extends Specification with DataTables with ValidatedMatchers with CatsEffect {
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

  val adapterWithDefaultSchemas = StatusGatorAdapter(schemas = statusGatorSchemas)
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
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beValid(expected))
  }

  def e2 = {
    val payload =
      CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.AdapterFailure
              .InputData("body", None, "empty body: no events to process")
          )
        )
      )
  }

  def e3 = {
    val body =
      "service_name=CloudFlare&favicon_url=https%3A%2F%2Fdwxjd9cd6rwno.cloudfront.net%2Ffavicons%2Fcloudflare.ico&status_page_url=https%3A%2F%2Fwww.cloudflarestatus.com%2F&home_page_url=http%3A%2F%2Fwww.cloudflare.com&current_status=up&last_status=warn&occurred_at=2016-05-19T09%3A26%3A31%2B00%3A00"
    val payload =
      CollectorPayload(Shared.api, Nil, None, body.some, Shared.cljSource, Shared.context)
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.AdapterFailure.InputData(
              "contentType",
              None,
              "no content type: expected application/x-www-form-urlencoded"
            )
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
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.AdapterFailure.InputData(
              "contentType",
              "application/json".some,
              "expected application/x-www-form-urlencoded"
            )
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
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beInvalid(expected))
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
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beInvalid(expected))
  }
}
