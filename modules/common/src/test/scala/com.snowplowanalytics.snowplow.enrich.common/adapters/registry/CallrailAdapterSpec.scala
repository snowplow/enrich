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
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry

import cats.data.NonEmptyList
import cats.syntax.option._

import cats.effect.testing.specs2.CatsEffect

import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class CallrailAdapterSpec extends Specification with DataTables with ValidatedMatchers with CatsEffect {
  def is = s2"""
  toRawEvents should return a NEL containing one RawEvent if the querystring is correctly populated $e1
  toRawEvents should return a Validation Failure if there are no parameters on the querystring      $e2
  """

  val adapterWithDefaultSchemas = CallrailAdapter(schemas = callrailSchemas)

  object Shared {
    val api = CollectorPayload.Api("com.callrail", "v1")
    val source = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
    val context = CollectorPayload.Context(
      DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    )
  }

  object Expected {
    val staticNoPlatform = Map(
      "tv" -> "com.callrail-v1",
      "e" -> "ue",
      "cv" -> "clj-0.6.0-tom-0.0.4"
    ).toOpt
    val static = staticNoPlatform ++ Map("p" -> "srv").toOpt
  }

  def e1 = {
    val params = toNameValuePairs(
      "answered" -> "true",
      "callercity" -> "BAKERSFIELD",
      "callercountry" -> "US",
      "callername" -> "SKYPE CALLER",
      "callernum" -> "+12612230240",
      "callerstate" -> "CA",
      "callerzip" -> "92307",
      "callsource" -> "keyword",
      "datetime" -> "2014-10-09 16:23:45",
      "destinationnum" -> "2012032051",
      "duration" -> "247",
      "first_call" -> "true",
      "ga" -> "",
      "gclid" -> "",
      "id" -> "201235151",
      "ip" -> "86.178.163.7",
      "keywords" -> "",
      "kissmetrics_id" -> "",
      "landingpage" -> "http://acme.com/",
      "recording" -> "http://app.callrail.com/calls/201235151/recording/9f59ad59ba1cfa264312",
      "referrer" -> "direct",
      "referrermedium" -> "Direct",
      "trackingnum" -> "+12012311668",
      "transcription" -> "",
      "utm_campaign" -> "",
      "utm_content" -> "",
      "utm_medium" -> "",
      "utm_source" -> "",
      "utm_term" -> "",
      "utma" -> "",
      "utmb" -> "",
      "utmc" -> "",
      "utmv" -> "",
      "utmx" -> "",
      "utmz" -> "",
      "cv" -> "clj-0.6.0-tom-0.0.4",
      "nuid" -> "-"
    )
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.source, Shared.context)

    val expectedJson =
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.callrail/call_complete/jsonschema/1-0-2",
              |"data":{
                |"duration":247,
                |"utm_source":null,
                |"utmv":null,
                |"ip":"86.178.163.7",
                |"utmx":null,
                |"ga":null,
                |"destinationnum":"2012032051",
                |"datetime":"2014-10-09T16:23:45.000Z",
                |"kissmetrics_id":null,
                |"landingpage":"http://acme.com/",
                |"callerzip":"92307",
                |"gclid":null,
                |"callername":"SKYPE CALLER",
                |"utmb":null,
                |"id":"201235151",
                |"callernum":"+12612230240",
                |"utm_content":null,
                |"trackingnum":"+12012311668",
                |"referrermedium":"Direct",
                |"utm_campaign":null,
                |"keywords":null,
                |"transcription":null,
                |"utmz":null,
                |"utma":null,
                |"referrer":"direct",
                |"callerstate":"CA",
                |"recording":"http://app.callrail.com/calls/201235151/recording/9f59ad59ba1cfa264312",
                |"first_call":true,
                |"utmc":null,
                |"callercountry":"US",
                |"utm_medium":null,
                |"callercity":"BAKERSFIELD",
                |"utm_term":null,
                |"answered":true,
                |"callsource":"keyword"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup)
      .map(
        _ must beValid(
          NonEmptyList.one(
            RawEvent(
              Shared.api,
              Expected.static ++ Map("ue_pr" -> expectedJson, "nuid" -> "-").toOpt,
              None,
              Shared.source,
              Shared.context
            )
          )
        )
      )
  }

  def e2 = {
    val params = toNameValuePairs()
    val payload = CollectorPayload(Shared.api, params, None, None, Shared.source, Shared.context)

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.AdapterFailure
              .InputData("querystring", None, "empty querystring")
          )
        )
      )
  }
}
