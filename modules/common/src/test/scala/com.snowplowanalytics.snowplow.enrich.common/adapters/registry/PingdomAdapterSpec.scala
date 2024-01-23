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

import io.circe.literal._

import org.joda.time.DateTime

import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class PingdomAdapterSpec extends Specification with DataTables with ValidatedMatchers with CatsEffect {

  val adapterWithDefaultSchemas = PingdomAdapter(schemas = pingdomSchemas)
  def is = s2"""
  reformatParameters should return either an updated JSON without the 'action' field or the same JSON $e1
  reformatMapParams must return a Failure Nel for any Python Unicode wrapped values                   $e2
  toRawEvents must return a Success Nel for a valid querystring                                       $e3
  toRawEvents must return a Failure Nel for an empty querystring                                      $e4
  toRawEvents must return a Failure Nel for a querystring which does not contain 'message' as a key   $e5
  """

  object Shared {
    val api = CollectorPayload.Api("com.pingdom", "v1")
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

  def e1 =
    "SPEC NAME" || "JSON" | "EXPECTED OUTPUT" |
      "Remove action field" !! json"""{"action":"assign","agent":"smith"}""" ! json"""{"agent":"smith"}""" |
      "Nothing removed" !! json"""{"actions":"assign","agent":"smith"}""" ! json"""{"actions":"assign","agent":"smith"}""" |> {
      (_, json, expected) =>
        adapterWithDefaultSchemas.reformatParameters(json) mustEqual expected
    }

  def e2 = {
    val nvPairs = SpecHelpers.toNameValuePairs("p" -> "(u'apps',)")
    val expected =
      FailureDetails.AdapterFailure.InputData(
        "p",
        "apps".some,
        """should not pass regex \(u'(.+)',\)"""
      )
    adapterWithDefaultSchemas.reformatMapParams(nvPairs) must beLeft(NonEmptyList.one(expected))
  }

  def e3 = {
    val querystring = SpecHelpers.toNameValuePairs(
      "p" -> "apps",
      "message" -> """{"check": "1421338", "checkname": "Webhooks_Test", "host": "7eef51c2.ngrok.com", "action": "assign", "incidentid": 3, "description": "down"}"""
    )
    val payload =
      CollectorPayload(Shared.api, querystring, None, None, Shared.cljSource, Shared.context)
    val expected = RawEvent(
      Shared.api,
      Map(
        "tv" -> "com.pingdom-v1",
        "e" -> "ue",
        "p" -> "apps",
        "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.pingdom/incident_assign/jsonschema/1-0-0","data":{"check":"1421338","checkname":"Webhooks_Test","host":"7eef51c2.ngrok.com","incidentid":3,"description":"down"}}}"""
      ).toOpt,
      None,
      Shared.cljSource,
      Shared.context
    )
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup)
      .map(
        _ must beValid(
          NonEmptyList.one(expected)
        )
      )
  }

  def e4 = {
    val payload = CollectorPayload(Shared.api, Nil, None, None, Shared.cljSource, Shared.context)
    val expected =
      FailureDetails.AdapterFailure.InputData(
        "querystring",
        None,
        "empty querystring: no events to process"
      )
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup)
      .map(
        _ must beInvalid(
          NonEmptyList.one(expected)
        )
      )
  }

  def e5 = {
    val querystring = SpecHelpers.toNameValuePairs("p" -> "apps")
    val payload =
      CollectorPayload(Shared.api, querystring, None, None, Shared.cljSource, Shared.context)
    val expected =
      FailureDetails.AdapterFailure.InputData(
        "querystring",
        "p=apps".some,
        "no `message` parameter provided"
      )
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup)
      .map(
        _ must beInvalid(
          NonEmptyList.one(expected)
        )
      )
  }
}
