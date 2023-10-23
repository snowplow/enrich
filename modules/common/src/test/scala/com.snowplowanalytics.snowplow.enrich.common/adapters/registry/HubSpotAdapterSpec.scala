/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
 *
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
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry

import cats.data.NonEmptyList
import cats.syntax.option._
import cats.effect.testing.specs2.CatsIO
import io.circe.literal._
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class HubSpotAdapterSpec extends Specification with DataTables with ValidatedMatchers with CatsIO {
  def is = s2"""
  payloadBodyToEvents must return a Success list of event JSON's from a valid payload body $e1
  payloadBodyToEvents must return a Failure Nel for an invalid payload body being passed   $e2
  toRawEvents must return a Success Nel if all events are successful                       $e3
  toRawEvents must return a Failure Nel if any of the events where not successes           $e4
  toRawEvents must return a Nel Failure if the request body is missing                     $e5
  toRawEvents must return a Nel Failure if the content type is missing                     $e6
  toRawEvents must return a Nel Failure if the content type is incorrect                   $e7
  """

  object Shared {
    val api = CollectorPayload.Api("com.hubspot", "v1")
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

  val ContentType = "application/json"
  val adapterWithDefaultSchemas = HubSpotAdapter(schemas = hubspotSchemas)

  def e1 = {
    val bodyStr = """[{"subscriptionType":"company.change","eventId":16}]"""
    val expected = json"""{
      "subscriptionType": "company.change",
      "eventId": 16
    }"""
    adapterWithDefaultSchemas.payloadBodyToEvents(bodyStr) must beRight(List(expected))
  }

  def e2 =
    "SPEC NAME" || "INPUT" | "EXPECTED OUTPUT" |
      "Failure, parse exception" !! """{"something:"some"}""" ! FailureDetails.AdapterFailure
        .NotJson(
          "body",
          """{"something:"some"}""".some,
          """invalid json: expected : got 'some"}' (line 1, column 14)"""
        ) |> { (_, input, expected) =>
      adapterWithDefaultSchemas.payloadBodyToEvents(input) must beLeft(expected)
    }

  def e3 = {
    val bodyStr =
      """[{"eventId":1,"subscriptionId":25458,"portalId":4737818,"occurredAt":1539145399845,"subscriptionType":"contact.creation","attemptNumber":0,"objectId":123,"changeSource":"CRM","changeFlag":"NEW","appId":177698}]"""
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      ContentType.some,
      bodyStr.some,
      Shared.cljSource,
      Shared.context
    )
    val expected = NonEmptyList.one(
      RawEvent(
        Shared.api,
        Map(
          "tv" -> "com.hubspot-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.hubspot/contact_creation/jsonschema/1-0-0","data":{"eventId":1,"subscriptionId":25458,"portalId":4737818,"occurredAt":"2018-10-10T04:23:19.845Z","attemptNumber":0,"objectId":123,"changeSource":"CRM","changeFlag":"NEW","appId":177698}}}"""
        ).toOpt,
        ContentType.some,
        Shared.cljSource,
        Shared.context
      )
    )
    adapterWithDefaultSchemas.toRawEvents(payload, SpecHelpers.client).map(_ must beValid(expected))
  }

  def e4 = {
    val bodyStr =
      """[{"eventId":1,"subscriptionId":25458,"portalId":4737818,"occurredAt":1539145399845,"subscriptionType":"contact","attemptNumber":0,"objectId":123,"changeSource":"CRM","changeFlag":"NEW","appId":177698}]"""
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      ContentType.some,
      bodyStr.some,
      Shared.cljSource,
      Shared.context
    )
    val expected = FailureDetails.AdapterFailure.SchemaMapping(
      "contact".some,
      adapterWithDefaultSchemas.EventSchemaMap,
      "no schema associated with the provided type parameter at index 0"
    )
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client)
      .map(
        _ must beInvalid(
          NonEmptyList.one(expected)
        )
      )
  }

  def e5 = {
    val payload =
      CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.AdapterFailure
              .InputData("body", None, "empty body: not events to process")
          )
        )
      )
  }

  def e6 = {
    val payload =
      CollectorPayload(Shared.api, Nil, None, "stub".some, Shared.cljSource, Shared.context)
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.AdapterFailure.InputData(
              "contentType",
              None,
              "no content type: expected application/json"
            )
          )
        )
      )
  }

  def e7 = {
    val ct = "application/x-www-form-urlencoded".some
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      ct,
      "stub".some,
      Shared.cljSource,
      Shared.context
    )
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.AdapterFailure
              .InputData("contentType", ct, "expected application/json")
          )
        )
      )
  }
}
