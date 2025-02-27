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
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry

import cats.data.NonEmptyList
import cats.syntax.option._

import cats.effect.unsafe.implicits.global

import cats.effect.testing.specs2.CatsEffect

import io.circe._
import io.circe.literal._

import org.joda.time.DateTime

import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class MailchimpAdapterSpec extends Specification with DataTables with ValidatedMatchers with CatsEffect {
  def is = s2"""
  toKeys should return a valid List of Keys from a string containing braces (or not)                $e1
  toNestedJson should return a valid JField nested to contain all keys and then the supplied value  $e2
  toJsons should return a valid list of Jsons based on the Map supplied                             $e3
  mergeJsons should return a correctly merged JSON which matches the expectation                    $e4
  reformatParameters should return a parameter Map with correctly formatted values                  $e5
  toRawEvents must return a Nel Success with a correctly formatted ue_pr json                       $e6
  toRawEvents must return a Nel Success with a correctly merged and formatted ue_pr json            $e7
  toRawEvents must return a Nel Success for a supported event type                                  $e8
  toRawEvents must return a Nel Failure error for an unsupported event type                         $e9
  toRawEvents must return a Nel Success containing an unsubscribe event and query string parameters $e10
  toRawEvents must return a Nel Failure if the request body is missing                              $e11
  toRawEvents must return a Nel Failure if the content type is missing                              $e12
  toRawEvents must return a Nel Failure if the content type is incorrect                            $e13
  toRawEvents must return a Nel Failure if the request body does not contain a type parameter       $e14
  """

  object Shared {
    val api = CollectorPayload.Api("com.mailchimp", "v1")
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

  val adapterWithDefaultSchemas = MailchimpAdapter(schemas = mailchimpSchemas)
  val ContentType = "application/x-www-form-urlencoded"

  def e1 = {
    val keys = "data[merges][LNAME]"
    val expected = NonEmptyList.of("data", "merges", "LNAME")

    adapterWithDefaultSchemas.toKeys(keys) mustEqual expected
  }

  def e2 = {
    val keys = NonEmptyList.of("data", "merges", "LNAME")
    val value = "Beemster"
    val expected = ("data", json"""{ "merges": { "LNAME": "Beemster" }}""")

    adapterWithDefaultSchemas.toNestedJson(keys, value) mustEqual expected
  }

  def e3 = {
    val map = Map(
      "data[merges][LNAME]" -> "Beemster",
      "data[merges][FNAME]" -> "Joshua"
    ).toOpt
    val expected = List(
      ("data", json"""{ "merges": { "LNAME": "Beemster" }}"""),
      ("data", json"""{ "merges": { "FNAME": "Joshua" }}""")
    )
    adapterWithDefaultSchemas.toJsons(map) mustEqual expected
  }

  def e4 = {
    val a = ("l1", Json.obj(("l2", Json.obj(("l3", Json.obj(("str", Json.fromString("hi"))))))))
    val b = ("l1", Json.obj(("l2", Json.obj(("l3", Json.obj(("num", Json.fromInt(42))))))))
    val expected = json"""{
      "l1": {
        "l2": {
          "l3": {
            "str": "hi",
            "num": 42
          }
        }
      }
    }"""
    adapterWithDefaultSchemas.mergeJsons(List(a, b)) mustEqual expected
  }

  def e5 =
    "SPEC NAME" || "PARAMS" | "EXPECTED OUTPUT" |
      "Return Updated Params" !! Map("type" -> "subscribe", "fired_at" -> "2014-10-22 13:50:00").toOpt ! Map(
        "type" -> "subscribe",
        "fired_at" -> "2014-10-22T13:50:00.000Z"
      ).toOpt |
      "Return Same Params" !! Map("type" -> "subscribe", "id" -> "some_id").toOpt ! Map(
        "type" -> "subscribe",
        "id" -> "some_id"
      ).toOpt |> { (_, params, expected) =>
      adapterWithDefaultSchemas.reformatParameters(params) mustEqual expected
    }

  def e6 = {
    val body = "type=subscribe&data%5Bmerges%5D%5BLNAME%5D=Beemster"
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
              |"schema":"iglu:com.mailchimp/subscribe/jsonschema/1-0-0",
              |"data":{
                |"data":{
                  |"merges":{
                    |"LNAME":"Beemster"
                  |}
                |},
                |"type":"subscribe"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beValid(
          NonEmptyList.one(
            RawEvent(
              Shared.api,
              Map("tv" -> "com.mailchimp-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson).toOpt,
              ContentType.some,
              Shared.cljSource,
              Shared.context
            )
          )
        )
      )
  }

  def e7 = {
    val body = "type=subscribe&data%5Bmerges%5D%5BFNAME%5D=Agent&data%5Bmerges%5D%5BLNAME%5D=Smith"
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
              |"schema":"iglu:com.mailchimp/subscribe/jsonschema/1-0-0",
              |"data":{
                |"data":{
                  |"merges":{
                    |"LNAME":"Smith",
                    |"FNAME":"Agent"
                  |}
                |},
                |"type":"subscribe"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beValid(
          NonEmptyList.one(
            RawEvent(
              Shared.api,
              Map("tv" -> "com.mailchimp-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson).toOpt,
              ContentType.some,
              Shared.cljSource,
              Shared.context
            )
          )
        )
      )
  }

  def e8 =
    "SPEC NAME" || "SCHEMA TYPE" | "EXPECTED SCHEMA" |
      "Valid, type subscribe" !! "subscribe" ! "iglu:com.mailchimp/subscribe/jsonschema/1-0-0" |
      "Valid, type unsubscribe" !! "unsubscribe" ! "iglu:com.mailchimp/unsubscribe/jsonschema/1-0-0" |
      "Valid, type profile" !! "profile" ! "iglu:com.mailchimp/profile_update/jsonschema/1-0-0" |
      "Valid, type email" !! "upemail" ! "iglu:com.mailchimp/email_address_change/jsonschema/1-0-0" |
      "Valid, type cleaned" !! "cleaned" ! "iglu:com.mailchimp/cleaned_email/jsonschema/1-0-0" |
      "Valid, type campaign" !! "campaign" ! "iglu:com.mailchimp/campaign_sending_status/jsonschema/1-0-0" |> { (_, schema, expected) =>
      val body = "type=" + schema
      val payload = CollectorPayload(
        Shared.api,
        Nil,
        ContentType.some,
        body.some,
        Shared.cljSource,
        Shared.context
      )
      val expectedJson =
        "{\"schema\":\"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0\",\"data\":{\"schema\":\"" + expected + "\",\"data\":{\"type\":\"" + schema + "\"}}}"
      adapterWithDefaultSchemas
        .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .map(
          _ must beValid(
            NonEmptyList.one(
              RawEvent(
                Shared.api,
                Map("tv" -> "com.mailchimp-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson).toOpt,
                ContentType.some,
                Shared.cljSource,
                Shared.context
              )
            )
          )
        )
        .unsafeRunSync()
    }

  def e9 =
    "SPEC NAME" || "SCHEMA TYPE" | "EXPECTED OUTPUT" |
      "Invalid, bad type" !! "bad" ! FailureDetails.AdapterFailure.SchemaMapping(
        "bad".some,
        adapterWithDefaultSchemas.EventSchemaMap,
        "no schema associated with the provided type parameter"
      ) |
      "Invalid, no type" !! "" ! FailureDetails.AdapterFailure.SchemaMapping(
        "".some,
        adapterWithDefaultSchemas.EventSchemaMap,
        "cannot determine event type: type parameter empty"
      ) |> { (_, schema, expected) =>
      val body = "type=" + schema
      val payload = CollectorPayload(
        Shared.api,
        Nil,
        ContentType.some,
        body.some,
        Shared.cljSource,
        Shared.context
      )
      adapterWithDefaultSchemas
        .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .map(_ must beInvalid(NonEmptyList.one(expected)))
        .unsafeRunSync()
    }

  def e10 = {
    val body =
      "type=unsubscribe&fired_at=2014-10-22+13%3A10%3A40&data%5Baction%5D=unsub&data%5Breason%5D=manual&data%5Bid%5D=94826aa750&data%5Bemail%5D=josh%40snowplowanalytics.com&data%5Bemail_type%5D=html&data%5Bip_opt%5D=82.225.169.220&data%5Bweb_id%5D=203740265&data%5Bmerges%5D%5BEMAIL%5D=josh%40snowplowanalytics.com&data%5Bmerges%5D%5BFNAME%5D=Joshua&data%5Bmerges%5D%5BLNAME%5D=Beemster&data%5Blist_id%5D=f1243a3b12"
    val qs = SpecHelpers.toNameValuePairs("nuid" -> "123")
    val payload = CollectorPayload(
      Shared.api,
      qs,
      ContentType.some,
      body.some,
      Shared.cljSource,
      Shared.context
    )
    val expectedJson =
      """|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.mailchimp/unsubscribe/jsonschema/1-0-0",
              |"data":{
                |"data":{
                  |"merges":{
                    |"EMAIL":"josh@snowplowanalytics.com",
                    |"FNAME":"Joshua",
                    |"LNAME":"Beemster"
                  |},
                  |"web_id":"203740265",
                  |"action":"unsub",
                  |"id":"94826aa750",
                  |"reason":"manual",
                  |"email_type":"html",
                  |"list_id":"f1243a3b12",
                  |"email":"josh@snowplowanalytics.com",
                  |"ip_opt":"82.225.169.220"
                |},
                |"type":"unsubscribe",
                |"fired_at":"2014-10-22T13:10:40.000Z"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beValid(
          NonEmptyList.one(
            RawEvent(
              Shared.api,
              Map(
                "tv" -> "com.mailchimp-v1",
                "e" -> "ue",
                "p" -> "srv",
                "ue_pr" -> expectedJson,
                "nuid" -> "123"
              ).toOpt,
              ContentType.some,
              Shared.cljSource,
              Shared.context
            )
          )
        )
      )
  }

  def e11 = {
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

  def e12 = {
    val payload =
      CollectorPayload(Shared.api, Nil, None, "stub".some, Shared.cljSource, Shared.context)
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

  def e13 = {
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      "application/json".some,
      "stub".some,
      Shared.cljSource,
      Shared.context
    )
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

  def e14 = {
    val body = "fired_at=2014-10-22+13%3A10%3A40"
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      ContentType.some,
      body.some,
      Shared.cljSource,
      Shared.context
    )
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.AdapterFailure.InputData(
              "body",
              "fired_at=2014-10-22+13%3A10%3A40".some,
              "no `type` parameter provided: cannot determine event type"
            )
          )
        )
      )
  }
}
