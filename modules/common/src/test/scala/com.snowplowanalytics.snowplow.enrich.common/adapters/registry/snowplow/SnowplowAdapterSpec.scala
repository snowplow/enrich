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

import io.circe.literal._

import org.joda.time.DateTime

import org.specs2.{ScalaCheck, Specification}
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import com.snowplowanalytics.iglu.client._
import com.snowplowanalytics.iglu.client.validator._

import com.snowplowanalytics.iglu.core._

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.loaders._
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils => CU}
import com.snowplowanalytics.snowplow.enrich.common.RawEventParameters
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class SnowplowAdapterSpec extends Specification with DataTables with ValidatedMatchers with ScalaCheck with CatsEffect {
  def is = s2"""
  Tp1.toRawEvents should return a NEL containing one RawEvent if the querystring is populated                             $e1
  Tp1.toRawEvents should return a Validation Failure if the querystring is empty                                          $e2
  Tp2.toRawEvents should return a NEL containing one RawEvent if only the querystring is populated                        $e3
  Tp2.toRawEvents should return a NEL containing one RawEvent if the querystring is empty but the body contains one event $e4
  Tp2.toRawEvents should return a NEL containing three RawEvents consolidating body's events and querystring's parameters $e5
  Tp1.toRawEvents should return a NEL containing one RawEvent if the Content-Type is application/json; charset=UTF-8      $e6
  Tp2.toRawEvents should return a Validation Failure if querystring, body and content type are mismatching                $e7
  Tp2.toRawEvents should return a Validation Failure if the body is not a self-describing JSON                            $e8
  Tp2.toRawEvents should return a Validation Failure if the body is in a JSON Schema other than payload_data              $e9
  Tp2.toRawEvents should return a Validation Failure if the body fails payload_data JSON Schema validation                $e10
  Redirect.toRawEvents should return a NEL of 1 RawEvent for a redirect with no event type specified                      $e11
  Redirect.toRawEvents should return a NEL of 1 RawEvent for a redirect with an event type but no contexts                $e12
  Redirect.toRawEvents should return a NEL of 1 RawEvent for a redirect with an event type and empty contexts             $e13
  Redirect.toRawEvents should return a NEL of 1 RawEvent for a redirect with an event type and unencoded contexts         $e14
  Redirect.toRawEvents should return a NEL of 1 RawEvent for a redirect with an event type and Base64-encoded contexts    $e15
  Redirect.toRawEvents should return a Validation Failure if the querystring is empty                                     $e16
  Redirect.toRawEvents should return a Validation Failure if the querystring does not contain a u parameter               $e17
  Redirect.toRawEvents should return a Validation Failure if the event type is specified and the co JSON is corrupted     $e18
  Redirect.toRawEvents should return a Validation Failure if the event type is specified and the cx Base64 is corrupted   $e19
  Redirect.toRawEvents should return a Validation Failure if the URI is null (&u param without a value)                   $e20
  """

  object Snowplow {
    private val api: (String) => CollectorPayload.Api = version => CollectorPayload.Api("com.snowplowanalytics.snowplow", version)
    val Tp1 = api("tp1")
    val Tp2 = api("tp2")
  }

  val ApplicationJson = "application/json"
  val ApplicationJsonWithCharset = "application/json; charset=utf-8"
  val ApplicationJsonWithCapitalCharset = "application/json; charset=UTF-8"

  object Shared {
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

  def e1 = {
    val payload =
      CollectorPayload(
        Snowplow.Tp1,
        SpecHelpers.toNameValuePairs("aid" -> "test"),
        None,
        None,
        Shared.source,
        Shared.context
      )
    Tp1Adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beValid(
          NonEmptyList.one(
            RawEvent(
              Snowplow.Tp1,
              Map("aid" -> "test").toOpt,
              None,
              Shared.source,
              Shared.context
            )
          )
        )
      )
  }

  def e2 = {
    val payload = CollectorPayload(Snowplow.Tp1, Nil, None, None, Shared.source, Shared.context)
    Tp1Adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.AdapterFailure.InputData(
              "querystring",
              None,
              "empty querystring: not a valid URI redirect"
            )
          )
        )
      )
  }

  def e3 = {
    val payload = CollectorPayload(
      Snowplow.Tp2,
      SpecHelpers.toNameValuePairs("aid" -> "tp2", "e" -> "se"),
      None,
      None,
      Shared.source,
      Shared.context
    )
    Tp2Adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beValid(
          NonEmptyList.one(
            RawEvent(
              Snowplow.Tp2,
              Map("aid" -> "tp2", "e" -> "se").toOpt,
              None,
              Shared.source,
              Shared.context
            )
          )
        )
      )
  }

  def e4 = {
    val body =
      SpecHelpers.toSelfDescJson("""[{"tv":"ios-0.1.0","p":"mob","e":"se"}]""", "payload_data")
    val payload =
      CollectorPayload(
        Snowplow.Tp2,
        Nil,
        ApplicationJsonWithCharset.some,
        body.some,
        Shared.source,
        Shared.context
      )
    Tp2Adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beValid(
          NonEmptyList.one(
            RawEvent(
              Snowplow.Tp2,
              Map("tv" -> "ios-0.1.0", "p" -> "mob", "e" -> "se").toOpt,
              ApplicationJsonWithCharset.some,
              Shared.source,
              Shared.context
            )
          )
        )
      )
  }

  def e5 = {
    val body = SpecHelpers.toSelfDescJson(
      """[{"tv":"1","p":"1","e":"1"},{"tv":"2","p":"2","e":"2"},{"tv":"3","p":"3","e":"3"}]""",
      "payload_data"
    )
    val payload = CollectorPayload(
      Snowplow.Tp2,
      SpecHelpers.toNameValuePairs("tv" -> "0", "nuid" -> "123"),
      ApplicationJsonWithCapitalCharset.some,
      body.some,
      Shared.source,
      Shared.context
    )
    val rawEvent: RawEventParameters => RawEvent = params =>
      RawEvent(
        Snowplow.Tp2,
        params,
        ApplicationJsonWithCapitalCharset.some,
        Shared.source,
        Shared.context
      )

    Tp2Adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beValid(
          NonEmptyList.of(
            rawEvent(Map("tv" -> "0", "p" -> "1", "e" -> "1", "nuid" -> "123").toOpt),
            rawEvent(Map("tv" -> "0", "p" -> "2", "e" -> "2", "nuid" -> "123").toOpt),
            rawEvent(Map("tv" -> "0", "p" -> "3", "e" -> "3", "nuid" -> "123").toOpt)
          )
        )
      )
  }

  def e6 = {
    val body =
      SpecHelpers.toSelfDescJson("""[{"tv":"ios-0.1.0","p":"mob","e":"se"}]""", "payload_data")
    val payload = CollectorPayload(
      Snowplow.Tp2,
      Nil,
      ApplicationJsonWithCapitalCharset.some,
      body.some,
      Shared.source,
      Shared.context
    )
    Tp2Adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beValid(
          NonEmptyList.one(
            RawEvent(
              Snowplow.Tp2,
              Map("tv" -> "ios-0.1.0", "p" -> "mob", "e" -> "se").toOpt,
              ApplicationJsonWithCapitalCharset.some,
              Shared.source,
              Shared.context
            )
          )
        )
      )
  }

  def e7 =
    "SPEC NAME" || "IN QUERYSTRING" | "IN CONTENT TYPE" | "IN BODY" | "EXP. FAILURE" |
      "Invalid content type" !! Nil ! "text/plain".some ! "body".some ! NonEmptyList.one(
        FailureDetails.TrackerProtocolViolation.InputData(
          "contentType",
          "text/plain".some,
          "expected one of application/json, application/json; charset=utf-8, application/json; charset=UTF-8"
        )
      ) |
      "Neither querystring nor body populated" !! Nil ! None ! None ! NonEmptyList.of(
        FailureDetails.TrackerProtocolViolation.InputData(
          "body",
          None,
          "empty body: not a valid tracker protocol event"
        ),
        FailureDetails.TrackerProtocolViolation.InputData(
          "querystring",
          None,
          "empty querystring: not a valid tracker protocol event"
        )
      ) |
      "Body populated but content type missing" !! Nil ! None ! "body".some ! NonEmptyList.one(
        FailureDetails.TrackerProtocolViolation.InputData(
          "contentType",
          None,
          "expected one of application/json, application/json; charset=utf-8, application/json; charset=UTF-8"
        )
      ) |
      "Content type populated but body missing" !! SpecHelpers.toNameValuePairs(
        "a" -> "b"
      ) ! ApplicationJsonWithCharset.some ! None ! NonEmptyList
        .one(
          FailureDetails.TrackerProtocolViolation.InputData(
            "body",
            None,
            "empty body: not a valid track protocol event"
          )
        ) |
      "Body is not a JSON" !! SpecHelpers.toNameValuePairs("a" -> "b") ! ApplicationJson.some ! "body".some ! NonEmptyList
        .one(
          FailureDetails.TrackerProtocolViolation.NotJson(
            "body",
            "body".some,
            "invalid json: expected json value got 'body' (line 1, column 1)"
          )
        ) |> { (_, querystring, contentType, body, expected) =>
      val payload = CollectorPayload(
        Snowplow.Tp2,
        querystring,
        contentType,
        body,
        Shared.source,
        Shared.context
      )
      Tp2Adapter
        .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .map(_ must beInvalid(expected))
        .unsafeRunSync()
    }

  def e8 = {
    val payload = CollectorPayload(
      Snowplow.Tp2,
      SpecHelpers.toNameValuePairs("aid" -> "test"),
      ApplicationJson.some,
      """{"not":"self-desc"}""".some,
      Shared.source,
      Shared.context
    )
    Tp2Adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.TrackerProtocolViolation
              .NotIglu(json"""{"not":"self-desc"}""", ParseError.InvalidData)
          )
        )
      )
  }

  def e9 = {
    val body = SpecHelpers.toSelfDescJson("""{"longitude":20.1234}""", "geolocation_context")
    val payload = CollectorPayload(
      Snowplow.Tp2,
      Nil,
      ApplicationJson.some,
      body.some,
      Shared.source,
      Shared.context
    )
    Tp2Adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.TrackerProtocolViolation.IgluError(
              SchemaKey(
                "com.snowplowanalytics.snowplow",
                "geolocation_context",
                "jsonschema",
                SchemaVer.Full(1, 0, 0)
              ),
              ClientError.ValidationError(
                ValidatorError.InvalidData(
                  NonEmptyList.one(
                    ValidatorReport(
                      "$.latitude: is missing but it is required",
                      "$".some,
                      List("latitude"),
                      "required".some
                    )
                  )
                ),
                None
              )
            )
          )
        )
      )
  }

  def e10 =
    "SPEC NAME" || "IN JSON DATA" | "EXP. FAILURES" |
      "JSON object instead of array" !! "{}" ! NonEmptyList.one(
        FailureDetails.TrackerProtocolViolation.IgluError(
          SchemaKey(
            "com.snowplowanalytics.snowplow",
            "payload_data",
            "jsonschema",
            SchemaVer.Full(1, 0, 0)
          ),
          ClientError.ValidationError(
            ValidatorError.InvalidData(
              NonEmptyList.of(
                ValidatorReport(
                  "$: object found, array expected",
                  "$".some,
                  List("object", "array"),
                  "type".some
                )
              )
            ),
            None
          )
        )
      ) |
      "Missing required properties" !! """[{"tv":"ios-0.1.0"}]""" ! NonEmptyList.one(
        FailureDetails.TrackerProtocolViolation.IgluError(
          SchemaKey(
            "com.snowplowanalytics.snowplow",
            "payload_data",
            "jsonschema",
            SchemaVer.Full(1, 0, 0)
          ),
          ClientError.ValidationError(
            ValidatorError.InvalidData(
              NonEmptyList.of(
                ValidatorReport(
                  "$[0].p: is missing but it is required",
                  "$[0]".some,
                  List("p"),
                  "required".some
                ),
                ValidatorReport(
                  "$[0].e: is missing but it is required",
                  "$[0]".some,
                  List("e"),
                  "required".some
                )
              )
            ),
            None
          )
        )
      ) |
      "1 valid, 1 invalid" !! """[{"tv":"ios-0.1.0","p":"mob","e":"se"},{"new":"foo"}]""" ! NonEmptyList
        .one(
          FailureDetails.TrackerProtocolViolation.IgluError(
            SchemaKey(
              "com.snowplowanalytics.snowplow",
              "payload_data",
              "jsonschema",
              SchemaVer.Full(1, 0, 0)
            ),
            ClientError.ValidationError(
              ValidatorError.InvalidData(
                NonEmptyList.of(
                  ValidatorReport(
                    "$[1].tv: is missing but it is required",
                    "$[1]".some,
                    List("tv"),
                    "required".some
                  ),
                  ValidatorReport(
                    "$[1].p: is missing but it is required",
                    "$[1]".some,
                    List("p"),
                    "required".some
                  ),
                  ValidatorReport(
                    "$[1].e: is missing but it is required",
                    "$[1]".some,
                    List("e"),
                    "required".some
                  ),
                  ValidatorReport(
                    "$[1].new: is not defined in the schema and the schema does not allow additional properties",
                    "$[1]".some,
                    List("new"),
                    "additionalProperties".some
                  )
                )
              ),
              None
            )
          )
        ) |> { (_, json, expected) =>
      val body = SpecHelpers.toSelfDescJson(json, "payload_data")
      val payload =
        CollectorPayload(
          Snowplow.Tp2,
          Nil,
          ApplicationJson.some,
          body.some,
          Shared.source,
          Shared.context
        )

      Tp2Adapter
        .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .map(_ must beInvalid(expected))
        .unsafeRunSync()
    }

  def e11 = {
    val payload = CollectorPayload(
      Snowplow.Tp2,
      SpecHelpers.toNameValuePairs(
        "u" -> "https://github.com/snowplow/snowplow",
        "cx" -> "dGVzdHRlc3R0ZXN0"
      ),
      None,
      None,
      Shared.source,
      Shared.context
    )
    RedirectAdapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beValid(
          NonEmptyList.one(
            RawEvent(
              Snowplow.Tp2,
              Map(
                "e" -> "ue",
                "tv" -> "r-tp2",
                "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/uri_redirect/jsonschema/1-0-0","data":{"uri":"https://github.com/snowplow/snowplow"}}}""",
                "p" -> "web",
                "cx" -> "dGVzdHRlc3R0ZXN0"
              ).toOpt,
              None,
              Shared.source,
              Shared.context
            )
          )
        )
      )
  }

  def e12 = {
    val payload = CollectorPayload(
      Snowplow.Tp2,
      SpecHelpers.toNameValuePairs(
        "u" -> "https://github.com/snowplow/snowplow",
        "e" -> "se",
        "aid" -> "ads"
      ),
      None,
      None,
      Shared.source,
      Shared.context
    )
    RedirectAdapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beValid(
          NonEmptyList.one(
            RawEvent(
              Snowplow.Tp2,
              Map(
                "e" -> "se",
                "aid" -> "ads",
                "tv" -> "r-tp2",
                "co" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/uri_redirect/jsonschema/1-0-0","data":{"uri":"https://github.com/snowplow/snowplow"}}]}""",
                "p" -> "web"
              ).toOpt,
              None,
              Shared.source,
              Shared.context
            )
          )
        )
      )
  }

  def e13 = {
    val payload = CollectorPayload(
      Snowplow.Tp2,
      SpecHelpers.toNameValuePairs(
        "u" -> "https://github.com/snowplow/snowplow",
        "e" -> "se",
        "aid" -> "ads",
        "co" -> ""
      ),
      None,
      None,
      Shared.source,
      Shared.context
    )
    RedirectAdapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beValid(
          NonEmptyList.one(
            RawEvent(
              Snowplow.Tp2,
              Map(
                "e" -> "se",
                "aid" -> "ads",
                "tv" -> "r-tp2",
                "co" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/uri_redirect/jsonschema/1-0-0","data":{"uri":"https://github.com/snowplow/snowplow"}}]}""",
                "p" -> "web"
              ).toOpt,
              None,
              Shared.source,
              Shared.context
            )
          )
        )
      )
  }

  def e14 = {
    val payload = CollectorPayload(
      Snowplow.Tp2,
      SpecHelpers.toNameValuePairs(
        "u" -> "https://github.com/snowplow/snowplow",
        "e" -> "se",
        "co" -> """{"data":[{"data":{"osType":"OSX","appleIdfv":"some_appleIdfv","openIdfa":"some_Idfa","carrier":"some_carrier","deviceModel":"large","osVersion":"3.0.0","appleIdfa":"some_appleIdfa","androidIdfa":"some_androidIdfa","deviceManufacturer":"Amstrad"},"schema":"iglu:com.snowplowanalytics.snowplow/mobile_context/jsonschema/1-0-0"},{"data":{"longitude":10,"bearing":50,"speed":16,"altitude":20,"altitudeAccuracy":0.3,"latitudeLongitudeAccuracy":0.5,"latitude":7},"schema":"iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0"}],"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"}"""
      ),
      None,
      None,
      Shared.source,
      Shared.context
    )
    RedirectAdapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beValid(
          NonEmptyList.one(
            RawEvent(
              Snowplow.Tp2,
              Map(
                "e" -> "se",
                "tv" -> "r-tp2",
                "co" -> """{"data":[{"schema":"iglu:com.snowplowanalytics.snowplow/uri_redirect/jsonschema/1-0-0","data":{"uri":"https://github.com/snowplow/snowplow"}},{"data":{"osType":"OSX","appleIdfv":"some_appleIdfv","openIdfa":"some_Idfa","carrier":"some_carrier","deviceModel":"large","osVersion":"3.0.0","appleIdfa":"some_appleIdfa","androidIdfa":"some_androidIdfa","deviceManufacturer":"Amstrad"},"schema":"iglu:com.snowplowanalytics.snowplow/mobile_context/jsonschema/1-0-0"},{"data":{"longitude":10,"bearing":50,"speed":16,"altitude":20,"altitudeAccuracy":0.3,"latitudeLongitudeAccuracy":0.5,"latitude":7},"schema":"iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0"}],"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"}""",
                "p" -> "web"
              ).toOpt,
              None,
              Shared.source,
              Shared.context
            )
          )
        )
      )
  }

  def e15 = {
    val payload = CollectorPayload(
      Snowplow.Tp2,
      SpecHelpers.toNameValuePairs(
        "u" -> "https://github.com/snowplow/snowplow",
        "e" -> "se",
        "cx" -> CU.encodeBase64Url(
          """{"data":[{"data":{"osType":"OSX","appleIdfv":"some_appleIdfv","openIdfa":"some_Idfa","carrier":"some_carrier","deviceModel":"large","osVersion":"3.0.0","appleIdfa":"some_appleIdfa","androidIdfa":"some_androidIdfa","deviceManufacturer":"Amstrad"},"schema":"iglu:com.snowplowanalytics.snowplow/mobile_context/jsonschema/1-0-0"},{"data":{"longitude":10,"bearing":50,"speed":16,"altitude":20,"altitudeAccuracy":0.3,"latitudeLongitudeAccuracy":0.5,"latitude":7},"schema":"iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0"}],"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"}"""
        ),
        "p" -> "web"
      ),
      None,
      None,
      Shared.source,
      Shared.context
    )
    RedirectAdapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beValid(
          NonEmptyList.one(
            RawEvent(
              Snowplow.Tp2,
              Map(
                "e" -> "se",
                "tv" -> "r-tp2",
                "cx" -> CU.encodeBase64Url(
                  """{"data":[{"schema":"iglu:com.snowplowanalytics.snowplow/uri_redirect/jsonschema/1-0-0","data":{"uri":"https://github.com/snowplow/snowplow"}},{"data":{"osType":"OSX","appleIdfv":"some_appleIdfv","openIdfa":"some_Idfa","carrier":"some_carrier","deviceModel":"large","osVersion":"3.0.0","appleIdfa":"some_appleIdfa","androidIdfa":"some_androidIdfa","deviceManufacturer":"Amstrad"},"schema":"iglu:com.snowplowanalytics.snowplow/mobile_context/jsonschema/1-0-0"},{"data":{"longitude":10,"bearing":50,"speed":16,"altitude":20,"altitudeAccuracy":0.3,"latitudeLongitudeAccuracy":0.5,"latitude":7},"schema":"iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0"}],"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0"}"""
                ),
                "p" -> "web"
              ).toOpt,
              None,
              Shared.source,
              Shared.context
            )
          )
        )
      )
  }

  def e16 = {
    val payload = CollectorPayload(Snowplow.Tp2, Nil, None, None, Shared.source, Shared.context)
    RedirectAdapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.TrackerProtocolViolation.InputData(
              "querystring",
              None,
              "empty querystring: not a valid URI redirect"
            )
          )
        )
      )
  }

  def e17 = {
    val payload =
      CollectorPayload(
        Snowplow.Tp2,
        SpecHelpers.toNameValuePairs("aid" -> "test"),
        None,
        None,
        Shared.source,
        Shared.context
      )
    RedirectAdapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.TrackerProtocolViolation.InputData(
              "querystring",
              "aid=test".some,
              "missing `u` parameter: not a valid URI redirect"
            )
          )
        )
      )
  }

  def e18 = {
    val payload = CollectorPayload(
      Snowplow.Tp2,
      SpecHelpers.toNameValuePairs(
        "u" -> "https://github.com/snowplow/snowplow",
        "e" -> "se",
        "co" -> """{[-"""
      ),
      None,
      None,
      Shared.source,
      Shared.context
    )
    RedirectAdapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.TrackerProtocolViolation.NotJson(
              "co|cx",
              "{[-".some,
              """invalid json: expected " got '[-' (line 1, column 2)"""
            )
          )
        )
      )
  }

  def e19 = {
    val payload = CollectorPayload(
      Snowplow.Tp2,
      SpecHelpers.toNameValuePairs(
        "u" -> "https://github.com/snowplow/snowplow",
        "e" -> "se",
        "cx" -> "¢¢¢"
      ),
      None,
      None,
      Shared.source,
      Shared.context
    )
    RedirectAdapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.TrackerProtocolViolation
              .NotJson("co|cx", "".some, "invalid json: exhausted input")
          )
        )
      )
  }

  def e20 = {
    val payload = CollectorPayload(
      Snowplow.Tp2,
      SpecHelpers.toNameValuePairs(
        "u" -> null, // happens with &u in the query string
        "cx" -> "dGVzdHRlc3R0ZXN0"
      ),
      None,
      None,
      Shared.source,
      Shared.context
    )
    RedirectAdapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.TrackerProtocolViolation.InputData(
              "querystring",
              "u=null&cx=dGVzdHRlc3R0ZXN0".some,
              "missing `u` parameter: not a valid URI redirect"
            )
          )
        )
      )
  }
}
