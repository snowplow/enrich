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

import cats.Monad
import cats.data.{NonEmptyList, Validated}
import cats.syntax.option._
import cats.syntax.validated._

import cats.effect.Clock

import io.circe._
import io.circe.literal._

import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class AdapterSpec extends Specification with DataTables with ValidatedMatchers {
  def is = s2"""
  toMap should convert a list of name-value pairs into a map                                                   $e1
  toUnstructEventParams should generate a boilerplate set of parameters for an empty unstructured event        $e2
  toUnstructEventParams should preserve nuid, aid, cv, url, eid, ttm and p outside of the unstructured event   $e3
  lookupSchema must return a Validated.Valid Nel for a valid key being passed against an event-schema map      $e4
  lookupSchema must return a Validated.Invalid Nel for an invalid key being passed against an event-schema map $e5
  lookupSchema must return a Validated.Invalid Nel with an index if one is passed to it                        $e6
  rawEventsListProcessor must return a Validated.Invalid Nel if there are any Validated.Invalids in the list   $e7
  rawEventsListProcessor must return a Validated.Valid Nel of RawEvents if the list is full of success         $e8
  cleanupJsonEventValues must clean 'ts':[JInt, JString] fields into to a valid JsonSchema date-time format    $e9
  cleanupJsonEventValues must remove key-pairs if specified                                                    $e10
  """

  // TODO: add test for buildFormatter()

  object BaseAdapter extends Adapter {
    override def toRawEvents[F[_]: Monad: Clock](
      payload: CollectorPayload,
      client: IgluCirceClient[F],
      registryLookup: RegistryLookup[F],
      maxJsonDepth: Int
    ) =
      Monad[F].pure(
        FailureDetails.AdapterFailure
          .InputData("base", None, payload.body.getOrElse("base"))
          .invalidNel
      )
  }

  object Shared {
    val api = CollectorPayload.Api("com.adapter", "v1")
    val cljSource = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
    val context = CollectorPayload.Context(
      DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    )
    val contentType = "application/x-www-form-urlencoded"
  }

  private val SchemaMap = Map(
    "adapterTest" -> SchemaKey("com.adaptertest", "test", "jsonschema", SchemaVer.Full(1, 0, 0))
  )

  def e1 = {
    val pairs = toNameValuePairs("a" -> "1", "b" -> "2", "c" -> "3")
    BaseAdapter.toMap(pairs) must_== Map("a" -> "1", "b" -> "2", "c" -> "3").toOpt
  }

  def e2 = {
    val params = BaseAdapter.toUnstructEventParams(
      "tv",
      Map.empty[String, Option[String]],
      SchemaKey("com.acme", "foo", "jsonschema", SchemaVer.Full(1, 0, 1)),
      _ => Json.fromJsonObject(JsonObject.empty),
      "app"
    )
    params must_== Map(
      "tv" -> "tv",
      "e" -> "ue",
      "p" -> "app",
      "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.acme/foo/jsonschema/1-0-1","data":{}}}"""
    ).toOpt
  }

  def e3 = {
    val shared = Map(
      "nuid" -> "123",
      "aid" -> "42",
      "cv" -> "clj-tomcat",
      "p" -> "srv",
      "eid" -> "321",
      "ttm" -> "2015-11-13T16:31:52.393Z",
      "url" -> "http://localhost"
    ).toOpt
    val params = BaseAdapter.toUnstructEventParams(
      "tv",
      shared,
      SchemaKey("com.acme", "foo", "jsonschema", SchemaVer.Full(1, 0, 1)),
      _ => Json.fromJsonObject(JsonObject.empty),
      "app"
    )
    params must_== shared ++ Map(
      "tv" -> "tv",
      "e" -> "ue",
      "eid" -> "321",
      "ttm" -> "2015-11-13T16:31:52.393Z",
      "url" -> "http://localhost",
      "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.acme/foo/jsonschema/1-0-1","data":{}}}"""
    ).toOpt
  }

  def e4 = {
    val expected = SchemaKey("com.adaptertest", "test", "jsonschema", SchemaVer.Full(1, 0, 0))
    BaseAdapter.lookupSchema("adapterTest".some, SchemaMap) must beRight(expected)
  }

  def e5 =
    "SPEC NAME" || "SCHEMA TYPE" | "EXPECTED OUTPUT" |
      "Failing, nothing passed" !! None ! FailureDetails.AdapterFailure.SchemaMapping(
        None,
        SchemaMap,
        "cannot determine event type: type parameter not provided"
      ) |
      "Failing, empty type" !! Some("") ! FailureDetails.AdapterFailure.SchemaMapping(
        "".some,
        SchemaMap,
        "cannot determine event type: type parameter empty"
      ) |
      "Failing, bad type passed" !! Some("bad") ! FailureDetails.AdapterFailure
        .SchemaMapping(
          "bad".some,
          SchemaMap,
          "no schema associated with the provided type parameter"
        ) |> { (_, et, expected) =>
      BaseAdapter.lookupSchema(et, SchemaMap) must beLeft(expected)
    }

  def e6 = {
    val expected = FailureDetails.AdapterFailure.SchemaMapping(
      None,
      SchemaMap,
      "cannot determine event type: type parameter not provided at index 2"
    )
    BaseAdapter.lookupSchema(None, 2, SchemaMap) must beLeft(expected)
  }

  def e7 = {
    val rawEvent = RawEvent(
      Shared.api,
      Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv").toOpt,
      Shared.contentType.some,
      Shared.cljSource,
      Shared.context
    )
    val validatedRawEventsList =
      List(
        Validated.Valid(rawEvent),
        Validated.Invalid(
          NonEmptyList
            .one(FailureDetails.AdapterFailure.InputData("s1", None, "failure 1"))
        ),
        Validated.Invalid(
          NonEmptyList
            .one(FailureDetails.AdapterFailure.InputData("s2", None, "failure 2"))
        )
      )
    val expected = NonEmptyList.of(
      FailureDetails.AdapterFailure.InputData("s1", None, "failure 1"),
      FailureDetails.AdapterFailure.InputData("s2", None, "failure 2")
    )
    BaseAdapter.rawEventsListProcessor(validatedRawEventsList) must beInvalid(expected)
  }

  def e8 = {
    val rawEvent = RawEvent(
      Shared.api,
      Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv").toOpt,
      Shared.contentType.some,
      Shared.cljSource,
      Shared.context
    )
    val validatedRawEventsList =
      List(Validated.Valid(rawEvent), Validated.Valid(rawEvent), Validated.Valid(rawEvent))
    val expected = NonEmptyList.of(
      RawEvent(
        Shared.api,
        Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv").toOpt,
        Shared.contentType.some,
        Shared.cljSource,
        Shared.context
      ),
      RawEvent(
        Shared.api,
        Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv").toOpt,
        Shared.contentType.some,
        Shared.cljSource,
        Shared.context
      ),
      RawEvent(
        Shared.api,
        Map("tv" -> "com.adapter-v1", "e" -> "ue", "p" -> "srv").toOpt,
        Shared.contentType.some,
        Shared.cljSource,
        Shared.context
      )
    )
    BaseAdapter.rawEventsListProcessor(validatedRawEventsList) must beValid(expected)
  }

  def e9 =
    "SPEC NAME" || "JSON" | "EXPECTED OUTPUT" |
      "Change one value" !! json"""{"ts":1415709559}""" ! json"""{ "ts": "2014-11-11T12:39:19.000Z" }""" |
      "Change multiple values" !! json"""{"ts":1415709559,"ts":1415700000}""" !
        json"""{ "ts": "2014-11-11T12:39:19.000Z", "ts": "2014-11-11T10:00:00.000Z"}""" |
      "Change nested values" !! json"""{"ts":1415709559,"nested":{"ts":1415700000}}""" !
        json"""{ "ts": "2014-11-11T12:39:19.000Z", "nested": {"ts": "2014-11-11T10:00:00.000Z" }}""" |
      "Change nested string values" !! json"""{"ts":1415709559,"nested":{"ts":"1415700000"}}""" !
        json"""{ "ts": "2014-11-11T12:39:19.000Z", "nested": { "ts": "2014-11-11T10:00:00.000Z" }}""" |
      "JStrings should also be changed" !! json"""{"ts":"1415709559"}""" !
        json"""{ "ts" : "2014-11-11T12:39:19.000Z" }""" |> { (_, json, expected) =>
      BaseAdapter.cleanupJsonEventValues(json, None, List("ts")) mustEqual expected
    }

  def e10 =
    "SPEC NAME" || "JSON" | "EXPECTED OUTPUT" |
      "Remove 'event'->'type'" !! json"""{"an_event":"type"}""" ! json"""{}""" |
      "Not remove existing values" !! json"""{"abc":1415709559, "an_event":"type", "cba":"type"}""" !
        json"""{ "abc": 1415709559, "cba": "type" }""" |
      "Works with ts value subs" !! json"""{"ts":1415709559, "an_event":"type", "abc":"type"}""" !
        json"""{ "ts": "2014-11-11T12:39:19.000Z", "abc": "type" }""" |
      "Removes nested values" !! json"""{"abc":"abc","nested":{"an_event":"type"}}""" !
        json"""{ "abc": "abc", "nested": {}}""" |> { (_, json, expected) =>
      BaseAdapter.cleanupJsonEventValues(json, ("an_event", "type").some, List("ts")) mustEqual
        expected
    }

}
