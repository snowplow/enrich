/*
 * Copyright (c) 2015-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry

import cats.data.{NonEmptyList, Validated}
import cats.syntax.either._
import cats.syntax.option._
import cats.effect.testing.specs2.CatsEffect
import io.circe.literal._
import io.circe.parser._
import org.joda.time.DateTime
import org.specs2.matcher.ValidatedMatchers
import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class UrbanAirshipAdapterSpec extends Specification with ValidatedMatchers with CatsEffect {

  object Shared {
    val api = CollectorPayload.Api("com.urbanairship.connect", "v1")
    val cljSource = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
    val context = CollectorPayload.Context(
      None,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    ) // NB the collector timestamp is set to None!
  }

  val adapterWithDefaultSchemas = UrbanAirshipAdapter(schemas = urbanAirshipSchemas)

  "toRawEvents" should {

    val validPayload = json"""{
      "id": "e3314efb-9058-dbaf-c4bb-b754fca73613",
      "offset": "1",
      "occurred": "2015-11-13T16:31:52.393Z",
      "processed": "2015-11-13T16:31:52.393Z",
      "device": {
        "amazon_channel": "cd97c95c-ed77-f15a-3a67-5c2e26799d35"
      },
      "body": {
        "session_id": "27c75cab-a0b8-9da2-bc07-6d7253e0e13f"
      },
      "type": "CLOSE"
    }"""

    val invalidEvent = json"""{
      "id": "e3314efb-9058-dbaf-c4bb-b754fca73613",
      "offset": "1",
      "occurred": "2015-11-13T16:31:52.393Z",
      "processed": "2015-11-13T16:31:52.393Z",
      "device": {
        "amazon_channel": "cd97c95c-ed77-f15a-3a67-5c2e26799d35"
      },
      "body": {
        "session_id": "27c75cab-a0b8-9da2-bc07-6d7253e0e13f"
      },
      "type": "NOT_AN_EVENT_TYPE"
    }"""

    val payload = CollectorPayload(
      Shared.api,
      Nil,
      None,
      validPayload.noSpaces.some,
      Shared.cljSource,
      Shared.context
    )
    val actual = adapterWithDefaultSchemas.toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup)

    val expectedUnstructEventJson = json"""{
      "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
      "data":{
        "schema":"iglu:com.urbanairship.connect/CLOSE/jsonschema/1-0-0",
        "data":{
            "id": "e3314efb-9058-dbaf-c4bb-b754fca73613",
            "offset": "1",
            "occurred": "2015-11-13T16:31:52.393Z",
            "processed": "2015-11-13T16:31:52.393Z",
            "device": {
                "amazon_channel": "cd97c95c-ed77-f15a-3a67-5c2e26799d35"
            },
            "body": {
              "session_id": "27c75cab-a0b8-9da2-bc07-6d7253e0e13f"
            },
            "type": "CLOSE"
        }
      }
    }"""

    "return the correct number of events (1)" in {
      actual.map { output =>
        output must beValid
        val items = output.toList.head.toList
        items must have size 1
      }
    }

    "link to the correct json schema for the event type" in {
      actual.map { output =>
        output must beValid
        val correctType = validPayload.hcursor.get[String]("type")
        correctType must be equalTo (Right("CLOSE"))
        val items = output.toList.head.toList
        val sentSchema = parse(items.head.parameters("ue_pr").getOrElse("{}"))
          .leftMap(_.getMessage)
          .flatMap(_.hcursor.downField("data").get[String]("schema").leftMap(_.getMessage))
        sentSchema must beRight("""iglu:com.urbanairship.connect/CLOSE/jsonschema/1-0-0""")
      }
    }

    "fail on unknown event types" in {
      val payload = CollectorPayload(
        Shared.api,
        Nil,
        None,
        invalidEvent.noSpaces.some,
        Shared.cljSource,
        Shared.context
      )
      adapterWithDefaultSchemas.toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup).map(_ must beInvalid)
    }

    "reject unparsable json" in {
      val payload =
        CollectorPayload(Shared.api, Nil, None, """{ """.some, Shared.cljSource, Shared.context)
      adapterWithDefaultSchemas.toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup).map(_ must beInvalid)
    }

    "reject badly formatted json" in {
      val payload =
        CollectorPayload(
          Shared.api,
          Nil,
          None,
          """{ "value": "str" }""".some,
          Shared.cljSource,
          Shared.context
        )
      adapterWithDefaultSchemas.toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup).map(_ must beInvalid)
    }

    "reject content types" in {
      val payload = CollectorPayload(
        Shared.api,
        Nil,
        "a/type".some,
        validPayload.noSpaces.some,
        Shared.cljSource,
        Shared.context
      )
      adapterWithDefaultSchemas
        .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup)
        .map(
          _ must beInvalid(
            NonEmptyList.one(
              FailureDetails.AdapterFailure
                .InputData("contentType", "a/type".some, "expected no content type")
            )
          )
        )
    }

    "populate content-type as None (it's not applicable)" in {
      actual.map { output =>
        val contentType = output.getOrElse(throw new IllegalStateException).head.contentType
        contentType must beEqualTo(None)
      }
    }

    "have the correct collector source" in {
      actual.map { output =>
        val source = output.getOrElse(throw new IllegalStateException).head.source
        source must beEqualTo(Shared.cljSource)
      }
    }

    "have the correct context, including setting the correct collector timestamp" in {
      actual.map { output =>
        val context = output.getOrElse(throw new IllegalStateException).head.context
        Shared.context.timestamp mustEqual None
        context mustEqual Shared.context.copy(
          timestamp = DateTime
            .parse("2015-11-13T16:31:52.393Z")
            .some
        ) // it should be set to the "processed" field by the adapter
      }
    }

    "return the correct unstruct_event json" in {
      actual.map {
        case Validated.Valid(successes) =>
          val event = successes.head
          parse(event.parameters("ue_pr").getOrElse("{}")) must beRight(expectedUnstructEventJson)
        case _ => ko("payload was not accepted")
      }
    }

    "correctly populate the true timestamp" in {
      actual.map {
        case Validated.Valid(successes) =>
          val event = successes.head
          // "occurred" field value in ms past epoch (2015-11-13T16:31:52.393Z)
          event.parameters("ttm") must beEqualTo(Some("1447432312393"))
        case _ => ko("payload was not populated")
      }
    }

    "correctly populate the eid" in {
      actual.map {
        case Validated.Valid(successes) =>
          val event = successes.head
          // id field value
          event.parameters("eid") must beEqualTo(Some("e3314efb-9058-dbaf-c4bb-b754fca73613"))
        case _ => ko("payload was not populated")
      }
    }
  }
}
