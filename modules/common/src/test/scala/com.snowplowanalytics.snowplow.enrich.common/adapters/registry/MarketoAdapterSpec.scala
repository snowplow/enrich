/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
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

class MarketoAdapterSpec extends Specification with DataTables with ValidatedMatchers with CatsEffect {
  def is = s2"""
  toRawEvents must return a success for a valid "event" type payload body being passed                $e1
  toRawEvents must return a Failure Nel if the payload body is empty                                  $e2
  """

  object Shared {
    val api = CollectorPayload.Api("com.marketo", "v1")
    val cljSource = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
    val context = CollectorPayload.Context(
      DateTime.parse("2018-01-01T00:00:00.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    )
  }

  val adapterWithDefaultSchemas = MarketoAdapter(schemas = marketoSchemas)
  val ContentType = "application/json"

  def e1 = {
    val bodyStr =
      """{"name": "webhook for A", "step": 6, "lead": {"acquisition_date": "2010-11-11 11:11:11", "black_listed": false, "first_name": "the hulk", "updated_at": "", "created_at": "2018-06-16 11:23:58", "last_interesting_moment_date": "2018-09-26 20:26:40"}, "company": {"name": "iron man", "notes": "the something dog leapt over the lazy fox"}, "campaign": {"id": 987, "name": "triggered event"}, "datetime": "2018-03-07 14:28:16"}"""
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
          "tv" -> "com.marketo-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.marketo/event/jsonschema/2-0-0","data":{"lead":{"first_name":"the hulk","acquisition_date":"2010-11-11T11:11:11.000Z","black_listed":false,"last_interesting_moment_date":"2018-09-26T20:26:40.000Z","created_at":"2018-06-16T11:23:58.000Z","updated_at":""},"name":"webhook for A","step":6,"campaign":{"id":987,"name":"triggered event"},"datetime":"2018-03-07T14:28:16.000Z","company":{"name":"iron man","notes":"the something dog leapt over the lazy fox"}}}}"""
        ).toOpt,
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
}
