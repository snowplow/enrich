/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.adapters

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsEffect

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class MarketoAdapterSpec extends Specification with CatsEffect {
  "enrichWith" should {
    "enrich with MarketoAdapter" in {
      val body =
        json"""{"name": "webhook for A", "step": 6, "campaign": {"id": 160, "name": "avengers assemble"}, "lead": {"acquisition_date": "2010-11-11 11:11:11", "black_listed": false, "first_name": "the hulk", "updated_at": "2018-06-16 11:23:58", "created_at": "2018-06-16 11:23:58", "last_interesting_moment_date": "2018-09-26 20:26:40"}, "company": {"name": "iron man", "notes": "the something dog leapt over the lazy fox"}, "campaign": {"id": 987, "name": "triggered event"}, "datetime": "2018-03-07 14:28:16"}"""
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/com.marketo/v1",
        body = body.noSpaces.some,
        contentType = "application/json".some
      )
      val expected = Map(
        "v_tracker" -> "com.marketo-v1",
        "event_vendor" -> "com.marketo",
        "event_name" -> "event",
        "event_format" -> "jsonschema",
        "event_version" -> "2-0-0",
        "event" -> "unstruct",
        "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.marketo/event/jsonschema/2-0-0","data":{"lead":{"first_name":"the hulk","acquisition_date":"2010-11-11T11:11:11.000Z","black_listed":false,"last_interesting_moment_date":"2018-09-26T20:26:40.000Z","created_at":"2018-06-16T11:23:58.000Z","updated_at":"2018-06-16T11:23:58.000Z"},"name":"webhook for A","step":6,"campaign":{"id":987,"name":"triggered event"},"datetime":"2018-03-07T14:28:16.000Z","company":{"name":"iron man","notes":"the something dog leapt over the lazy fox"}}}}""".noSpaces
      )
      BlackBoxTesting.runTest(input, expected)
    }
  }
}
