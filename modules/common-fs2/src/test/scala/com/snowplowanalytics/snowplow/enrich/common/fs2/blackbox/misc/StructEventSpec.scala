/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.misc

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsIO

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class StructEventSpec extends Specification with CatsIO {
  "enrichWith" should {
    "enrich struct events" in {
      val querystring =
        "e=se&se_ca=ecomm&se_ac=add-to-basket&se_la=%CE%A7%CE%B1%CF%81%CE%B9%CF%84%CE%AF%CE%BD%CE%B7&se_pr=1&se_va=35708.23"
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/ice.png",
        querystring = querystring.some
      )
      val expected = Map(
        "event_vendor" -> "com.google.analytics",
        "event_name" -> "event",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event" -> "struct",
        "se_action" -> "add-to-basket",
        "se_category" -> "ecomm",
        "se_label" -> "Χαριτίνη",
        "se_property" -> "1",
        "se_value" -> "35708.23"
      )
      BlackBoxTesting.runTest(input, expected)
    }
  }
}
