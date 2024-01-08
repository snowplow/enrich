/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
