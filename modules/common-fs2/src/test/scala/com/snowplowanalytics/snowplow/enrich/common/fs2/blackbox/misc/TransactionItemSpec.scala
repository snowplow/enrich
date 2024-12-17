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
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.misc

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsEffect

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class TransactionItemSpec extends Specification with CatsEffect {
  "enrichWith" should {
    "enrich transaction items" in {
      val querystring =
        "e=ti&ti_id=order-123&ti_sk=PBZ1001&ti_na=Blue%20t-shirt&ti_ca=APPAREL&ti_pr=2000&ti_qu=2"
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/ice.png",
        querystring = querystring.some
      )
      val expected = Map(
        "event_vendor" -> "com.snowplowanalytics.snowplow",
        "event_name" -> "transaction_item",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event" -> "transaction_item",
        "ti_orderid" -> "order-123",
        "ti_sku" -> "PBZ1001",
        "ti_quantity" -> "2",
        "ti_currency" -> "",
        "ti_category" -> "APPAREL",
        "ti_price" -> "2000",
        "ti_price_base" -> ""
      )
      BlackBoxTesting.runTest(input, expected)
    }
  }
}
