/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.enrichments

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsIO

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class CurrencyConversionEnrichmentTransactionItemSpec extends Specification with CatsIO {

  args(skipAll = !sys.env.contains("OER_KEY"))

  "enrichWith" should {
    "enrich with CurrencyConversionEnrichment" in {
      val querystring =
        "e=ti&ti_id=order-123&ti_sk=PBZ1001&ti_na=Blue%20t-shirt&ti_ca=APPAREL&ti_pr=2000&ti_qu=2&ti_cu=GBP&tid=851830"
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/ice.png",
        querystring = querystring.some,
        timestamp = 1562008983000L
      )
      val expected = Map(
        "event_vendor" -> "com.snowplowanalytics.snowplow",
        "event_name" -> "transaction_item",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event" -> "transaction_item",
        "base_currency" -> "EUR",
        "ti_currency" -> "GBP",
        "ti_orderid" -> "order-123",
        "ti_sku" -> "PBZ1001",
        "ti_quantity" -> "2",
        "ti_category" -> "APPAREL",
        "ti_price" -> "2000",
        "ti_price_base" -> "2240.45",
        "ti_name" -> "Blue t-shirt",
        "collector_tstamp" -> "2019-07-01 19:23:03.000"
      )
      BlackBoxTesting.runTest(input, expected, Some(CurrencyConversionEnrichmentTransactionItemSpec.conf))
    }
  }
}

object CurrencyConversionEnrichmentTransactionItemSpec {
  val conf = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/currency_conversion_config/jsonschema/1-0-0",
      "data": {
        "enabled": true,
        "vendor": "com.snowplowanalytics.snowplow",
        "name": "currency_conversion_config",
        "parameters": {
          "accountType": "DEVELOPER",
          "apiKey": ${sys.env.get("OER_KEY").getOrElse("-")},
          "baseCurrency": "EUR",
          "rateAt": "EOD_PRIOR"
        }
      }
    }
  """
}
