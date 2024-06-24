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
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.enrichments

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsEffect

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class CurrencyConversionEnrichmentTransactionItemSpec extends Specification with CatsEffect {

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
