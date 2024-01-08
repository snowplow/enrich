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

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsIO

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class TransactionSpec extends Specification with CatsIO {
  "enrichWith" should {
    "enrich transactions" in {
      val querystring =
        "e=tr&tr_id=order-123&tr_af=pb&tr_tt=8000&tr_tx=200&tr_sh=50&tr_ci=London&tr_st=England&tr_co=UK&cx=eyJkYXRhIjpbeyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91cmlfcmVkaXJlY3QvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsidXJpIjoiaHR0cDovL3Nub3dwbG93YW5hbHl0aWNzLmNvbS8ifX1dLCJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIn0=&tid=028288"
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/ice.png",
        querystring = querystring.some
      )
      val expected = Map(
        "event_vendor" -> "com.snowplowanalytics.snowplow",
        "event_name" -> "transaction",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event" -> "transaction",
        "tr_affiliation" -> "pb",
        "tr_total" -> "8000",
        "tr_total_base" -> "",
        "tr_tax" -> "200",
        "tr_tax_base" -> "",
        "tr_shipping" -> "50",
        "tr_shipping_base" -> "",
        "tr_orderid" -> "order-123",
        "tr_state" -> "England",
        "txn_id" -> "28288",
        "tr_country" -> "UK",
        "tr_city" -> "London",
        "contexts" -> json"""{"data":[{"schema":"iglu:com.snowplowanalytics.snowplow/uri_redirect/jsonschema/1-0-0","data":{"uri":"http://snowplowanalytics.com/"}}],"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1"}""".noSpaces
      )
      BlackBoxTesting.runTest(input, expected)
    }
  }
}
