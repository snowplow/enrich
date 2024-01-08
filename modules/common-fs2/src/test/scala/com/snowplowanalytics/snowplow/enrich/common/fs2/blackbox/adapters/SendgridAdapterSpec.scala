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
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.adapters

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsIO

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class SendgridAdapterSpec extends Specification with CatsIO {
  "enrichWith" should {
    "enrich with SendgridAdapter" in {
      val body =
        json"""[{"email":"example@test.com","timestamp":1446549615,"smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e","event":"processed","category":"cat facts","sg_event_id":"sZROwMGMagFgnOEmSdvhig==","sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0","marketing_campaign_id":12345,"marketing_campaign_name":"campaign name","marketing_campaign_version":"B","marketing_campaign_split_id":13471}]"""
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/com.sendgrid/v3",
        body = body.noSpaces.some,
        contentType = "application/json".some
      )
      val expected = Map(
        "v_tracker" -> "com.sendgrid-v3",
        "event_vendor" -> "com.sendgrid",
        "event_name" -> "processed",
        "event_format" -> "jsonschema",
        "event_version" -> "3-0-0",
        "event" -> "unstruct",
        "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.sendgrid/processed/jsonschema/3-0-0","data":{"timestamp":"2015-11-03T11:20:15.000Z","email":"example@test.com","marketing_campaign_name":"campaign name","sg_event_id":"sZROwMGMagFgnOEmSdvhig==","smtp-id":"<14c5d75ce93.dfd.64b469@ismtpd-555>","marketing_campaign_version":"B","marketing_campaign_id":12345,"marketing_campaign_split_id":13471,"category":"cat facts","sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0"}}}""".noSpaces
      )
      BlackBoxTesting.runTest(input, expected)
    }
  }
}
