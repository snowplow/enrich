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

import cats.effect.testing.specs2.CatsEffect

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class MailgunAdapterSpec extends Specification with CatsEffect {
  "enrichWith" should {
    "enrich with MailgunAdapter" in {
      val body =
        """{"signature":{"token":"090e1ce3702378c8121f3765a8efe0ffb97c4e2ca2adda6729","timestamp":"1657907833","signature":"496413b238ab6affce021b850d3fc72c4832fb04f1aed6d4a2fda7d08e6e95a6"},"event-data":{"id":"CPgfbmQMTCKtHW6uIWtuVe","timestamp":1521472262.908181,"log-level":"info","event":"delivered","delivery-status":{"tls":true,"mx-host":"smtp-in.example.com","code":250,"description":"","session-seconds":0.4331989288330078,"utf8":true,"attempt-no":1,"message":"OK","certificate-verified":true},"flags":{"is-routed":false,"is-authenticated":true,"is-system-test":false,"is-test-mode":false},"envelope":{"transport":"smtp","sender":"bob@sandbox22aee81b13674403a5335202df94f7e7.mailgun.org","sending-ip":"209.61.154.250","targets":"alice@example.com"},"message":{"headers":{"to":"Alice <alice@example.com>","message-id":"20130503182626.18666.16540@sandbox22aee81b13674403a5335202df94f7e7.mailgun.org","from":"Bob <bob@sandbox22aee81b13674403a5335202df94f7e7.mailgun.org>","subject":"Test delivered webhook"},"attachments":[],"size":111},"recipient":"alice@example.com","recipient-domain":"example.com","storage":{"url":"https://se.api.mailgun.net/v3/domains/sandbox22aee81b13674403a5335202df94f7e7.mailgun.org/messages/message_key","key":"message_key"},"campaigns":[],"tags":["my_tag_1","my_tag_2"],"user-variables":{"my_var_1":"Mailgun Variable #1","my-var-2":"awesome"}}}"""
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/com.mailgun/v1",
        body = body.some,
        contentType = "application/json".some
      )
      val expected = Map(
        "v_tracker" -> "com.mailgun-v1",
        "event_vendor" -> "com.mailgun",
        "event_name" -> "message_delivered",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event" -> "unstruct",
        "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0", "data":{ "schema":"iglu:com.mailgun/message_delivered/jsonschema/1-0-0", "data":{"recipient":"alice@example.com","deliveryStatus":{"certificateVerified":true,"sessionSeconds":0.4331989288330078,"description":"","mxHost":"smtp-in.example.com","tls":true,"code":250,"attemptNo":1,"utf8":true,"message":"OK"},"timestamp":"2022-07-15T17:57:13.000Z","flags":{"isRouted":false,"isAuthenticated":true,"isSystemTest":false,"isTestMode":false},"tags":["my_tag_1","my_tag_2"],"signature":"496413b238ab6affce021b850d3fc72c4832fb04f1aed6d4a2fda7d08e6e95a6","id":"CPgfbmQMTCKtHW6uIWtuVe","recipientDomain":"example.com","userVariables":{"myVar1":"Mailgun Variable #1","myVar2":"awesome"},"token":"090e1ce3702378c8121f3765a8efe0ffb97c4e2ca2adda6729","message":{"headers":{"to":"Alice <alice@example.com>","messageId":"20130503182626.18666.16540@sandbox22aee81b13674403a5335202df94f7e7.mailgun.org","from":"Bob <bob@sandbox22aee81b13674403a5335202df94f7e7.mailgun.org>","subject":"Test delivered webhook"},"attachments":[],"size":111},"storage":{"url":"https://se.api.mailgun.net/v3/domains/sandbox22aee81b13674403a5335202df94f7e7.mailgun.org/messages/message_key","key":"message_key"},"campaigns":[],"envelope":{"transport":"smtp","sender":"bob@sandbox22aee81b13674403a5335202df94f7e7.mailgun.org","sendingIp":"209.61.154.250","targets":"alice@example.com"},"logLevel":"info"}}}""".noSpaces
      )
      BlackBoxTesting.runTest(input, expected)
    }
  }
}
