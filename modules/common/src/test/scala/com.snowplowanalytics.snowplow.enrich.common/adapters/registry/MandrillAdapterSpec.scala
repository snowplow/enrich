/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
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
import io.circe.literal._
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class MandrillAdapterSpec extends Specification with DataTables with ValidatedMatchers with CatsEffect {
  def is = s2"""
  payloadBodyToEvents must return a Success List[JValue] for a valid events string                      $e1
  payloadBodyToEvents must return a Failure String if the mapped events string is not in a valid format $e2
  payloadBodyToEvents must return a Failure String if the event string could not be parsed into JSON    $e3
  toRawEvents must return a Success Nel if every event in the payload is successful                     $e4
  toRawEvents must return a Failure Nel if any of the events in the payload do not pass full validation $e5
  toRawEvents must return a Failure Nel if the payload body is empty                                    $e6
  toRawEvents must return a Failure Nel if the payload content type is empty                            $e7
  toRawEvents must return a Failure Nel if the payload content type does not match expectation          $e8
  """

  object Shared {
    val api = CollectorPayload.Api("com.mandrill", "v1")
    val cljSource = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
    val context = CollectorPayload.Context(
      DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    )
  }

  val adapterWithDefaultSchemas = MandrillAdapter(schemas = mandrillSchemas)
  val ContentType = "application/x-www-form-urlencoded"

  def e1 = {
    val bodyStr = "mandrill_events=%5B%7B%22event%22%3A%20%22subscribe%22%7D%5D"
    val expected = List(json"""{"event": "subscribe"}""")
    adapterWithDefaultSchemas.payloadBodyToEvents(bodyStr, SpecHelpers.DefaultMaxJsonDepth) must beRight(expected)
  }

  def e2 =
    "SPEC NAME" || "STRING TO PROCESS" | "EXPECTED OUTPUT" |
      "Failure, empty events string" !! "mandrill_events=" ! FailureDetails.AdapterFailure
        .InputData(
          "body",
          "mandrill_events=".some,
          "`mandrill_events` field is empty"
        ) |
      "Failure, too many key-value pairs" !! "mandrill_events=some&mandrill_extra=some" ! FailureDetails.AdapterFailure
        .InputData(
          "body",
          "mandrill_events=some&mandrill_extra=some".some,
          "body should have size 1: actual size 2"
        ) |
      "Failure, incorrect key" !! "events_mandrill=something" ! FailureDetails.AdapterFailure
        .InputData(
          "body",
          "events_mandrill=something".some,
          "no `mandrill_events` parameter provided"
        ) |> { (_, str, expected) =>
      adapterWithDefaultSchemas.payloadBodyToEvents(str, SpecHelpers.DefaultMaxJsonDepth) must beLeft(expected)
    }

  def e3 = {
    val bodyStr = "mandrill_events=%5B%7B%22event%22%3A%22click%7D%5D"
    val expected = FailureDetails.AdapterFailure.NotJson(
      "mandril_events",
      """[{"event":"click}]""".some,
      "invalid json: exhausted input"
    )
    adapterWithDefaultSchemas.payloadBodyToEvents(bodyStr, SpecHelpers.DefaultMaxJsonDepth) must beLeft(expected)
  }

  def e4 = { // Spec for nine seperate events being passed and returned.
    val bodyStr =
      "mandrill_events=%5B%7B%22event%22%3A%22send%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%5D%2C%22clicks%22%3A%5B%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa%22%2C%22ts%22%3A1695142708%7D%2C%7B%22event%22%3A%22delivered%22%2C%22ts%22%3A1695142708%2C%22diag%22%3A%22250+2.0.0+OK+1234567890+abcdefghijk.000%22%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa1%22%2C%22msg%22%3A%7B%22ts%22%3A1643831131%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa1%22%2C%22state%22%3A%22sent%22%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%5D%2C%22clicks%22%3A%5B%5D%2C%22smtp_events%22%3A%5B%7B%22ts%22%3A1643831132%2C%22type%22%3A%22sent%22%2C%22diag%22%3A%22250+2.0.0+OK+1234567890+abcdefghijk.000%22%2C%22source_ip%22%3A%22127.0.0.1%22%2C%22destination_ip%22%3A%22127.0.0.1%22%2C%22size%22%3A0%7D%5D%2C%22resends%22%3A%5B%5D%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22template%22%3Anull%7D%7D%2C%7B%22event%22%3A%22deferral%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%5D%2C%22clicks%22%3A%5B%5D%2C%22state%22%3A%22deferred%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa2%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%2C%22smtp_events%22%3A%5B%7B%22destination_ip%22%3A%22127.0.0.1%22%2C%22diag%22%3A%22451+4.3.5+Temporarily+unavailable%2C+try+again+later.%22%2C%22source_ip%22%3A%22127.0.0.1%22%2C%22ts%22%3A1365111111%2C%22type%22%3A%22deferred%22%2C%22size%22%3A0%7D%5D%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa2%22%2C%22ts%22%3A1695142708%7D%2C%7B%22event%22%3A%22hard_bounce%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22state%22%3A%22bounced%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa3%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%2C%22bounce_description%22%3A%22bad_mailbox%22%2C%22bgtools_code%22%3A10%2C%22diag%22%3A%22smtp%3B550+5.1.1+The+email+account+that+you+tried+to+reach+does+not+exist.+Please+try+double-checking+the+recipient%27s+email+address+for+typos+or+unnecessary+spaces.%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa3%22%2C%22ts%22%3A1695142708%7D%2C%7B%22event%22%3A%22soft_bounce%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22state%22%3A%22soft-bounced%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa4%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%2C%22bounce_description%22%3A%22mailbox_full%22%2C%22bgtools_code%22%3A22%2C%22diag%22%3A%22smtp%3B552+5.2.2+Over+Quota%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa4%22%2C%22ts%22%3A1695142708%7D%2C%7B%22event%22%3A%22open%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%7B%22ts%22%3A1365111111%7D%5D%2C%22clicks%22%3A%5B%7B%22ts%22%3A1365111111%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%7D%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa5%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa5%22%2C%22ip%22%3A%22127.0.0.1%22%2C%22location%22%3A%7B%22country_short%22%3A%22US%22%2C%22country%22%3A%22United+States%22%2C%22region%22%3A%22Oklahoma%22%2C%22city%22%3A%22Oklahoma+City%22%2C%22latitude%22%3A35.4675598145%2C%22longitude%22%3A-97.5164337158%2C%22postal_code%22%3A%2273101%22%2C%22timezone%22%3A%22-05%3A00%22%7D%2C%22user_agent%22%3A%22Mozilla%5C%2F5.0+%28Macintosh%3B+U%3B+Intel+Mac+OS+X+10.6%3B+en-US%3B+rv%3A1.9.1.8%29+Gecko%5C%2F20100317+Postbox%5C%2F1.1.3%22%2C%22user_agent_parsed%22%3A%7B%22type%22%3A%22Email+Client%22%2C%22ua_family%22%3A%22Postbox%22%2C%22ua_name%22%3A%22Postbox+1.1.3%22%2C%22ua_version%22%3A%221.1.3%22%2C%22ua_url%22%3A%22http%3A%5C%2F%5C%2Fwww.postbox-inc.com%5C%2F%22%2C%22ua_company%22%3A%22Postbox%2C+Inc.%22%2C%22ua_company_url%22%3A%22http%3A%5C%2F%5C%2Fwww.postbox-inc.com%5C%2F%22%2C%22ua_icon%22%3A%22http%3A%5C%2F%5C%2Fcdn.mandrill.com%5C%2Fimg%5C%2Femail-client-icons%5C%2Fpostbox.png%22%2C%22os_family%22%3A%22OS+X%22%2C%22os_name%22%3A%22OS+X+10.6+Snow+Leopard%22%2C%22os_url%22%3A%22http%3A%5C%2F%5C%2Fwww.apple.com%5C%2Fosx%5C%2F%22%2C%22os_company%22%3A%22Apple+Computer%2C+Inc.%22%2C%22os_company_url%22%3A%22http%3A%5C%2F%5C%2Fwww.apple.com%5C%2F%22%2C%22os_icon%22%3A%22http%3A%5C%2F%5C%2Fcdn.mandrill.com%5C%2Fimg%5C%2Femail-client-icons%5C%2Fmacosx.png%22%2C%22mobile%22%3Afalse%7D%2C%22ts%22%3A1695142708%7D%2C%7B%22event%22%3A%22click%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%7B%22ts%22%3A1365111111%7D%5D%2C%22clicks%22%3A%5B%7B%22ts%22%3A1365111111%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%7D%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa6%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa6%22%2C%22ip%22%3A%22127.0.0.1%22%2C%22location%22%3A%7B%22country_short%22%3A%22US%22%2C%22country%22%3A%22United+States%22%2C%22region%22%3A%22Oklahoma%22%2C%22city%22%3A%22Oklahoma+City%22%2C%22latitude%22%3A35.4675598145%2C%22longitude%22%3A-97.5164337158%2C%22postal_code%22%3A%2273101%22%2C%22timezone%22%3A%22-05%3A00%22%7D%2C%22user_agent%22%3A%22Mozilla%5C%2F5.0+%28Macintosh%3B+U%3B+Intel+Mac+OS+X+10.6%3B+en-US%3B+rv%3A1.9.1.8%29+Gecko%5C%2F20100317+Postbox%5C%2F1.1.3%22%2C%22user_agent_parsed%22%3A%7B%22type%22%3A%22Email+Client%22%2C%22ua_family%22%3A%22Postbox%22%2C%22ua_name%22%3A%22Postbox+1.1.3%22%2C%22ua_version%22%3A%221.1.3%22%2C%22ua_url%22%3A%22http%3A%5C%2F%5C%2Fwww.postbox-inc.com%5C%2F%22%2C%22ua_company%22%3A%22Postbox%2C+Inc.%22%2C%22ua_company_url%22%3A%22http%3A%5C%2F%5C%2Fwww.postbox-inc.com%5C%2F%22%2C%22ua_icon%22%3A%22http%3A%5C%2F%5C%2Fcdn.mandrill.com%5C%2Fimg%5C%2Femail-client-icons%5C%2Fpostbox.png%22%2C%22os_family%22%3A%22OS+X%22%2C%22os_name%22%3A%22OS+X+10.6+Snow+Leopard%22%2C%22os_url%22%3A%22http%3A%5C%2F%5C%2Fwww.apple.com%5C%2Fosx%5C%2F%22%2C%22os_company%22%3A%22Apple+Computer%2C+Inc.%22%2C%22os_company_url%22%3A%22http%3A%5C%2F%5C%2Fwww.apple.com%5C%2F%22%2C%22os_icon%22%3A%22http%3A%5C%2F%5C%2Fcdn.mandrill.com%5C%2Fimg%5C%2Femail-client-icons%5C%2Fmacosx.png%22%2C%22mobile%22%3Afalse%7D%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%2C%22ts%22%3A1695142708%7D%2C%7B%22event%22%3A%22spam%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%7B%22ts%22%3A1365111111%7D%5D%2C%22clicks%22%3A%5B%7B%22ts%22%3A1365111111%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%7D%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa7%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa7%22%2C%22ts%22%3A1695142708%7D%2C%7B%22event%22%3A%22unsub%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%7B%22ts%22%3A1365111111%7D%5D%2C%22clicks%22%3A%5B%7B%22ts%22%3A1365111111%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%7D%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa8%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa8%22%2C%22ts%22%3A1695142708%7D%2C%7B%22event%22%3A%22reject%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%5D%2C%22clicks%22%3A%5B%5D%2C%22state%22%3A%22rejected%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa9%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa9%22%2C%22ts%22%3A1695142708%7D%5D"
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      ContentType.some,
      bodyStr.some,
      Shared.cljSource,
      Shared.context
    )
    val expected = NonEmptyList.of(
      RawEvent(
        Shared.api,
        Map(
          "tv" -> "com.mandrill-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mandrill/message_sent/jsonschema/1-0-1","data":{"msg":{"_version":"exampleaaaaaaaaaaaaaaa","subject":"This an example webhook message","email":"example.webhook@mandrillapp.com","state":"sent","_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa","tags":["webhook-example"],"ts":"2013-04-04T21:13:19.000Z","clicks":[],"metadata":{"user_id":111},"sender":"example.sender@mandrillapp.com","opens":[]},"_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa","ts":"2023-09-19T16:58:28.000Z"}}}"""
        ).toOpt,
        ContentType.some,
        Shared.cljSource,
        Shared.context
      ),
      RawEvent(
        Shared.api,
        Map(
          "tv" -> "com.mandrill-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mandrill/message_delivered/jsonschema/1-0-0","data":{"_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa1","diag":"250 2.0.0 OK 1234567890 abcdefghijk.000","ts":"2023-09-19T16:58:28.000Z","msg":{"resends":[],"_version":"exampleaaaaaaaaaaaaaaa","subject":"This an example webhook message","email":"example.sender@mandrillapp.com","state":"sent","_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa1","tags":["webhook-example"],"ts":"2022-02-02T19:45:31.000Z","smtp_events":[{"size":0,"destination_ip":"127.0.0.1","diag":"250 2.0.0 OK 1234567890 abcdefghijk.000","ts":"2022-02-02T19:45:32.000Z","source_ip":"127.0.0.1","type":"sent"}],"clicks":[],"template":null,"sender":"example.sender@mandrillapp.com","opens":[]}}}}"""
        ).toOpt,
        ContentType.some,
        Shared.cljSource,
        Shared.context
      ),
      RawEvent(
        Shared.api,
        Map(
          "tv" -> "com.mandrill-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mandrill/message_delayed/jsonschema/1-0-2","data":{"msg":{"_version":"exampleaaaaaaaaaaaaaaa","subject":"This an example webhook message","email":"example.webhook@mandrillapp.com","state":"deferred","_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa2","tags":["webhook-example"],"ts":"2013-04-04T21:13:19.000Z","smtp_events":[{"size":0,"destination_ip":"127.0.0.1","diag":"451 4.3.5 Temporarily unavailable, try again later.","ts":"2013-04-04T21:31:51.000Z","source_ip":"127.0.0.1","type":"deferred"}],"clicks":[],"metadata":{"user_id":111},"sender":"example.sender@mandrillapp.com","opens":[]},"_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa2","ts":"2023-09-19T16:58:28.000Z"}}}"""
        ).toOpt,
        ContentType.some,
        Shared.cljSource,
        Shared.context
      ),
      RawEvent(
        Shared.api,
        Map(
          "tv" -> "com.mandrill-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mandrill/message_bounced/jsonschema/1-0-2","data":{"msg":{"_version":"exampleaaaaaaaaaaaaaaa","subject":"This an example webhook message","email":"example.webhook@mandrillapp.com","state":"bounced","_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa3","tags":["webhook-example"],"diag":"smtp;550 5.1.1 The email account that you tried to reach does not exist. Please try double-checking the recipient's email address for typos or unnecessary spaces.","ts":"2013-04-04T21:13:19.000Z","metadata":{"user_id":111},"sender":"example.sender@mandrillapp.com","bounce_description":"bad_mailbox","bgtools_code":10},"_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa3","ts":"2023-09-19T16:58:28.000Z"}}}"""
        ).toOpt,
        ContentType.some,
        Shared.cljSource,
        Shared.context
      ),
      RawEvent(
        Shared.api,
        Map(
          "tv" -> "com.mandrill-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mandrill/message_soft_bounced/jsonschema/1-0-2","data":{"msg":{"_version":"exampleaaaaaaaaaaaaaaa","subject":"This an example webhook message","email":"example.webhook@mandrillapp.com","state":"soft-bounced","_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa4","tags":["webhook-example"],"diag":"smtp;552 5.2.2 Over Quota","ts":"2013-04-04T21:13:19.000Z","metadata":{"user_id":111},"sender":"example.sender@mandrillapp.com","bounce_description":"mailbox_full","bgtools_code":22},"_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa4","ts":"2023-09-19T16:58:28.000Z"}}}"""
        ).toOpt,
        ContentType.some,
        Shared.cljSource,
        Shared.context
      ),
      RawEvent(
        Shared.api,
        Map(
          "tv" -> "com.mandrill-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mandrill/message_opened/jsonschema/1-0-3","data":{"ip":"127.0.0.1","location":{"city":"Oklahoma City","latitude":35.4675598145,"timezone":"-05:00","country":"United States","longitude":-97.5164337158,"country_short":"US","postal_code":"73101","region":"Oklahoma"},"_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa5","ts":"2023-09-19T16:58:28.000Z","user_agent":"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; en-US; rv:1.9.1.8) Gecko/20100317 Postbox/1.1.3","msg":{"_version":"exampleaaaaaaaaaaaaaaa","subject":"This an example webhook message","email":"example.webhook@mandrillapp.com","state":"sent","_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa5","tags":["webhook-example"],"ts":"2013-04-04T21:13:19.000Z","clicks":[{"ts":"2013-04-04T21:31:51.000Z","url":"http://mandrill.com"}],"metadata":{"user_id":111},"sender":"example.sender@mandrillapp.com","opens":[{"ts":"2013-04-04T21:31:51.000Z"}]},"user_agent_parsed":{"os_company_url":"http://www.apple.com/","os_family":"OS X","os_company":"Apple Computer, Inc.","os_url":"http://www.apple.com/osx/","ua_url":"http://www.postbox-inc.com/","ua_icon":"http://cdn.mandrill.com/img/email-client-icons/postbox.png","ua_version":"1.1.3","os_name":"OS X 10.6 Snow Leopard","ua_company":"Postbox, Inc.","ua_family":"Postbox","os_icon":"http://cdn.mandrill.com/img/email-client-icons/macosx.png","ua_company_url":"http://www.postbox-inc.com/","ua_name":"Postbox 1.1.3","type":"Email Client","mobile":false}}}}"""
        ).toOpt,
        ContentType.some,
        Shared.cljSource,
        Shared.context
      ),
      RawEvent(
        Shared.api,
        Map(
          "tv" -> "com.mandrill-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mandrill/message_clicked/jsonschema/1-0-2","data":{"ip":"127.0.0.1","location":{"city":"Oklahoma City","latitude":35.4675598145,"timezone":"-05:00","country":"United States","longitude":-97.5164337158,"country_short":"US","postal_code":"73101","region":"Oklahoma"},"url":"http://mandrill.com","_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa6","ts":"2023-09-19T16:58:28.000Z","user_agent":"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; en-US; rv:1.9.1.8) Gecko/20100317 Postbox/1.1.3","msg":{"_version":"exampleaaaaaaaaaaaaaaa","subject":"This an example webhook message","email":"example.webhook@mandrillapp.com","state":"sent","_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa6","tags":["webhook-example"],"ts":"2013-04-04T21:13:19.000Z","clicks":[{"ts":"2013-04-04T21:31:51.000Z","url":"http://mandrill.com"}],"metadata":{"user_id":111},"sender":"example.sender@mandrillapp.com","opens":[{"ts":"2013-04-04T21:31:51.000Z"}]},"user_agent_parsed":{"os_company_url":"http://www.apple.com/","os_family":"OS X","os_company":"Apple Computer, Inc.","os_url":"http://www.apple.com/osx/","ua_url":"http://www.postbox-inc.com/","ua_icon":"http://cdn.mandrill.com/img/email-client-icons/postbox.png","ua_version":"1.1.3","os_name":"OS X 10.6 Snow Leopard","ua_company":"Postbox, Inc.","ua_family":"Postbox","os_icon":"http://cdn.mandrill.com/img/email-client-icons/macosx.png","ua_company_url":"http://www.postbox-inc.com/","ua_name":"Postbox 1.1.3","type":"Email Client","mobile":false}}}}"""
        ).toOpt,
        ContentType.some,
        Shared.cljSource,
        Shared.context
      ),
      RawEvent(
        Shared.api,
        Map(
          "tv" -> "com.mandrill-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mandrill/message_marked_as_spam/jsonschema/1-0-2","data":{"msg":{"_version":"exampleaaaaaaaaaaaaaaa","subject":"This an example webhook message","email":"example.webhook@mandrillapp.com","state":"sent","_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa7","tags":["webhook-example"],"ts":"2013-04-04T21:13:19.000Z","clicks":[{"ts":"2013-04-04T21:31:51.000Z","url":"http://mandrill.com"}],"metadata":{"user_id":111},"sender":"example.sender@mandrillapp.com","opens":[{"ts":"2013-04-04T21:31:51.000Z"}]},"_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa7","ts":"2023-09-19T16:58:28.000Z"}}}"""
        ).toOpt,
        ContentType.some,
        Shared.cljSource,
        Shared.context
      ),
      RawEvent(
        Shared.api,
        Map(
          "tv" -> "com.mandrill-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mandrill/recipient_unsubscribed/jsonschema/1-0-2","data":{"msg":{"_version":"exampleaaaaaaaaaaaaaaa","subject":"This an example webhook message","email":"example.webhook@mandrillapp.com","state":"sent","_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa8","tags":["webhook-example"],"ts":"2013-04-04T21:13:19.000Z","clicks":[{"ts":"2013-04-04T21:31:51.000Z","url":"http://mandrill.com"}],"metadata":{"user_id":111},"sender":"example.sender@mandrillapp.com","opens":[{"ts":"2013-04-04T21:31:51.000Z"}]},"_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa8","ts":"2023-09-19T16:58:28.000Z"}}}"""
        ).toOpt,
        ContentType.some,
        Shared.cljSource,
        Shared.context
      ),
      RawEvent(
        Shared.api,
        Map(
          "tv" -> "com.mandrill-v1",
          "e" -> "ue",
          "p" -> "srv",
          "ue_pr" -> """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.mandrill/message_rejected/jsonschema/1-0-1","data":{"msg":{"_version":"exampleaaaaaaaaaaaaaaa","subject":"This an example webhook message","email":"example.webhook@mandrillapp.com","state":"rejected","_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa9","tags":["webhook-example"],"ts":"2013-04-04T21:13:19.000Z","clicks":[],"metadata":{"user_id":111},"sender":"example.sender@mandrillapp.com","opens":[]},"_id":"exampleaaaaaaaaaaaaaaaaaaaaaaaaa9","ts":"2023-09-19T16:58:28.000Z"}}}"""
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

  def e5 = { // Spec for nine seperate events where two have incorrect event names and one does not have event as a parameter
    val bodyStr =
      "mandrill_events=%5B%7B%22event%22%3A%22sending%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%5D%2C%22clicks%22%3A%5B%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa%22%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22deferred%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%5D%2C%22clicks%22%3A%5B%5D%2C%22state%22%3A%22deferred%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa1%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%2C%22smtp_events%22%3A%5B%7B%22destination_ip%22%3A%22127.0.0.1%22%2C%22diag%22%3A%22451+4.3.5+Temporarily+unavailable%2C+try+again+later.%22%2C%22source_ip%22%3A%22127.0.0.1%22%2C%22ts%22%3A1365111111%2C%22type%22%3A%22deferred%22%2C%22size%22%3A0%7D%5D%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa1%22%2C%22ts%22%3A1415267366%7D%2C%7B%22eventsss%22%3A%22hard_bounce%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22state%22%3A%22bounced%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa2%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%2C%22bounce_description%22%3A%22bad_mailbox%22%2C%22bgtools_code%22%3A10%2C%22diag%22%3A%22smtp%3B550+5.1.1+The+email+account+that+you+tried+to+reach+does+not+exist.+Please+try+double-checking+the+recipient%27s+email+address+for+typos+or+unnecessary+spaces.%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa2%22%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22soft_bounce%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22state%22%3A%22soft-bounced%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa3%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%2C%22bounce_description%22%3A%22mailbox_full%22%2C%22bgtools_code%22%3A22%2C%22diag%22%3A%22smtp%3B552+5.2.2+Over+Quota%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa3%22%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22open%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%7B%22ts%22%3A1365111111%7D%5D%2C%22clicks%22%3A%5B%7B%22ts%22%3A1365111111%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%7D%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa4%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa4%22%2C%22ip%22%3A%22127.0.0.1%22%2C%22location%22%3A%7B%22country_short%22%3A%22US%22%2C%22country%22%3A%22United+States%22%2C%22region%22%3A%22Oklahoma%22%2C%22city%22%3A%22Oklahoma+City%22%2C%22latitude%22%3A35.4675598145%2C%22longitude%22%3A-97.5164337158%2C%22postal_code%22%3A%2273101%22%2C%22timezone%22%3A%22-05%3A00%22%7D%2C%22user_agent%22%3A%22Mozilla%5C%2F5.0+%28Macintosh%3B+U%3B+Intel+Mac+OS+X+10.6%3B+en-US%3B+rv%3A1.9.1.8%29+Gecko%5C%2F20100317+Postbox%5C%2F1.1.3%22%2C%22user_agent_parsed%22%3A%7B%22type%22%3A%22Email+Client%22%2C%22ua_family%22%3A%22Postbox%22%2C%22ua_name%22%3A%22Postbox+1.1.3%22%2C%22ua_version%22%3A%221.1.3%22%2C%22ua_url%22%3A%22http%3A%5C%2F%5C%2Fwww.postbox-inc.com%5C%2F%22%2C%22ua_company%22%3A%22Postbox%2C+Inc.%22%2C%22ua_company_url%22%3A%22http%3A%5C%2F%5C%2Fwww.postbox-inc.com%5C%2F%22%2C%22ua_icon%22%3A%22http%3A%5C%2F%5C%2Fcdn.mandrill.com%5C%2Fimg%5C%2Femail-client-icons%5C%2Fpostbox.png%22%2C%22os_family%22%3A%22OS+X%22%2C%22os_name%22%3A%22OS+X+10.6+Snow+Leopard%22%2C%22os_url%22%3A%22http%3A%5C%2F%5C%2Fwww.apple.com%5C%2Fosx%5C%2F%22%2C%22os_company%22%3A%22Apple+Computer%2C+Inc.%22%2C%22os_company_url%22%3A%22http%3A%5C%2F%5C%2Fwww.apple.com%5C%2F%22%2C%22os_icon%22%3A%22http%3A%5C%2F%5C%2Fcdn.mandrill.com%5C%2Fimg%5C%2Femail-client-icons%5C%2Fmacosx.png%22%2C%22mobile%22%3Afalse%7D%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22click%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%7B%22ts%22%3A1365111111%7D%5D%2C%22clicks%22%3A%5B%7B%22ts%22%3A1365111111%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%7D%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa5%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa5%22%2C%22ip%22%3A%22127.0.0.1%22%2C%22location%22%3A%7B%22country_short%22%3A%22US%22%2C%22country%22%3A%22United+States%22%2C%22region%22%3A%22Oklahoma%22%2C%22city%22%3A%22Oklahoma+City%22%2C%22latitude%22%3A35.4675598145%2C%22longitude%22%3A-97.5164337158%2C%22postal_code%22%3A%2273101%22%2C%22timezone%22%3A%22-05%3A00%22%7D%2C%22user_agent%22%3A%22Mozilla%5C%2F5.0+%28Macintosh%3B+U%3B+Intel+Mac+OS+X+10.6%3B+en-US%3B+rv%3A1.9.1.8%29+Gecko%5C%2F20100317+Postbox%5C%2F1.1.3%22%2C%22user_agent_parsed%22%3A%7B%22type%22%3A%22Email+Client%22%2C%22ua_family%22%3A%22Postbox%22%2C%22ua_name%22%3A%22Postbox+1.1.3%22%2C%22ua_version%22%3A%221.1.3%22%2C%22ua_url%22%3A%22http%3A%5C%2F%5C%2Fwww.postbox-inc.com%5C%2F%22%2C%22ua_company%22%3A%22Postbox%2C+Inc.%22%2C%22ua_company_url%22%3A%22http%3A%5C%2F%5C%2Fwww.postbox-inc.com%5C%2F%22%2C%22ua_icon%22%3A%22http%3A%5C%2F%5C%2Fcdn.mandrill.com%5C%2Fimg%5C%2Femail-client-icons%5C%2Fpostbox.png%22%2C%22os_family%22%3A%22OS+X%22%2C%22os_name%22%3A%22OS+X+10.6+Snow+Leopard%22%2C%22os_url%22%3A%22http%3A%5C%2F%5C%2Fwww.apple.com%5C%2Fosx%5C%2F%22%2C%22os_company%22%3A%22Apple+Computer%2C+Inc.%22%2C%22os_company_url%22%3A%22http%3A%5C%2F%5C%2Fwww.apple.com%5C%2F%22%2C%22os_icon%22%3A%22http%3A%5C%2F%5C%2Fcdn.mandrill.com%5C%2Fimg%5C%2Femail-client-icons%5C%2Fmacosx.png%22%2C%22mobile%22%3Afalse%7D%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22spam%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%7B%22ts%22%3A1365111111%7D%5D%2C%22clicks%22%3A%5B%7B%22ts%22%3A1365111111%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%7D%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa6%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa6%22%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22unsub%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%7B%22ts%22%3A1365111111%7D%5D%2C%22clicks%22%3A%5B%7B%22ts%22%3A1365111111%2C%22url%22%3A%22http%3A%5C%2F%5C%2Fmandrill.com%22%7D%5D%2C%22state%22%3A%22sent%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa7%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa7%22%2C%22ts%22%3A1415267366%7D%2C%7B%22event%22%3A%22reject%22%2C%22msg%22%3A%7B%22ts%22%3A1365109999%2C%22subject%22%3A%22This+an+example+webhook+message%22%2C%22email%22%3A%22example.webhook%40mandrillapp.com%22%2C%22sender%22%3A%22example.sender%40mandrillapp.com%22%2C%22tags%22%3A%5B%22webhook-example%22%5D%2C%22opens%22%3A%5B%5D%2C%22clicks%22%3A%5B%5D%2C%22state%22%3A%22rejected%22%2C%22metadata%22%3A%7B%22user_id%22%3A111%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa8%22%2C%22_version%22%3A%22exampleaaaaaaaaaaaaaaa%22%7D%2C%22_id%22%3A%22exampleaaaaaaaaaaaaaaaaaaaaaaaaa8%22%2C%22ts%22%3A1415267366%7D%5D"
    val payload = CollectorPayload(
      Shared.api,
      Nil,
      ContentType.some,
      bodyStr.some,
      Shared.cljSource,
      Shared.context
    )
    val expected = NonEmptyList.of(
      FailureDetails.AdapterFailure.SchemaMapping(
        "sending".some,
        adapterWithDefaultSchemas.EventSchemaMap,
        "no schema associated with the provided type parameter at index 0"
      ),
      FailureDetails.AdapterFailure.SchemaMapping(
        "deferred".some,
        adapterWithDefaultSchemas.EventSchemaMap,
        "no schema associated with the provided type parameter at index 1"
      ),
      FailureDetails.AdapterFailure.SchemaMapping(
        None,
        adapterWithDefaultSchemas.EventSchemaMap,
        "cannot determine event type: type parameter not provided at index 2"
      )
    )
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beInvalid(expected))
  }

  def e6 = {
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

  def e7 = {
    val body = "mandrill_events=%5B%7B%22event%22%3A%20%22subscribe%22%7D%5D"
    val payload =
      CollectorPayload(Shared.api, Nil, None, body.some, Shared.cljSource, Shared.context)
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.AdapterFailure.InputData(
              "contentType",
              None,
              "no content type: expected application/x-www-form-urlencoded"
            )
          )
        )
      )
  }

  def e8 = {
    val body = "mandrill_events=%5B%7B%22event%22%3A%20%22subscribe%22%7D%5D"
    val ct = "application/x-www-form-urlencoded; charset=utf-8".some
    val payload =
      CollectorPayload(Shared.api, Nil, ct, body.some, Shared.cljSource, Shared.context)
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.AdapterFailure
              .InputData("contentType", ct, "expected application/x-www-form-urlencoded")
          )
        )
      )
  }
}
