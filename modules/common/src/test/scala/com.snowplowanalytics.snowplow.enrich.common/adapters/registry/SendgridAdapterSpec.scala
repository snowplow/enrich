/*
 * Copyright (c) 2015-present Snowplow Analytics Ltd.
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
import org.joda.time.DateTime
import org.specs2.matcher.ValidatedMatchers
import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class SendgridAdapterSpec extends Specification with ValidatedMatchers with CatsEffect {
  object Shared {
    val api = CollectorPayload.Api("com.sendgrid", "v3")
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

  val ContentType = "application/json"
  val adapterWithDefaultSchemas = SendgridAdapter(schemas = sendgridSchemas)

  // this could do with going somewhere else
  val samplePostPayload =
    """
[
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"processed",
      "category":"cat facts",
      "sg_event_id":"sZROwMGMagFgnOEmSdvhig==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"deferred",
      "category":"cat facts",
      "sg_event_id":"jWmZXTZbtHTV2-S47asrww==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "response":"400 try again later",
      "attempt":"5"
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"delivered",
      "category":"cat facts",
      "sg_event_id":"cikAODhD-ffTphZ7xixsRw==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "response":"250 OK"
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"open",
      "category":"cat facts",
      "sg_event_id":"VGRrZCh-qMkOaAmuxcFujA==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "useragent":"Mozilla/4.0 (compatible; MSIE 6.1; Windows XP; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
      "ip":"255.255.255.255"
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"click",
      "category":"cat facts",
      "sg_event_id":"QjGWYpcksoD31aVQAONfAg==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "useragent":"Mozilla/4.0 (compatible; MSIE 6.1; Windows XP; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
      "ip":"255.255.255.255",
      "url":"http://www.sendgrid.com/"
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"bounce",
      "category":"cat facts",
      "sg_event_id":"PQmsSRnaTMVde4mu4TUgTQ==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "reason":"500 unknown recipient",
      "status":"5.0.0"
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"dropped",
      "category":"cat facts",
      "sg_event_id":"BP0-vnv2BjDPzwaldo-XVg==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "reason":"Bounced Address",
      "status":"5.0.0"
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"spamreport",
      "category":"cat facts",
      "sg_event_id":"ApWZolLiPe04wm5jAhFifA==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"unsubscribe",
      "category":"cat facts",
      "sg_event_id":"HoBsy5C1Tcoc1dJNsy5SfA==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"group_unsubscribe",
      "category":"cat facts",
      "sg_event_id":"hew55AFBIgLbd33pcviQTQ==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "useragent":"Mozilla/4.0 (compatible; MSIE 6.1; Windows XP; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
      "ip":"255.255.255.255",
      "url":"http://www.sendgrid.com/",
      "asm_group_id":10
   },
   {
      "email":"example@test.com",
      "timestamp":1446549615,
      "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
      "event":"group_resubscribe",
      "category":"cat facts",
      "sg_event_id":"TDlqEy7cUfKLVMY3EAVCag==",
      "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
      "marketing_campaign_id":12345,
      "marketing_campaign_name":"campaign name",
      "marketing_campaign_version":"B",
      "marketing_campaign_split_id":13471,
      "useragent":"Mozilla/4.0 (compatible; MSIE 6.1; Windows XP; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
      "ip":"255.255.255.255",
      "url":"http://www.sendgrid.com/",
      "asm_group_id":10
   }
]
    """

  "toRawEvents" should {

    val payload =
      CollectorPayload(
        Shared.api,
        Nil,
        ContentType.some,
        samplePostPayload.some,
        Shared.cljSource,
        Shared.context
      )
    val actual =
      adapterWithDefaultSchemas.toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)

    "return the correct number of events" in {
      actual.map { output =>
        output must beValid
        val items = output.toList.head.toList
        items must have size 11
      }
    }

    "have the correct api endpoint for each element" in {
      actual.map { output =>
        output must beValid
        val items = output.toList.head.toList
        val siz = items.count(itm => itm.api == Shared.api)
        siz must beEqualTo(items.size)
      }
    }

    "have the correct content type for each element" in {
      actual.map { output =>
        output must beValid
        val items = output.toList.head.toList
        val siz = items.count(itm => itm.contentType.get == ContentType)
        siz must beEqualTo(items.toList.size)
      }
    }

    "have the correct source for each element" in {
      actual.map { output =>
        output must beValid
        val items = output.toList.head.toList
        val siz = items.count(itm => itm.source == Shared.cljSource)
        siz must beEqualTo(items.toList.size)
      }
    }

    "have the correct context for each element" in {
      actual.map { output =>
        output must beValid
        val items = output.toList.head.toList
        val siz = items.count(itm => itm.context == Shared.context)
        siz must beEqualTo(items.toList.size)
      }
    }

    "reject empty bodies" in {
      val invalidpayload =
        CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
      adapterWithDefaultSchemas
        .toRawEvents(invalidpayload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .map(_ must beInvalid)
    }

    "reject empty content type" in {
      val invalidpayload =
        CollectorPayload(
          Shared.api,
          Nil,
          None,
          samplePostPayload.some,
          Shared.cljSource,
          Shared.context
        )
      adapterWithDefaultSchemas
        .toRawEvents(invalidpayload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .map(_ must beInvalid)
    }

    "reject unexpected content type" in {
      val invalidpayload =
        CollectorPayload(
          Shared.api,
          Nil,
          "invalidtype/invalid".some,
          samplePostPayload.some,
          Shared.cljSource,
          Shared.context
        )
      adapterWithDefaultSchemas
        .toRawEvents(invalidpayload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .map(_ must beInvalid)
    }

    "accept content types with explicit charsets" in {
      val payload =
        CollectorPayload(
          Shared.api,
          Nil,
          "application/json; charset=utf-8".some,
          samplePostPayload.some,
          Shared.cljSource,
          Shared.context
        )
      adapterWithDefaultSchemas
        .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .map(_ must beValid)
    }

    "reject unsupported event types" in {

      val invalidEventTypeJson =
        """
            [
               {
                 "email": "example@test.com",
                 "timestamp": 1446549615,
                 "smtp-id": "\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
                 "event": "moon landing",
                 "category": "cat facts",
                 "sg_event_id": "sZROwMGMagFgnOEmSdvhig==",
                 "sg_message_id": "14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0"
                }
            ]"""

      val invalidpayload =
        CollectorPayload(
          Shared.api,
          Nil,
          ContentType.some,
          invalidEventTypeJson.some,
          Shared.cljSource,
          Shared.context
        )

      adapterWithDefaultSchemas
        .toRawEvents(invalidpayload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .map(_ must beInvalid)
    }

    "reject invalid/unparsable json" in {
      val unparsableJson = """[ """
      adapterWithDefaultSchemas
        .toRawEvents(
          CollectorPayload(
            Shared.api,
            Nil,
            ContentType.some,
            unparsableJson.some,
            Shared.cljSource,
            Shared.context
          ),
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth
        )
        .map(_ must beInvalid)
    }

    "reject valid json in incorrect format" in {
      val incorrectlyFormattedJson = """[ ]"""
      adapterWithDefaultSchemas
        .toRawEvents(
          CollectorPayload(
            Shared.api,
            Nil,
            ContentType.some,
            incorrectlyFormattedJson.some,
            Shared.cljSource,
            Shared.context
          ),
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth
        )
        .map(_ must beInvalid)
    }

    "reject a payload with a some valid, some invalid events" in {
      val missingEventType =
        """
      [
         {
           "email": "example@test.com",
           "timestamp": 1446549615,
           "smtp-id": "\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
           "event": "processed",
           "category": "cat facts",
           "sg_event_id": "sZROwMGMagFgnOEmSdvhig==",
           "sg_message_id": "14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0"
          },
          {
           "email": "example@test.com",
           "timestamp": 1446549615,
           "smtp-id": "\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
           "category": "cat facts",
           "sg_event_id": "sZROwMGMagFgnOEmSdvhig==",
           "sg_message_id": "14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0"
          }
      ]"""

      val payload =
        CollectorPayload(
          Shared.api,
          Nil,
          ContentType.some,
          missingEventType.some,
          Shared.cljSource,
          Shared.context
        )
      adapterWithDefaultSchemas
        .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .map(
          _ must beInvalid(
            NonEmptyList.one(
              FailureDetails.AdapterFailure.SchemaMapping(
                None,
                adapterWithDefaultSchemas.EventSchemaMap,
                "cannot determine event type: type parameter not provided at index 1"
              )
            )
          )
        )
    }

    "return correct json for sample event, including stripping out event keypair and fixing timestamp" in {

      val inputJson =
        """
      [
         {
           "email": "example@test.com",
           "timestamp": 1446549615,
           "smtp-id": "\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
           "event": "processed",
           "category": "cat facts",
           "sg_event_id": "sZROwMGMagFgnOEmSdvhig==",
           "sg_message_id": "14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
           "marketing_campaign_id":12345,
           "marketing_campaign_name":"campaign name",
           "marketing_campaign_version":"B",
           "marketing_campaign_split_id":13471
          }
      ]"""

      val payload =
        CollectorPayload(
          Shared.api,
          Nil,
          ContentType.some,
          inputJson.some,
          Shared.cljSource,
          Shared.context
        )

      val expectedJson =
        """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.sendgrid/processed/jsonschema/3-0-0","data":{"timestamp":"2015-11-03T11:20:15.000Z","email":"example@test.com","marketing_campaign_name":"campaign name","sg_event_id":"sZROwMGMagFgnOEmSdvhig==","smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e","marketing_campaign_version":"B","marketing_campaign_id":12345,"marketing_campaign_split_id":13471,"category":"cat facts","sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0"}}}"""

      adapterWithDefaultSchemas
        .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .map(
          _ must beValid(
            NonEmptyList.one(
              RawEvent(
                Shared.api,
                Map(
                  "tv" -> "com.sendgrid-v3",
                  "e" -> "ue",
                  "p" -> "srv",
                  "ue_pr" -> expectedJson // NB this includes removing the "event" keypair as redundant
                ).toOpt,
                ContentType.some,
                Shared.cljSource,
                Shared.context
              )
            )
          )
        )
    }

    "filter events if they are exact duplicates" in {
      val inputJson =
        """
        [
          {
            "email":"example@test.com",
            "timestamp":1446549615,
            "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
            "event":"processed",
            "category":"cat facts",
            "sg_event_id":"sZROwMGMagFgnOEmSdvhig==",
            "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
            "marketing_campaign_id":12345,
            "marketing_campaign_name":"campaign name",
            "marketing_campaign_version":"B",
            "marketing_campaign_split_id":13471
          },
          {
            "email":"example@test.com",
            "timestamp":1446549615,
            "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
            "event":"processed",
            "category":"cat facts",
            "sg_event_id":"sZROwMGMagFgnOEmSdvhig==",
            "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
            "marketing_campaign_id":12345,
            "marketing_campaign_name":"campaign name",
            "marketing_campaign_version":"B",
            "marketing_campaign_split_id":13471
          }
        ]
        """

      val payload =
        CollectorPayload(
          Shared.api,
          Nil,
          ContentType.some,
          inputJson.some,
          Shared.cljSource,
          Shared.context
        )

      adapterWithDefaultSchemas
        .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .map(_ must beValid.like {
          case nel: NonEmptyList[RawEvent] =>
            nel.toList must have size 1
        })
    }

    "uses custom schema and tracker overrides from configuration" in {
      val inputJson =
        """
        [
        {
           "email":"example@test.com",
           "timestamp":1446549615,
           "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
           "event":"processed",
           "category":"cat facts",
           "sg_event_id":"sZROwMGMagFgnOEmSdvhig==",
           "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
           "marketing_campaign_id":12345,
           "marketing_campaign_name":"campaign name",
           "marketing_campaign_version":"B",
           "marketing_campaign_split_id":13471
        },
        {
           "email":"example@test.com",
           "timestamp":1446549615,
           "smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e",
           "event":"deferred",
           "category":"cat facts",
           "sg_event_id":"jWmZXTZbtHTV2-S47asrww==",
           "sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0",
           "marketing_campaign_id":12345,
           "marketing_campaign_name":"campaign name",
           "marketing_campaign_version":"B",
           "marketing_campaign_split_id":13471,
           "response":"400 try again later",
           "attempt":"5"
        }
        ]
        """

      val payload =
        CollectorPayload(
          Shared.api,
          Nil,
          ContentType.some,
          inputJson.some,
          Shared.cljSource,
          Shared.context
        )

      val adapter = SendgridAdapter(
        schemas = sendgridSchemas.copy(
          processed = "iglu:com.custom/processed/jsonschema/1-2-3"
        )
      )

      val expectedJsonProcessed =
        """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.custom/processed/jsonschema/1-2-3","data":{"timestamp":"2015-11-03T11:20:15.000Z","email":"example@test.com","marketing_campaign_name":"campaign name","sg_event_id":"sZROwMGMagFgnOEmSdvhig==","smtp-id":"\u003c14c5d75ce93.dfd.64b469@ismtpd-555\u003e","marketing_campaign_version":"B","marketing_campaign_id":12345,"marketing_campaign_split_id":13471,"category":"cat facts","sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0"}}}"""
      val expectedJsonDeferred =
        """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.sendgrid/deferred/jsonschema/3-0-0","data":{"timestamp":"2015-11-03T11:20:15.000Z","email":"example@test.com","marketing_campaign_name":"campaign name","sg_event_id":"jWmZXTZbtHTV2-S47asrww==","smtp-id":"<14c5d75ce93.dfd.64b469@ismtpd-555>","marketing_campaign_version":"B","response":"400 try again later","marketing_campaign_id":12345,"marketing_campaign_split_id":13471,"category":"cat facts","attempt":"5","sg_message_id":"14c5d75ce93.dfd.64b469.filter0001.16648.5515E0B88.0"}}}"""

      adapter
        .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .map(
          _ must beValid(
            NonEmptyList.of(
              RawEvent(
                Shared.api,
                Map(
                  "tv" -> "com.sendgrid-v3",
                  "e" -> "ue",
                  "p" -> "srv",
                  "ue_pr" -> expectedJsonProcessed // NB this includes removing the "event" keypair as redundant
                ).toOpt,
                ContentType.some,
                Shared.cljSource,
                Shared.context
              ),
              RawEvent(
                Shared.api,
                Map(
                  "tv" -> "com.sendgrid-v3",
                  "e" -> "ue",
                  "p" -> "srv",
                  "ue_pr" -> expectedJsonDeferred // NB this includes removing the "event" keypair as redundant
                ).toOpt,
                ContentType.some,
                Shared.cljSource,
                Shared.context
              )
            )
          )
        )
    }
  }
}
