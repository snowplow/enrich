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
package com.snowplowanalytics.snowplow.enrich.core

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Base64, UUID}

import scala.concurrent.duration._

import org.apache.http.NameValuePair
import org.apache.http.message.BasicNameValuePair

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import cats.implicits._
import cats.data.NonEmptyList

import cats.effect.IO
import cats.effect.kernel.Unique

import fs2.{Chunk, Stream}

import io.circe.Json
import io.circe.parser
import io.circe.literal._

import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.FailureDetails._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.Contexts

import com.snowplowanalytics.snowplow.streams.TokenedEvents

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EventEnrichments

object EventUtils {

  case class GoodBatch(value: List[CollectorPayload])

  def mkGoodStream(batches: (GoodBatch, Unique.Token)*): Stream[IO, TokenedEvents] =
    Stream.emits(batches).map {
      case (batch, ack) =>
        val serialized = Chunk.from(batch.value).map { cp =>
          ByteBuffer.wrap(cp.toRaw)
        }
        TokenedEvents(serialized, ack)
    }

  case class BadBatch(value: Chunk[String])

  def mkBadStream(batches: (BadBatch, Unique.Token)*): Stream[IO, TokenedEvents] =
    Stream.emits(batches).map {
      case (batch, ack) =>
        val serialized = batch.value.map(s => ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        TokenedEvents(serialized, ack)
    }

  def pageView(
    eventId: UUID,
    appID: String = appID,
    tiQuantity: Option[Int] = None
  ): CollectorPayload = {
    val api: CollectorPayload.Api =
      CollectorPayload.Api(vendor, vendorVersion)
    val source: CollectorPayload.Source =
      CollectorPayload.Source(collectorName, collectorEncoding, Some(collectorHostname))
    val context: CollectorPayload.Context =
      CollectorPayload.Context(new DateTime(collectorTstamp.toEpochMilli), Some(ip), None, None, List(), None)
    val queryString: List[NameValuePair] = List(
      new BasicNameValuePair("eid", eventId.toString),
      new BasicNameValuePair("e", pageViewType.short),
      new BasicNameValuePair("aid", appID)
    )
    val queryStringFull = tiQuantity.fold(queryString)(q => new BasicNameValuePair("ti_qu", q.toString) :: queryString)
    CollectorPayload(api, queryStringFull, None, None, source, context)
  }

  def expectedIdentityContext(eventId: UUID, etlTstamp: Instant): SelfDescribingData[Json] =
    SelfDescribingData(
      SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/identity/jsonschema/1-0-0").toOption.get,
      json"""
      {
        "snowplowId": $eventId,
        "createdAt": $etlTstamp,
        "merged": []
      }
      """
    )

  def expectedPageView(eventId: UUID): Event =
    Event
      .minimal(
        id = eventId,
        collectorTstamp = collectorTstamp,
        vCollector = EventUtils.collectorName,
        vEtl = vEtl
      )
      .copy(
        app_id = Some(appID),
        etl_tstamp = Some(etlTstamp),
        event = Some(pageViewType.full),
        user_ipaddress = Some(EventUtils.ip),
        derived_tstamp = Some(collectorTstamp),
        event_vendor = Some(vendor),
        event_name = Some(pageViewType.full),
        event_format = Some("jsonschema"),
        event_version = Some("1-0-0"),
        derived_contexts = Contexts(List(expectedIdentityContext(eventId, etlTstamp)))
      )

  def expectedPageViewMetadata = Metadata.MetadataEvent(None, Some(appID), None, None, None)
  def expectedPageViewMetadataEntities =
    Set(
      SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/identity/jsonschema/1-0-0").toOption.get
    )

  def invalidAddToCart(eventId: UUID): CollectorPayload = {
    val queryString: List[NameValuePair] = List(
      new BasicNameValuePair("eid", eventId.toString),
      new BasicNameValuePair("e", unstructType.short),
      new BasicNameValuePair("ue_px", uePxInvalidAddToCart)
    )
    pageView(eventId).copy(querystring = queryString)
  }

  def expectedFailedSV(eventId: UUID) =
    Event
      .minimal(
        id = eventId,
        collectorTstamp = collectorTstamp,
        vCollector = EventUtils.collectorName,
        vEtl = s"${MockEnvironment.appInfo.name}-${MockEnvironment.appInfo.version}"
      )
      .copy(
        etl_tstamp = Some(etlTstamp),
        event = Some(unstructType.full),
        user_ipaddress = Some(EventUtils.ip),
        derived_tstamp = Some(collectorTstamp),
        derived_contexts = Contexts(List(expectedIdentityContext(eventId, etlTstamp), failureAddToCart))
      )

  def expectedFailedJavascript(eventId: UUID) =
    expectedPageView(eventId)
      .copy(
        derived_contexts = Contexts(List(expectedIdentityContext(eventId, etlTstamp), failureJavascript))
      )

  def expectedBadSV(eventId: UUID) = {
    val processor = Processor(MockEnvironment.appInfo.name, MockEnvironment.appInfo.version)
    val failureJson = parser
      .parse(s"""
    {
      "timestamp": "$etlTstamp",
      "messages": [
      {
        "schemaKey": "iglu:com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0",
        "error": {
          "error": "ValidationError",
          "dataReports": [
          {
            "message": "$$.sku: is missing but it is required",
            "path": "$$",
            "keyword": "required",
            "targets": [
              "sku"
            ]
          },
          {
            "message": "$$.skuu: is not defined in the schema and the schema does not allow additional properties",
            "path": "$$",
            "keyword": "additionalProperties",
            "targets": [
              "skuu"
            ]
          }
          ]
        }
      }
      ]
    }""")
      .toOption
      .get
    val failure = failureJson
      .as[Failure.SchemaViolations]
      .toOption
      .get

    val enriched = new EnrichedEvent
    enriched.etl_tstamp = EventEnrichments.toTimestamp(new DateTime(etlTstamp.toEpochMilli))
    enriched.collector_tstamp = EventEnrichments.toTimestamp(new DateTime(collectorTstamp.toEpochMilli))
    enriched.event = unstructType.full
    enriched.event_id = eventId.toString
    enriched.v_collector = collectorName
    enriched.v_etl = vEtl
    enriched.user_ipaddress = ip
    enriched.derived_contexts = List(failureAddToCart)
    enriched.derived_tstamp = enriched.collector_tstamp

    val raw = Payload.RawEvent(
      vendor = vendor,
      version = vendorVersion,
      parameters = List(
        NVP("eid", Some(eventId.toString)),
        NVP("e", Some(unstructType.short)),
        NVP("ue_px", Some(uePxInvalidAddToCart))
      ),
      contentType = None,
      loaderName = collectorName,
      encoding = collectorEncoding,
      hostname = Some(collectorHostname),
      timestamp = Some(new DateTime(collectorTstamp.toEpochMilli, DateTimeZone.UTC)),
      ipAddress = Some(ip),
      useragent = None,
      refererUri = None,
      headers = Nil,
      userId = None
    )
    val payload = Payload.EnrichmentPayload(
      EnrichedEvent.toPartiallyEnrichedEvent(enriched),
      raw
    )
    BadRow.SchemaViolations(processor, failure, payload)
  }

  def expectedBadCPF(rawPayload: String) = {
    val processor = Processor(MockEnvironment.appInfo.name, MockEnvironment.appInfo.version)
    val failure = Failure.CPFormatViolation(
      etlTstamp,
      "thrift",
      FailureDetails.CPFormatViolationMessage.Fallback("error deserializing raw event: Unrecognized type 110")
    )
    val payload = Payload.RawPayload(rawPayload)
    BadRow.CPFormatViolation(processor, failure, payload)
  }

  def expectedBadJavascript(eventId: UUID) = {
    val processor = Processor(MockEnvironment.appInfo.name, MockEnvironment.appInfo.version)
    val failure = Failure.EnrichmentFailures(
      etlTstamp,
      messages = NonEmptyList.one(
        FailureDetails.EnrichmentFailure(
          Some(
            EnrichmentInformation(
              SchemaKey("com.snowplowanalytics.snowplow", "javascript_script_config", "jsonschema", SchemaVer.Full(1, 0, 0)),
              "Javascript enrichment"
            )
          ),
          EnrichmentFailureMessage.Simple(
            "Error compiling JavaScript function: [<eval>:3:19 Expected an operand but found ;\n          return [ ;\n                   ^ in <eval> at line number 3 at column number 19]"
          )
        )
      )
    )

    val enriched = new EnrichedEvent
    enriched.app_id = appID
    enriched.etl_tstamp = EventEnrichments.toTimestamp(new DateTime(etlTstamp.toEpochMilli))
    enriched.collector_tstamp = EventEnrichments.toTimestamp(new DateTime(collectorTstamp.toEpochMilli))
    enriched.event = pageViewType.full
    enriched.event_id = eventId.toString
    enriched.v_collector = collectorName
    enriched.v_etl = vEtl
    enriched.user_ipaddress = ip
    enriched.derived_contexts = List(failureJavascript)
    enriched.derived_tstamp = enriched.collector_tstamp
    enriched.event_vendor = vendor
    enriched.event_name = pageViewType.full
    enriched.event_format = "jsonschema"
    enriched.event_version = "1-0-0"

    val raw = Payload.RawEvent(
      vendor = vendor,
      version = vendorVersion,
      parameters = List(
        NVP("eid", Some(eventId.toString)),
        NVP("e", Some(pageViewType.short)),
        NVP("aid", Some(appID))
      ),
      contentType = None,
      loaderName = collectorName,
      encoding = collectorEncoding,
      hostname = Some(collectorHostname),
      timestamp = Some(new DateTime(collectorTstamp.toEpochMilli, DateTimeZone.UTC)),
      ipAddress = Some(ip),
      useragent = None,
      refererUri = None,
      headers = Nil,
      userId = None
    )
    val payload = Payload.EnrichmentPayload(
      EnrichedEvent.toPartiallyEnrichedEvent(enriched),
      raw
    )
    BadRow.EnrichmentFailures(processor, failure, payload)
  }

  val collectorTstamp = Instant.parse("2025-04-30T10:00:00.000Z")
  val etlLatency = 5.minutes
  val etlTstamp = collectorTstamp.plus(etlLatency.toMinutes, ChronoUnit.MINUTES)
  val vendor = "com.snowplowanalytics.snowplow"
  val vendorVersion = "tp2"
  val collectorName = "test-collector"
  val collectorEncoding = "UTF-8"
  val collectorHostname = "collector.snplow.net"
  val vEtl = s"${MockEnvironment.appInfo.name}-${MockEnvironment.appInfo.version}"
  val ip = "175.16.199.0"
  val appID = "test_app"

  case class EventType(short: String, full: String)
  val pageViewType = EventType("pv", "page_view")
  val unstructType = EventType("ue", "unstruct")

  val uePrInvalidAddToCart = """
  {
    "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
    "data": {
      "schema": "iglu:com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0",
      "data": {
        "skuu": "pedals",
        "quantity": 2
      }
    }
  }""".filterNot(_.isWhitespace)
  val uePxInvalidAddToCart = new String(Base64.getEncoder().encode(uePrInvalidAddToCart.getBytes(StandardCharsets.UTF_8)))

  val failureAddToCart: SelfDescribingData[Json] = SelfDescribingData(
    SchemaKey("com.snowplowanalytics.snowplow", "failure", "jsonschema", SchemaVer.Full(1, 0, 0)),
    parser
      .parse(s"""
    {
      "failureType" : "ValidationError",
      "errors" : [
        {
          "message" : "$$.sku: is missing but it is required",
          "source" : "unstruct",
          "path" : "$$",
          "keyword" : "required",
          "targets" : [
            "sku"
          ]
        },
        {
          "message" : "$$.skuu: is not defined in the schema and the schema does not allow additional properties",
          "source" : "unstruct",
          "path" : "$$",
          "keyword" : "additionalProperties",
          "targets" : [
            "skuu"
          ]
        }
      ],
      "schema" : "iglu:com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0",
      "data" : {
        "skuu" : "pedals",
        "quantity" : 2
      },
      "timestamp" : "$etlTstamp",
      "componentName" : "${MockEnvironment.appInfo.name}",
      "componentVersion" : "${MockEnvironment.appInfo.version}"
    }""")
      .toOption
      .get
  )

  val failureJavascript: SelfDescribingData[Json] = SelfDescribingData(
    SchemaKey("com.snowplowanalytics.snowplow", "failure", "jsonschema", SchemaVer.Full(1, 0, 0)),
    parser
      .parse(s"""
    {
      "failureType" : "EnrichmentError: Javascript enrichment",
      "errors" : [
        {
          "message" : "Error compiling JavaScript function: [<eval>:3:19 Expected an operand but found ;\\n          return [ ;\\n                   ^ in <eval> at line number 3 at column number 19]"
        }
      ],
      "schema" : "iglu:com.snowplowanalytics.snowplow/javascript_script_config/jsonschema/1-0-0",
      "data" : null,
      "timestamp" : "$etlTstamp",
      "componentName" : "${MockEnvironment.appInfo.name}",
      "componentVersion" : "${MockEnvironment.appInfo.version}"
    }""")
      .toOption
      .get
  )

  def addToCartSDD(quantity: Int): SelfDescribingData[Json] =
    parser
      .parse(s"""
      {
        "schema" : "iglu:com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0",
        "data": {
          "sku": "pedals",
          "quantity": $quantity
        }
      }
      """)
      .toOption
      .get
      .as[SelfDescribingData[Json]]
      .toOption
      .get
}
