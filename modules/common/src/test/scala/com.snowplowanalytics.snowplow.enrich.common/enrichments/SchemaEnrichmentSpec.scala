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
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import org.specs2.Specification
import org.specs2.matcher.DataTables

import io.circe.Json
import io.circe.literal._

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

class SchemaEnrichmentSpec extends Specification with DataTables {

  val signupFormSubmitted = SelfDescribingData(
    SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow-website/signup_form_submitted/jsonschema/1-0-0").toOption.get,
    json"""{
      "name":"Χαριτίνη NEW Unicode test",
      "email":"alex+test@snowplowanalytics.com",
      "company":"SP",
      "eventsPerMonth":"< 1 million",
      "serviceType":"unsure"
    }"""
  )

  def is = s2"""
  extracting SchemaKeys from valid events should work $e1
  invalid events should fail when extracting SchemaKeys $e2
  """

  def e1 =
    "SPEC NAME" || "EVENT" | "EXPECTED SCHEMA" |
      "page view" !! event("page_view") ! SchemaKey(
        "com.snowplowanalytics.snowplow",
        "page_view",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ) |
      "ping ping" !! event("page_ping") ! SchemaKey(
        "com.snowplowanalytics.snowplow",
        "page_ping",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ) |
      "transaction" !! event("transaction") ! SchemaKey(
        "com.snowplowanalytics.snowplow",
        "transaction",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ) |
      "transaction item" !! event("transaction_item") ! SchemaKey(
        "com.snowplowanalytics.snowplow",
        "transaction_item",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ) |
      "struct event" !! event("struct") ! SchemaKey(
        "com.google.analytics",
        "event",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ) |
      "unstruct event" !! unstructEvent(signupFormSubmitted) ! SchemaKey(
        "com.snowplowanalytics.snowplow-website",
        "signup_form_submitted",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ) |> { (_, event, expected) =>
      val schema = SchemaEnrichment.extractSchema(event)
      schema must beRight(Some(expected))
    }

  def e2 =
    "SPEC NAME" || "EVENT" |
      "unknown event" !! event("unknown") |
      "missing event" !! event(null) |> { (_, event) =>
      SchemaEnrichment.extractSchema(event) must beLeft
    }

  def event(eventType: String) = {
    val event: EnrichedEvent = new EnrichedEvent()
    event.setEvent(eventType)
    event
  }

  def unstructEvent(unstruct: SelfDescribingData[Json]) = {
    val event: EnrichedEvent = new EnrichedEvent()
    event.setEvent("unstruct")
    event.unstruct_event = Some(unstruct)
    event
  }
}
