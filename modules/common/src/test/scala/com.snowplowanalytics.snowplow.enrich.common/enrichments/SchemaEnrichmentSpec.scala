/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import org.specs2.Specification
import org.specs2.matcher.DataTables

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers

class SchemaEnrichmentSpec extends Specification with DataTables {

  val signupFormSubmitted =
    """{"schema":"iglu:com.snowplowanalytics.snowplow-website/signup_form_submitted/jsonschema/1-0-0","data":{"name":"Χαριτίνη NEW Unicode test","email":"alex+test@snowplowanalytics.com","company":"SP","eventsPerMonth":"< 1 million","serviceType":"unsure"}}"""

  def is = s2"""
  extracting SchemaKeys from valid events should work $e1
  invalid events should fail when extracting SchemaKeys $e2
  """

  def e1 =
    "SPEC NAME" || "EVENT" | "UNSTRUCT EVENT" | "EXPECTED SCHEMA" |
      "page view" !! event("page_view") ! None ! SchemaKey(
        "com.snowplowanalytics.snowplow",
        "page_view",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ) |
      "ping ping" !! event("page_ping") ! None ! SchemaKey(
        "com.snowplowanalytics.snowplow",
        "page_ping",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ) |
      "transaction" !! event("transaction") ! None ! SchemaKey(
        "com.snowplowanalytics.snowplow",
        "transaction",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ) |
      "transaction item" !! event("transaction_item") ! None ! SchemaKey(
        "com.snowplowanalytics.snowplow",
        "transaction_item",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ) |
      "struct event" !! event("struct") ! None ! SchemaKey(
        "com.google.analytics",
        "event",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ) |
      "unstruct event" !! unstructEvent(signupFormSubmitted) ! Some(
        SpecHelpers.jsonStringToSDJ(signupFormSubmitted).right.get
      ) ! SchemaKey(
        "com.snowplowanalytics.snowplow-website",
        "signup_form_submitted",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ) |> { (_, event, unstruct, expected) =>
      val schema = SchemaEnrichment.extractSchema(event, unstruct)
      schema must beRight(Some(expected))
    }

  def e2 =
    "SPEC NAME" || "EVENT" |
      "unknown event" !! event("unknown") |
      "missing event" !! event(null) |> { (_, event) =>
      SchemaEnrichment.extractSchema(event, None) must beLeft
    }

  def event(eventType: String) = {
    val event: EnrichedEvent = new EnrichedEvent()
    event.setEvent(eventType)
    event
  }

  def unstructEvent(unstruct: String) = {
    val event: EnrichedEvent = new EnrichedEvent()
    event.setEvent("unstruct")
    event.setUnstruct_event(unstruct)
    event
  }
}
