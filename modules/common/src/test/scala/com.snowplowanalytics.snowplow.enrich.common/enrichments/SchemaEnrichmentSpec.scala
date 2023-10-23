/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
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
