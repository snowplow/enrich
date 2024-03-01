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

import io.circe.Json

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

object SchemaEnrichment {

  private object Schemas {
    private val Vendor = "com.snowplowanalytics.snowplow"
    private val Format = "jsonschema"
    private val SchemaVersion = SchemaVer.Full(1, 0, 0)
    val pageViewSchema = SchemaKey(Vendor, "page_view", Format, SchemaVersion)
    val pagePingSchema = SchemaKey(Vendor, "page_ping", Format, SchemaVersion)
    val transactionSchema = SchemaKey(Vendor, "transaction", Format, SchemaVersion)
    val transactionItemSchema = SchemaKey(Vendor, "transaction_item", Format, SchemaVersion)
    val structSchema = SchemaKey("com.google.analytics", "event", Format, SchemaVersion)
  }

  /**
   * Returns an Option so that if there has already been a failure to validate the unstructured event,
   * it returns `None` instead of creating an `EnrichmentFailure` for this enrichment,
   * thus making it 2 bad rows for the same problem.
   */
  def extractSchema(
    event: EnrichedEvent,
    unstructEvent: Option[SelfDescribingData[Json]]
  ): Either[FailureDetails.EnrichmentFailure, Option[SchemaKey]] =
    event.event match {
      case "page_view" => Right(Some(Schemas.pageViewSchema))
      case "page_ping" => Right(Some(Schemas.pagePingSchema))
      case "struct" => Right(Some(Schemas.structSchema))
      case "transaction" => Right(Some(Schemas.transactionSchema))
      case "transaction_item" => Right(Some(Schemas.transactionItemSchema))
      case "unstruct" =>
        unstructEvent match {
          case Some(sdj) => Right(Some(sdj.schema))
          case _ => Right(None)
        }
      case eventType =>
        val f = FailureDetails.EnrichmentFailureMessage.InputData(
          "event",
          Option(eventType),
          s"""trying to extract the schema of the enriched event but event type [$eventType] doesn't match
          any of page_view, page_ping, struct, transaction, transaction_item and unstruct
          """
        )
        Left(FailureDetails.EnrichmentFailure(None, f))
    }
}
