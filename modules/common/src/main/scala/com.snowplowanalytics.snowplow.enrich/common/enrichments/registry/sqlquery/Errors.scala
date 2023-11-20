/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery

sealed trait SqlQueryEnrichmentError extends Throwable {
  val message: String
  override def toString = message
  override def getMessage = "SQL Query enrichment: " ++ message
}
final case class ValueNotFoundException(message: String) extends SqlQueryEnrichmentError
final case class JsonPathException(message: String) extends SqlQueryEnrichmentError
final case class InvalidStateException(message: String) extends SqlQueryEnrichmentError
final case class InvalidConfiguration(message: String) extends SqlQueryEnrichmentError
final case class InvalidDbResponse(message: String) extends SqlQueryEnrichmentError
final case class InvalidInput(message: String) extends SqlQueryEnrichmentError
