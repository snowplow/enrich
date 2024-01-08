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
