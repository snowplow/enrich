/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest

final case class ValueNotFoundException(message: String) extends Throwable {
  override def getMessage: String = "API Request enrichment:" ++ toString
  override def toString = s"Value not found $message"
}

final case class JsonPathException(message: String) extends Throwable {
  override def getMessage: String = "API Request enrichment:" ++ toString
  override def toString = s"JSONPath error $message"
}

final case class InvalidStateException(message: String) extends Throwable {
  override def getMessage: String = "API Request enrichment:" ++ toString
  override def toString = message
}
