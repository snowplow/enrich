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
