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
package com.snowplowanalytics.snowplow.enrich.common.adapters

import com.snowplowanalytics.snowplow.badrows.Payload.{RawEvent => RE}
import com.snowplowanalytics.snowplow.badrows.NVP

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.RawEventParameters

/**
 * The canonical input format for the ETL process:
 * it should be possible to convert any collector payload to this raw event format via an `Adapter`,
 * ready for the main, collector-agnostic stage of the Enrichment.
 * Unlike `CollectorPayload`, where `body` can contain full POST payload with multiple events,
 * [[RawEvent]] is always a single Snowplow event
 */
final case class RawEvent(
  api: CollectorPayload.Api,
  parameters: RawEventParameters,
  contentType: Option[String], // Not yet used but should be logged
  source: CollectorPayload.Source,
  context: CollectorPayload.Context
)

object RawEvent {
  def toRawEvent(re: RawEvent): RE =
    RE(
      re.api.vendor,
      re.api.version,
      re.parameters.toList.map { case (k, v) => NVP(k, v) },
      re.contentType,
      re.source.name,
      re.source.encoding,
      re.source.hostname,
      re.context.timestamp,
      re.context.ipAddress,
      re.context.useragent,
      re.context.refererUri,
      re.context.headers,
      re.context.userId
    )
}
