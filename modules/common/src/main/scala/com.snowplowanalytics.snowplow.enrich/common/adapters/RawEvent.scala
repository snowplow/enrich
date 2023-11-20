/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
