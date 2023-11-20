/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry

import cats.Monad
import cats.data.NonEmptyList
import cats.effect.Clock
import cats.syntax.validated._

import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonUtils
import com.snowplowanalytics.snowplow.enrich.common.adapters._
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter.Adapted

/**
 * Transforms a collector payload which conforms to
 * a known version of the AD-X Tracking webhook
 * into raw events.
 */
case class CallrailAdapter(schemas: CallrailSchemas) extends Adapter {

  // Tracker version for an AD-X Tracking webhook
  private val TrackerVersion = "com.callrail-v1"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private object SchemaUris {
    val CallComplete = schemas.callCompleteSchemaKey
  }

  // Datetime format used by CallRail (as we will need to massage)
  private val CallrailDateTimeFormat =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC)

  // Create a simple formatter function
  private val CallrailFormatter: FormatterFunc = {
    val bools = List("first_call", "answered")
    val ints = List("duration")
    val dateTimes: JsonUtils.DateTimeFields =
      Some((NonEmptyList.of("datetime"), CallrailDateTimeFormat))
    buildFormatter(bools, ints, dateTimes)
  }

  /**
   * Converts a CollectorPayload instance into raw events. A CallRail payload only contains a single
   * event.
   * @param payload The CollectorPaylod containing one or more raw events
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad: RegistryLookup: Clock](
    payload: CollectorPayload,
    client: IgluCirceClient[F]
  ): F[Adapted] = {
    val _ = client
    val params = toMap(payload.querystring)
    if (params.isEmpty) {
      val failure = FailureDetails.AdapterFailure.InputData(
        "querystring",
        None,
        "empty querystring"
      )
      Monad[F].pure(failure.invalidNel)
    } else
      Monad[F].pure(
        NonEmptyList
          .of(
            RawEvent(
              api = payload.api,
              parameters = toUnstructEventParams(
                TrackerVersion,
                params,
                SchemaUris.CallComplete,
                CallrailFormatter,
                "srv"
              ),
              contentType = payload.contentType,
              source = payload.source,
              context = payload.context
            )
          )
          .valid
      )
  }
}
