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
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry.snowplow

import cats.Monad
import cats.data.NonEmptyList

import cats.effect.Clock
import cats.syntax.validated._

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter.Adapted
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

/** Version 1 of the Tracker Protocol is GET only. All data comes in on the querystring. */
object Tp1Adapter extends Adapter {

  /**
   * Converts a CollectorPayload instance into raw events. Tracker Protocol 1 only supports a single
   * event in a payload.
   * @param payload The CollectorPaylod containing one or more raw events
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad: Clock](
    payload: CollectorPayload,
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F]
  ): F[Adapted] = {
    val _ = client
    val params = toMap(payload.querystring)
    if (params.isEmpty) {
      val msg = "empty querystring: not a valid URI redirect"
      val failure = FailureDetails.AdapterFailure.InputData("querystring", None, msg)
      Monad[F].pure(failure.invalidNel)
    } else
      Monad[F].pure(
        NonEmptyList
          .one(
            RawEvent(
              api = payload.api,
              parameters = params,
              contentType = payload.contentType,
              source = payload.source,
              context = payload.context
            )
          )
          .valid
      )
  }
}
