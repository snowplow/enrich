/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.test

import cats.effect.Sync
import cats.effect.concurrent.Ref
import fs2.Stream
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.experimental.Metadata
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.experimental.Metadata.{EntitiesAndCount, MetadataEvent}

object AggregatesSpec {
  def init[F[_]: Sync] = Ref.of[F, Map[MetadataEvent, EntitiesAndCount]](Map.empty)

  def metadata[F[_]](ref: Ref[F, Map[MetadataEvent, EntitiesAndCount]]): Metadata[F] =
    new Metadata[F] {
      def report: Stream[F, Unit] = Stream.empty.covary[F]
      def observe(events: List[EnrichedEvent]): F[Unit] =
        ref.update(Metadata.recalculate(_, events))
    }
}
