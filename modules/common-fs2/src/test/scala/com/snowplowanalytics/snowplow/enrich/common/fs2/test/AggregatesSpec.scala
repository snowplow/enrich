/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
