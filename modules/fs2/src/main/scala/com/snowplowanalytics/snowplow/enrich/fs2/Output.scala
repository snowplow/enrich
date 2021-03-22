/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.fs2

import cats.effect.Concurrent
import fs2.Pipe

import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

/** Represents the three different outputs of enrichment that should be delivered to three different sinks */
sealed trait Output

object Output {

  case class Bad(badRow: BadRow) extends Output
  case class Good(event: EnrichedEvent) extends Output
  case class Pii(event: EnrichedEvent) extends Output

  /** A variant of fs2's observeEither, sinking three output types into three Pipe's */
  def sink[F[_]: Concurrent](
    bad: Pipe[F, BadRow, Unit],
    good: Pipe[F, EnrichedEvent, Unit],
    pii: Pipe[F, EnrichedEvent, Unit]
  ): Pipe[F, Output, Unit] =
    _.observe(_.collect { case Bad(br) => br }.through(bad))
      .observe(_.collect { case Good(event) => event }.through(good))
      .observe(_.collect { case Pii(event) => event }.through(pii))
      .drain

}
