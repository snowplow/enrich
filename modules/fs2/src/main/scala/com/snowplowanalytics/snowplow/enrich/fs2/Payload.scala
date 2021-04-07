/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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

/**
 * Anything that has been read from [[RawSource]] and needs to be acknowledged
 * or a derivative (parsed `A`) that can be used to acknowledge the original message
 * @param data original data or anything it has been transformed to
 * @param finalise a side-effect to acknowledge (commit, log on-finish) the message or
 *                 no-op in case the original message has been flattened into
 *                 multiple rows and only last row contains the actual side-effect
 */
case class Payload[F[_], A](data: A, finalise: F[Unit])

object Payload {

  def sink[F[_]: Concurrent, A](inner: Pipe[F, A, Unit]): Pipe[F, Payload[F, A], Unit] =
    _.observeAsync(Enrich.ConcurrencyLevel)(_.map(_.data).through(inner))
      .evalMap(_.finalise)

}
