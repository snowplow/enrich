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
package com.snowplowanalytics.snowplow.enrich.pubsub

import cats.effect.Concurrent
import cats.Parallel
import cats.implicits._
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

  def sink[F[_]: Concurrent, A](f: A => F[Unit]): Pipe[F, Payload[F, A], Unit] =
    _.parEvalMap(SinkConcurrency)(p => f(p.data) *> p.finalise)

  def sinkAll[F[_]: Concurrent: Parallel, A](f: A => F[Unit]): Pipe[F, Payload[F, List[A]], Unit] =
    sink(_.parTraverse_(f))

  /**
   * Controls the maximum number of payloads we can be waiting to get sunk
   *
   *  For the Pubsub sink this should at least exceed the number events we can sink within
   *  [[io.Sinks.DefaultDelayThreshold]].
   *
   *  For the FileSystem source this is the primary way that we control the memory footprint of the
   *  app.
   */
  val SinkConcurrency = 10000

}
