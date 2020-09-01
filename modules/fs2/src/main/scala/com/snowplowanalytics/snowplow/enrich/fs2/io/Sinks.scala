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
package com.snowplowanalytics.snowplow.enrich.fs2.io

import java.nio.file.{Path, StandardOpenOption}

import scala.concurrent.duration._

import cats.syntax.flatMap._
import cats.syntax.functor._

import cats.effect.{Blocker, Concurrent, ContextShift, Resource, Sync}

import fs2.{Pipe, Stream, text}
import fs2.io.file.writeAll

import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.fs2.{BadSink, Enrich, GoodSink, Payload}
import com.snowplowanalytics.snowplow.enrich.fs2.config.io.{Authentication, Output}

import com.permutive.pubsub.producer.Model.{ProjectId, Topic}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

object Sinks {

  /**
   * Set the delay threshold to use for batching. After this amount of time has elapsed (counting
   * from the first element added), the elements will be wrapped up in a batch and sent. This
   * value should not be set too high, usually on the order of milliseconds. Otherwise, calls
   * might appear to never complete.
   */
  val DelayThreshold: FiniteDuration = 5.seconds

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def goodSink[F[_]: Concurrent: ContextShift](
    blocker: Blocker,
    auth: Authentication,
    output: Output
  ): Resource[F, GoodSink[F]] =
    (auth, output) match {
      case (Authentication.Gcp, o: Output.PubSub) =>
        pubsubSink[F, EnrichedEvent](o)
      case (_, o: Output.FileSystem) =>
        Resource.pure(goodFileSink(o.dir, blocker))
    }

  def badSink[F[_]: Concurrent: ContextShift](
    blocker: Blocker,
    auth: Authentication,
    output: Output
  ): Resource[F, BadSink[F]] =
    (auth, output) match {
      case (Authentication.Gcp, o: Output.PubSub) =>
        pubsubSink[F, BadRow](o)
      case (_, o: Output.FileSystem) =>
        Resource.pure(badFileSink(o.dir, blocker))
    }

  def pubsubSink[F[_]: Concurrent, A: MessageEncoder](
    output: Output.PubSub
  ): Resource[F, Pipe[F, Payload[F, A], Unit]] = {
    val config = PubsubProducerConfig[F](
      batchSize = 5,
      delayThreshold = DelayThreshold,
      onFailedTerminate = err => Logger[F].error(err)("PubSub sink termination error")
    )

    GooglePubsubProducer
      .of[F, A](ProjectId(output.project), Topic(output.name), config)
      .map(producer =>
        (s: Stream[F, Payload[F, A]]) => s.parEvalMapUnordered(Enrich.ConcurrencyLevel)(row => producer.produce(row.data) >> row.finalise)
      )
  }

  def goodFileSink[F[_]: Sync: ContextShift](goodOut: Path, blocker: Blocker): GoodSink[F] =
    goodStream =>
      goodStream
        .evalMap(p => p.finalise.as(Enrich.encodeEvent(p.data)))
        .intersperse("\n")
        .through(text.utf8Encode)
        .through(writeAll[F](goodOut, blocker, List(StandardOpenOption.CREATE_NEW)))

  def badFileSink[F[_]: Sync: ContextShift](badOut: Path, blocker: Blocker): BadSink[F] =
    badStream =>
      badStream
        .evalMap(p => p.finalise.as(p.data.compact))
        .intersperse("\n")
        .through(text.utf8Encode)
        .through(writeAll[F](badOut, blocker, List(StandardOpenOption.CREATE_NEW)))
}
