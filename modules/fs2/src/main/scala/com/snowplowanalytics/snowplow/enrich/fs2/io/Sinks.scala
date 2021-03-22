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

import cats.implicits._

import cats.effect.{Blocker, Concurrent, ContextShift, Resource, Sync}

import fs2.{Pipe, Stream, text}
import fs2.io.file.writeAll

import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import com.permutive.pubsub.producer.Model.{ProjectId, Topic}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}

import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

import com.snowplowanalytics.snowplow.enrich.fs2.{BadSink, Enrich, GoodSink}
import com.snowplowanalytics.snowplow.enrich.fs2.config.io.{Authentication, Output}

object Sinks {

  /**
   * Set the delay threshold to use for batching. After this amount of time has elapsed (counting
   * from the first element added), the elements will be wrapped up in a batch and sent. This
   * value should not be set too high, usually on the order of milliseconds. Otherwise, calls
   * might appear to never complete.
   */
  val DelayThreshold: FiniteDuration = 200.milliseconds

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
        Resource.pure[F, GoodSink[F]](goodFileSink(o.dir, blocker))
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
        Resource.pure[F, BadSink[F]](badFileSink(o.dir, blocker))
    }

  def pubsubSink[F[_]: Concurrent, A: MessageEncoder](
    output: Output.PubSub
  ): Resource[F, Pipe[F, A, Unit]] = {
    val config = PubsubProducerConfig[F](
      batchSize = 5,
      delayThreshold = DelayThreshold,
      onFailedTerminate = err => Logger[F].error(err)("PubSub sink termination error")
    )

    GooglePubsubProducer
      .of[F, A](ProjectId(output.project), Topic(output.name), config)
      .map(producer => (s: Stream[F, A]) => s.parEvalMapUnordered(Enrich.ConcurrencyLevel)(row => producer.produce(row).void))
  }

  def goodFileSink[F[_]: Concurrent: ContextShift](goodOut: Path, blocker: Blocker): GoodSink[F] =
    fileSink(goodOut, blocker).compose(in => in.map(Enrich.encodeEvent(_)))

  def badFileSink[F[_]: Concurrent: ContextShift](badOut: Path, blocker: Blocker): BadSink[F] =
    fileSink(badOut, blocker).compose(in => in.map(_.compact))

  private def fileSink[F[_]: Sync: ContextShift](path: Path, blocker: Blocker): Pipe[F, String, Unit] =
    _.intersperse("\n")
      .through(text.utf8Encode)
      .through(writeAll[F](path, blocker, List(StandardOpenOption.CREATE_NEW)))

}
