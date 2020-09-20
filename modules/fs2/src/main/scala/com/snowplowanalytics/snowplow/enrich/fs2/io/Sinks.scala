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

import java.nio.file.Path

import scala.concurrent.duration._

import cats.syntax.apply._

import cats.effect.{Async, Blocker, ContextShift, Resource, Sync}

import fs2.{Stream, Pipe, text}
import fs2.io.file.writeAll

import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.fs2.{BadSink, Enrich, GoodSink, Payload}
import com.snowplowanalytics.snowplow.enrich.fs2.config.io.{Authentication, Output}

import com.permutive.pubsub.producer.Model.{ProjectId, Topic}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}

object Sinks {

  def goodSink[F[_]: Async](auth: Authentication, output: Output): Resource[F, GoodSink[F]] =
    (auth, output) match {
      case (a: Authentication.Gcp, o: Output.PubSub) =>
        pubsubSink[F, EnrichedEvent](a, o)
      case _ =>
        ???
    }


  def badSink[F[_]: Async](
    auth: Authentication,
    output: Output
  ): Resource[F, BadSink[F]] =
    (auth, output) match {
      case (a: Authentication.Gcp, o: Output.PubSub) =>
        pubsubSink[F, BadRow](a, o)
      case _ =>
        ???
    }

  def after[F[_], A](count: F[Unit])(pipe: Pipe[F, Payload[F, A], Unit]): Pipe[F, Payload[F, A], Unit] =
    pipe.andThen(sink => sink.evalMap(_ => count))

  def pubsubSink[F[_]: Async, A: MessageEncoder](auth: Authentication.Gcp, output: Output.PubSub): Resource[F, Pipe[F, Payload[F, A], Unit]] = {
    val config = PubsubProducerConfig[F](
      batchSize = 10,
      delayThreshold = 5.seconds,
      onFailedTerminate = err => Sync[F].delay(System.err.println(err))
    )

    GooglePubsubProducer
      .of[F, A](ProjectId(auth.projectId), Topic(output.topic), config)
      .map(producer => (s: Stream[F, Payload[F, A]]) => s.evalMap(row => producer.produce(row.data) *> row.finalise))
  }

  def goodFileSink[F[_]: Sync: ContextShift](goodOut: Path, blocker: Blocker): GoodSink[F] =
    goodStream =>
      goodStream
        .map(p => Enrich.encodeEvent(p.data))
        .intersperse("\n")
        .through(text.utf8Encode)
        .through(writeAll[F](goodOut, blocker))

  def badFileSink[F[_]: Sync: ContextShift](badOut: Path, blocker: Blocker): BadSink[F] =
    badStream =>
      badStream
        .map(p => p.data.compact)
        .intersperse("\n")
        .through(text.utf8Encode)
        .through(writeAll[F](badOut, blocker))
}
