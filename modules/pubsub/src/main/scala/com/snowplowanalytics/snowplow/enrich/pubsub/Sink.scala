/*
 * Copyright (c) 2021-2021 Snowplow Analytics Ltd. All rights reserved.
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

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.implicits._

import cats.effect.{Concurrent, ContextShift, Resource, Sync, Timer}

import com.permutive.pubsub.producer.Model.{ProjectId, Topic}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}

import com.snowplowanalytics.snowplow.enrich.common.fs2.{AttributedByteSink, AttributedData, ByteSink}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Output

object Sink {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def init[F[_]: Concurrent: ContextShift: Timer](
    output: Output
  ): Resource[F, ByteSink[F]] =
    output match {
      case o: Output.PubSub =>
        pubsubSink[F, Array[Byte]](o).map(sink => bytes => sink(AttributedData(bytes, Map.empty)))
      case o =>
        Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Output $o is not PubSub")))
    }

  def initAttributed[F[_]: Concurrent: ContextShift: Timer](
    output: Output
  ): Resource[F, AttributedByteSink[F]] =
    output match {
      case o: Output.PubSub =>
        pubsubSink[F, Array[Byte]](o)
      case o =>
        Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Output $o is not PubSub")))
    }

  private def pubsubSink[F[_]: Concurrent, A: MessageEncoder](
    output: Output.PubSub
  ): Resource[F, AttributedData[A] => F[Unit]] = {
    val config = PubsubProducerConfig[F](
      batchSize = output.maxBatchSize,
      requestByteThreshold = Some(output.maxBatchBytes),
      delayThreshold = output.delayThreshold,
      onFailedTerminate = err => Logger[F].error(err)("PubSub sink termination error")
    )

    GooglePubsubProducer
      .of[F, A](ProjectId(output.project), Topic(output.name), config)
      .map(producer => row => producer.produce(row.data, row.attributes).void)
  }
}
