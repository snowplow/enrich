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
package com.snowplowanalytics.snowplow.enrich.fs2.io

import java.nio.ByteBuffer
import java.nio.file.{Path, StandardOpenOption}
import java.nio.channels.FileChannel

import scala.concurrent.duration._

import cats.implicits._

import cats.effect.{Blocker, Concurrent, ContextShift, Resource, Sync, Timer}
import cats.effect.concurrent.Semaphore

import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import com.permutive.pubsub.producer.Model.{ProjectId, Topic}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}

import com.snowplowanalytics.snowplow.enrich.fs2.{AttributedByteSink, AttributedData, ByteSink}
import com.snowplowanalytics.snowplow.enrich.fs2.config.io.{Authentication, Output}

object Sinks {

  /**
   * Set the delay threshold to use for batching. After this amount of time has elapsed (counting
   * from the first element added), the elements will be wrapped up in a batch and sent.
   *
   * If the source MaxQueueSize is small, then the queue could block between batches if
   * DelayThreshold is too large.  If the source MaxQueueSize is sufficiently large, then the
   * DelayThreshold should not ever cause blocking.
   */
  val DelayThreshold: FiniteDuration = 200.milliseconds

  /**
   * A batch of messages will be emitted to PubSub when the batch reaches 1000 messages.
   * We use 1000 because it is the maximum batch size allowed by PubSub.
   * This overrides the permutive library default of `5`
   */
  val PubsubMaxBatchSize = 1000L

  /**
   * A batch of messages will be emitted to PubSub when the batch reaches 10 MB.
   * We use 10MB because it is the maximum batch size allowed by PubSub.
   */
  val PubsubMaxBatchBytes = 10000000L

  /**
   * The number of threads used internally by permutive library to process the callback after message delivery.
   * The callback does very little "work" so we use the minimum number of threads.
   * This overrides the permutive library default of 3 * num processors
   */
  val NumCallbackExecutors = 1

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def sink[F[_]: Concurrent: ContextShift: Timer](
    blocker: Blocker,
    auth: Authentication,
    output: Output
  ): Resource[F, ByteSink[F]] =
    (auth, output) match {
      case (Authentication.Gcp, o: Output.PubSub) =>
        pubsubSink[F, Array[Byte]](o).map(sink => bytes => sink(AttributedData(bytes, Map.empty)))
      case (_, o: Output.FileSystem) =>
        fileSink(o.dir, blocker)
    }

  def attributedSink[F[_]: Concurrent: ContextShift: Timer](
    blocker: Blocker,
    auth: Authentication,
    output: Output
  ): Resource[F, AttributedByteSink[F]] =
    (auth, output) match {
      case (Authentication.Gcp, o: Output.PubSub) =>
        pubsubSink[F, Array[Byte]](o)
      case (_, o: Output.FileSystem) =>
        fileSink(o.dir, blocker).map(sink => row => sink(row.data))
    }

  def pubsubSink[F[_]: Concurrent, A: MessageEncoder](
    output: Output.PubSub
  ): Resource[F, AttributedData[A] => F[Unit]] = {
    val config = PubsubProducerConfig[F](
      batchSize = PubsubMaxBatchSize,
      requestByteThreshold = Some(PubsubMaxBatchBytes),
      delayThreshold = DelayThreshold,
      callbackExecutors = NumCallbackExecutors,
      onFailedTerminate = err => Logger[F].error(err)("PubSub sink termination error")
    )

    GooglePubsubProducer
      .of[F, A](ProjectId(output.project), Topic(output.name), config)
      .map { producer => row: AttributedData[A] => producer.produce(row.data, row.attributes).void }
  }

  private def fileSink[F[_]: Concurrent: ContextShift](path: Path, blocker: Blocker): Resource[F, ByteSink[F]] =
    for {
      channel <- Resource.fromAutoCloseableBlocking(blocker)(
                   Sync[F].delay(FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))
                 )
      sem <- Resource.eval(Semaphore(1L))
    } yield { bytes =>
      sem.withPermit {
        blocker.delay {
          channel.write(ByteBuffer.wrap(bytes))
          channel.write(ByteBuffer.wrap(Array('\n'.toByte)))
        }.void
      }
    }

}
