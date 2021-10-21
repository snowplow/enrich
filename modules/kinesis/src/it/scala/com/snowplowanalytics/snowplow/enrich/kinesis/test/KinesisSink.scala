/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.kinesis.it

import java.nio.ByteBuffer
import java.util.UUID

import scala.concurrent.duration._

import cats.implicits._

import cats.effect.{Async, Blocker, Resource, Timer, Sync}

import fs2.{ Pipe, Stream }

import fs2.aws.internal.KinesisProducerClient

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import retry.syntax.all._
import retry.RetryPolicies._

import com.google.common.util.concurrent.{Futures, ListenableFuture, FutureCallback}

import com.amazonaws.services.kinesis.producer.{KinesisProducerConfiguration, KinesisProducer, UserRecordResult}

object KinesisSink {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def init[F[_]: Async: Timer](
    blocker: Blocker,
    stream: String
  ): Pipe[F, Array[Byte], Unit] = {
    val producerConfig = new KinesisProducerConfiguration()
      .setThreadingModel(KinesisProducerConfiguration.ThreadingModel.POOLED)
      .setRegion(Resources.region)
      .setMetricsLevel("none")

    (payloads: Stream[F, Array[Byte]]) =>
      Stream.resource(mkProducer[F](producerConfig)).flatMap { producer =>
        payloads.evalMap(bytes => writeToKinesis(blocker, producer, stream, bytes))
      }
  }

  def mkProducer[F[_]: Sync](config: KinesisProducerConfiguration): Resource[F, KinesisProducerClient[F]] = {
    val mkProducer =
      Resource.make(Sync[F].delay(new KinesisProducer(config)))(producer => Sync[F].delay(producer.flushSync()) >> Sync[F].delay(producer.destroy()))

    mkProducer.map { client =>
      new KinesisProducerClient[F] {
        def putData(streamName: String, partitionKey: String, data: ByteBuffer)(implicit F: Sync[F]): F[ListenableFuture[UserRecordResult]] =
          F.delay(client.addUserRecord(streamName, partitionKey, data))
      }
    }
  }

  private def writeToKinesis[F[_]: Async: Timer](
    blocker: Blocker,
    producer: KinesisProducerClient[F],
    stream: String,
    data: Array[Byte]
  ): F[Unit] = {
    val retryPolicy = capDelay[F](10.second, exponentialBackoff[F](100.millisecond))
    val partitionKey = UUID.randomUUID().toString

    val res = for {
      byteBuffer <- Async[F].delay(ByteBuffer.wrap(data))
      cb <- producer.putData(stream, partitionKey, byteBuffer)
      cbRes <- registerCallback(blocker, cb)
    } yield cbRes

    res
      .retryingOnFailuresAndAllErrors(
        wasSuccessful = _.isSuccessful,
        policy = retryPolicy,
        onFailure = (result, retryDetails) =>
          Logger[F].warn(s"Writing to shard ${result.getShardId()} failed after ${retryDetails.retriesSoFar} retry"),
        onError = (exception, retryDetails) =>
          Logger[F]
            .error(s"Writing to Kinesis errored after ${retryDetails.retriesSoFar} retry. Error: ${exception.toString}") >>
            Async[F].raiseError(exception)
      )
      .void
  }

  private def registerCallback[F[_]: Async](blocker: Blocker, f: ListenableFuture[UserRecordResult]): F[UserRecordResult] =
    Async[F].async[UserRecordResult] { cb =>
      Futures.addCallback(
        f,
        new FutureCallback[UserRecordResult] {
          override def onFailure(t: Throwable): Unit = cb(Left(t))
          override def onSuccess(result: UserRecordResult): Unit = cb(Right(result))
        },
        (command: Runnable) => blocker.blockingContext.execute(command)
      )
    }
}
