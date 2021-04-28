/*
 * Copyright (c) 2019-2021 Snowplow Analytics Ltd. All rights reserved.
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

import cats.effect.{Blocker, Concurrent, ContextShift, Sync}
import cats.implicits._

import fs2.Stream
import fs2.io.file.{directoryStream, readAll}

import com.permutive.pubsub.consumer.Model
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}

import com.snowplowanalytics.snowplow.enrich.fs2.{Payload, RawSource}
import com.snowplowanalytics.snowplow.enrich.fs2.config.io.{Authentication, Input}

import com.google.pubsub.v1.PubsubMessage

import java.nio.file.{Files, Path}

object Source {

  /**
   * Number of threads used internally by permutive library to handle incoming messages.
   * These threads do very little "work" apart from writing the message to a concurrent Queue.
   * Overrides the permutive library default of `3`.
   */
  val ParallelPullCount = 1

  /**
   * Configures the "max outstanding element count" of pubSub.
   *
   * This is the principal way we control concurrency in the app; it puts an upper bound on the number
   * of events in memory at once. An event counts towards this limit starting from when it received
   * by the permutive library, until we ack it (after publishing to output). The value must be large
   * enough that it does not cause the sink to block whilst it is waiting for a batch to be
   * completed.
   *
   * (`1000` is the permutive library default; we re-define it here to be explicit)
   */
  val MaxQueueSize = 1000

  def read[F[_]: Concurrent: ContextShift](
    blocker: Blocker,
    auth: Authentication,
    input: Input
  ): RawSource[F] =
    (auth, input) match {
      case (Authentication.Gcp, p: Input.PubSub) =>
        pubSub(blocker, p)
      case (_, p: Input.FileSystem) =>
        recursiveDirectoryStream(blocker, p.dir)
          .evalMap { file =>
            readAll[F](file, blocker, 4096).compile
              .to(Array)
              .map(bytes => Payload(bytes, Sync[F].unit))
          }
    }

  def pubSub[F[_]: Concurrent: ContextShift](
    blocker: Blocker,
    input: Input.PubSub
  ): Stream[F, Payload[F, Array[Byte]]] = {
    val onFailedTerminate: Throwable => F[Unit] =
      e => Sync[F].delay(System.err.println(s"Cannot terminate ${e.getMessage}"))
    val pubSubConfig =
      PubsubGoogleConsumerConfig(onFailedTerminate = onFailedTerminate, parallelPullCount = ParallelPullCount, maxQueueSize = MaxQueueSize)
    val projectId = Model.ProjectId(input.project)
    val subscriptionId = Model.Subscription(input.name)
    val errorHandler: (PubsubMessage, Throwable, F[Unit], F[Unit]) => F[Unit] = // Should be useless
      (message, error, _, _) =>
        Sync[F].delay(System.err.println(s"Cannot decode message ${message.getMessageId} into array of bytes. ${error.getMessage}"))
    PubsubGoogleConsumer
      .subscribe[F, Array[Byte]](blocker, projectId, subscriptionId, errorHandler, pubSubConfig)
      .map(record => Payload(record.value, record.ack))
  }

  private def recursiveDirectoryStream[F[_]: Sync: ContextShift](blocker: Blocker, path: Path): Stream[F, Path] =
    for {
      subPath <- directoryStream(blocker, path)
      isDir <- Stream.eval(blocker.delay(Files.isDirectory(subPath)))
      file <- if (isDir) recursiveDirectoryStream(blocker, subPath) else Stream.emit(subPath)
    } yield file
}
