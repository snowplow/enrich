/*
 * Copyright (c) 2019 Snowplow Analytics Ltd. All rights reserved.
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

import fs2.Stream

import com.snowplowanalytics.snowplow.enrich.fs2.{Payload, RawSource}

import com.permutive.pubsub.consumer.Model
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import com.snowplowanalytics.snowplow.enrich.fs2.config.io.{Authentication, Input}
import com.snowplowanalytics.snowplow.enrich.fs2.config.io.Input.PubSub

import com.google.pubsub.v1.PubsubMessage

object Source {

  def read[F[_]: Concurrent: ContextShift](
    blocker: Blocker,
    auth: Authentication,
    input: Input
  ): RawSource[F] =
    (auth, input) match {
      case (g: Authentication.Gcp, p: Input.PubSub) => pubSub(blocker, g, p)
      case _ => ???
    }

  def pubSub[F[_]: Concurrent: ContextShift](
    blocker: Blocker,
    auth: Authentication.Gcp,
    input: PubSub
  ): Stream[F, Payload[F, Array[Byte]]] = {
    val onFailedTerminate: Throwable => F[Unit] =
      e => Sync[F].delay(System.err.println(s"Boom ${e.getMessage}"))
    val pubSubConfig = PubsubGoogleConsumerConfig(onFailedTerminate = onFailedTerminate)
    val projectId = Model.ProjectId(auth.projectId)
    val subscriptionId = Model.Subscription(input.subscriptionId)
    val errorHandler: (PubsubMessage, Throwable, F[Unit], F[Unit]) => F[Unit] = // Should be useless
      (message, error, _, _) =>
        Sync[F].delay(System.err.println(s"Cannot decode message ${message.getMessageId} into array of bytes. ${error.getMessage}"))
    PubsubGoogleConsumer
      .subscribe[F, Array[Byte]](blocker, projectId, subscriptionId, errorHandler, pubSubConfig)
      .map(record => Payload(record.value, record.ack))
  }
}
