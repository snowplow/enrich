/*
 * Copyright (c) 2019-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.pubsub

import cats.effect.kernel.Sync

import com.google.api.gax.batching.FlowControlSettings
import com.google.api.gax.batching.FlowController.LimitExceededBehavior

import com.permutive.pubsub.consumer.{ConsumerRecord, Model}
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}

import com.google.pubsub.v1.PubsubMessage

import fs2.Stream

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Input

object Source {

  def init[F[_]: Sync](
    input: Input
  ): Stream[F, ConsumerRecord[F, Array[Byte]]] =
    input match {
      case p: Input.PubSub =>
        pubSub(p)
      case i =>
        Stream.raiseError[F](new IllegalArgumentException(s"Input $i is not PubSub"))
    }

  def pubSub[F[_]: Sync](
    input: Input.PubSub
  ): Stream[F, ConsumerRecord[F, Array[Byte]]] = {
    val onFailedTerminate: Throwable => F[Unit] =
      e => Sync[F].delay(System.err.println(s"Cannot terminate ${e.getMessage}"))

    val flowControlSettings: FlowControlSettings =
      FlowControlSettings
        .newBuilder()
        .setMaxOutstandingElementCount(input.maxQueueSize)
        .setMaxOutstandingRequestBytes(input.maxRequestBytes)
        .setLimitExceededBehavior(LimitExceededBehavior.Block)
        .build()

    val pubSubConfig =
      PubsubGoogleConsumerConfig(
        onFailedTerminate = onFailedTerminate,
        parallelPullCount = input.parallelPullCount,
        maxQueueSize = input.maxQueueSize,
        maxAckExtensionPeriod = input.maxAckExtensionPeriod,
        customizeSubscriber = Some {
          _.setFlowControlSettings(flowControlSettings)
            .setHeaderProvider(Utils.createPubsubUserAgentHeader(input.gcpUserAgent))
        }
      )

    val projectId = Model.ProjectId(input.project)
    val subscriptionId = Model.Subscription(input.name)
    val errorHandler: (PubsubMessage, Throwable, F[Unit], F[Unit]) => F[Unit] = // Should be useless
      (message, error, _, _) =>
        Sync[F].delay(System.err.println(s"Cannot decode message ${message.getMessageId} into array of bytes. ${error.getMessage}"))

    PubsubGoogleConsumer
      .subscribe[F, Array[Byte]](projectId, subscriptionId, errorHandler, pubSubConfig)
  }
}
