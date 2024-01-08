/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.enrich.kafka

import cats.effect.{ConcurrentEffect, ContextShift, Timer}

import fs2.kafka.{CommittableConsumerRecord, ConsumerSettings, KafkaConsumer}
import fs2.Stream

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Input

object Source {

  def init[F[_]: ConcurrentEffect: ContextShift: Timer](
    input: Input
  ): Stream[F, CommittableConsumerRecord[F, String, Array[Byte]]] =
    input match {
      case k: Input.Kafka => kafka(k)
      case i => Stream.raiseError[F](new IllegalArgumentException(s"Input $i is not Kafka"))
    }

  def kafka[F[_]: ConcurrentEffect: ContextShift: Timer](
    input: Input.Kafka
  ): Stream[F, CommittableConsumerRecord[F, String, Array[Byte]]] = {
    val consumerSettings =
      ConsumerSettings[F, String, Array[Byte]]
        .withBootstrapServers(input.bootstrapServers)
        .withProperties(input.consumerConf)
        .withEnableAutoCommit(false) // prevent enabling auto-commits by setting this after user-provided config
        .withProperties(
          ("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"),
          ("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        )

    KafkaConsumer[F]
      .stream(consumerSettings)
      .subscribeTo(input.topicName)
      .records
  }
}
