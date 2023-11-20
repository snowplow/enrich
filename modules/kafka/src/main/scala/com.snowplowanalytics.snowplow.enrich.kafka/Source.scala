/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
