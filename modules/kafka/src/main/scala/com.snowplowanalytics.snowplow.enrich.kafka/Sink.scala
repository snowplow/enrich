/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */

package com.snowplowanalytics.snowplow.enrich.kafka

import java.util.UUID

import cats.Parallel
import cats.implicits._
import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, Resource, Timer}

import fs2.kafka._

import com.snowplowanalytics.snowplow.enrich.common.fs2.{AttributedByteSink, AttributedData, ByteSink}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Output

object Sink {

  def init[F[_]: ConcurrentEffect: ContextShift: Parallel: Timer](
    blocker: Blocker,
    output: Output
  ): Resource[F, ByteSink[F]] =
    for {
      sink <- initAttributed(blocker, output)
    } yield (records: List[Array[Byte]]) => sink(records.map(AttributedData(_, UUID.randomUUID().toString, Map.empty)))

  def initAttributed[F[_]: ConcurrentEffect: ContextShift: Parallel: Timer](
    blocker: Blocker,
    output: Output
  ): Resource[F, AttributedByteSink[F]] =
    output match {
      case k: Output.Kafka =>
        mkProducer(blocker, k).map { producer => records =>
          records.parTraverse_ { record =>
            producer
              .produceOne_(toProducerRecord(k.topicName, record))
              .flatten
              .void
          }
        }
      case o => Resource.eval(Concurrent[F].raiseError(new IllegalArgumentException(s"Output $o is not Kafka")))
    }

  private def mkProducer[F[_]: ConcurrentEffect: ContextShift](
    blocker: Blocker,
    output: Output.Kafka
  ): Resource[F, KafkaProducer[F, String, Array[Byte]]] = {
    val producerSettings =
      ProducerSettings[F, String, Array[Byte]]
        .withBootstrapServers(output.bootstrapServers)
        .withProperties(output.producerConf)
        .withBlocker(blocker)
        .withProperties(
          ("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
          ("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        )

    KafkaProducer[F].resource(producerSettings)
  }

  private def toProducerRecord(topicName: String, record: AttributedData[Array[Byte]]): ProducerRecord[String, Array[Byte]] =
    ProducerRecord(topicName, record.partitionKey, record.data)
      .withHeaders(Headers.fromIterable(record.attributes.map(t => Header(t._1, t._2))))
}
