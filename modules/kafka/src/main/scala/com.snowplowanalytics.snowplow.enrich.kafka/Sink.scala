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

import java.util.UUID
import cats.Parallel
import cats.implicits._
import cats.effect.kernel.{Async, Resource, Sync}
import fs2.kafka._
import com.snowplowanalytics.snowplow.enrich.common.fs2.{AttributedByteSink, AttributedData, ByteSink}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Output
import fs2.Chunk

object Sink {

  def init[F[_]: Async: Parallel](
    output: Output,
    authCallbackClass: String
  ): Resource[F, ByteSink[F]] =
    for {
      sink <- initAttributed(output, authCallbackClass)
    } yield (records: List[Array[Byte]]) => sink(records.map(AttributedData(_, UUID.randomUUID().toString, Map.empty)))

  def initAttributed[F[_]: Async: Parallel](
    output: Output,
    authCallbackClass: String
  ): Resource[F, AttributedByteSink[F]] =
    output match {
      case k: Output.Kafka =>
        mkProducer(k, authCallbackClass).map { producer => records =>
          producer
            .produce(Chunk.from(records.map(toProducerRecord(k.topicName, _))))
            .flatten
            .void
        }
      case o => Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Output $o is not Kafka")))
    }

  private def mkProducer[F[_]: Async](
    output: Output.Kafka,
    authCallbackClass: String
  ): Resource[F, KafkaProducer[F, String, Array[Byte]]] = {
    val producerSettings =
      ProducerSettings[F, String, Array[Byte]]
        .withBootstrapServers(output.bootstrapServers)
        // set before user-provided config to make it possible to override it via config
        .withProperty("sasl.login.callback.handler.class", authCallbackClass)
        .withProperties(output.producerConf)
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
