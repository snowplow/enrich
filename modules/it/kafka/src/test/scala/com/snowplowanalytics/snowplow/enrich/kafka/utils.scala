/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.kafka

import scala.concurrent.duration._

import cats.Id

import cats.effect.IO

import com.snowplowanalytics.snowplow.streams.kafka.{KafkaFactory, KafkaSinkConfigM, KafkaSourceConfig}

import com.snowplowanalytics.snowplow.enrich.core.Utils

object utils {

  def run(
    enrichKafka: EnrichKafka,
    nbEnriched: Long,
    nbBad: Long
  ): IO[Utils.Output] = {
    val rawSinkConfig = sinkConfig(enrichKafka.kafkaConfig.host, enrichKafka.kafkaConfig.port, enrichKafka.rawTopic)
    val enrichedSourceConfig = sourceConfig(enrichKafka.kafkaConfig.host, enrichKafka.kafkaConfig.port, enrichKafka.enrichedTopic)
    val failedSourceConfig = sourceConfig(enrichKafka.kafkaConfig.host, enrichKafka.kafkaConfig.port, enrichKafka.failedTopic)
    val badSourceConfig = sourceConfig(enrichKafka.kafkaConfig.host, enrichKafka.kafkaConfig.port, enrichKafka.badTopic)

    KafkaFactory.resource[IO].use { factory =>
      Utils.runEnrichPipe(
        factory,
        rawSinkConfig,
        enrichedSourceConfig,
        failedSourceConfig,
        badSourceConfig,
        nbEnriched,
        nbBad
      )
    }
  }

  private def sourceConfig(
    host: String,
    port: Int,
    topic: String
  ) =
    KafkaSourceConfig(
      topicName = topic,
      bootstrapServers = s"$host:$port",
      consumerConf = Map(
        "group.id" -> "it-enrich",
        "auto.offset.reset" -> "earliest",
        "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "security.protocol" -> "PLAINTEXT",
        "sasl.mechanism" -> "GSSAPI"
      ),
      debounceCommitOffsets = 1.seconds
    )

  private def sinkConfig(
    host: String,
    port: Int,
    topic: String
  ) =
    KafkaSinkConfigM[Id](
      topicName = topic,
      bootstrapServers = s"$host:$port",
      producerConf = Map(
        "acks" -> "all",
        "security.protocol" -> "PLAINTEXT",
        "sasl.mechanism" -> "GSSAPI"
      )
    )
}
