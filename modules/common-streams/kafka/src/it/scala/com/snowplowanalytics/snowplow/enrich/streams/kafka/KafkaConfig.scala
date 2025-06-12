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
package com.snowplowanalytics.snowplow.enrich.streams.kafka

import scala.concurrent.duration._

import cats.Id

import com.snowplowanalytics.snowplow.streams.kafka.{KafkaSinkConfigM, KafkaSourceConfig}

object KafkaConfig {

  def sourceConfig(kafkaPort: Int, kafkaTopic: String) =
    KafkaSourceConfig(
      topicName = kafkaTopic,
      bootstrapServers = getKafkaAddress(kafkaPort),
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

  def sinkConfig(kafkaPort: Int, kafkaTopic: String) =
    KafkaSinkConfigM[Id](
      topicName = kafkaTopic,
      bootstrapServers = getKafkaAddress(kafkaPort),
      producerConf = Map(
        "acks" -> "all",
        "security.protocol" -> "PLAINTEXT",
        "sasl.mechanism" -> "GSSAPI"
      )
    )

  private def getKafkaAddress(kafkaPort: Int): String = s"localhost:$kafkaPort"

  case class Topics(
    raw: String,
    enriched: String,
    failed: String,
    bad: String
  )

  def getTopics: Topics = Topics(raw = "raw", enriched = "enriched", failed = "failed", bad = "bad")
}
