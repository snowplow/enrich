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

import cats.implicits._

import cats.effect.IO
import cats.effect.kernel.Resource

import org.testcontainers.containers.{BindMode, Network}
import org.testcontainers.containers.wait.strategy.Wait

import com.dimafeng.testcontainers.{FixedHostPortGenericContainer, GenericContainer}

import com.snowplowanalytics.snowplow.enrich.streams.common.Utils._

case class EnrichKafka(
  kafkaConfig: KafkaConfig,
  rawTopic: String,
  enrichedTopic: String,
  failedTopic: String,
  badTopic: String
)

case class KafkaConfig(
  host: String,
  port: Int,
  internalAlias: String,
  internalPort: Int
)

object Containers {
  object Images {
    case class DockerImage(image: String, tag: String) {
      def toStr = s"$image:$tag"
    }
    val Kafka = DockerImage("apache/kafka", "4.0.0")
    val Enrich = DockerImage("snowplow/snowplow-enrich-kafka", s"${BuildInfo.version}-next-distroless")
  }

  def resource: Resource[IO, EnrichKafka] =
    for {
      network <- Resource.fromAutoCloseable(IO(Network.newNetwork()))
      kafkaConfig = KafkaConfig(
                      host = "localhost",
                      port = 9092,
                      internalAlias = "broker",
                      internalPort = 29092
                    )
      enrichKafka = EnrichKafka(
                      kafkaConfig = kafkaConfig,
                      rawTopic = "raw",
                      enrichedTopic = "enriched",
                      failedTopic = "failed",
                      badTopic = "bad"
                    )
      kafka <- kafka(network, kafkaConfig)
      _ <- Resource.eval(createTopics(kafka, enrichKafka))
      _ <- enrich(network, enrichKafka)
    } yield enrichKafka

  private def kafka(
    network: Network,
    kafkaConfig: KafkaConfig
  ): Resource[IO, FixedHostPortGenericContainer] = {
    val container = FixedHostPortGenericContainer(
      imageName = Images.Kafka.toStr,
      env = Map(
        "KAFKA_PROCESS_ROLES" -> "broker,controller",
        "KAFKA_BROKER_ID" -> "1",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP" -> "PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "KAFKA_LISTENERS" -> s"PLAINTEXT://:${kafkaConfig.port},PLAINTEXT_INTERNAL://:${kafkaConfig.internalPort},CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS" -> s"PLAINTEXT://localhost:${kafkaConfig.port},PLAINTEXT_INTERNAL://${kafkaConfig.internalAlias}:${kafkaConfig.internalPort}",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR" -> "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR" -> "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR" -> "1",
        "KAFKA_CONTROLLER_LISTENER_NAMES" -> "CONTROLLER",
        "KAFKA_CONTROLLER_QUORUM_VOTERS" -> "1@broker:9093"
      ),
      waitStrategy = Wait.forLogMessage(".*started.*", 1),
      exposedPorts = Seq(kafkaConfig.port),
      exposedHostPort = kafkaConfig.port,
      exposedContainerPort = kafkaConfig.port
    )
    container.container.withFixedExposedPort(kafkaConfig.port, kafkaConfig.port)
    container.container.withNetwork(network)
    container.container.withNetworkAliases(kafkaConfig.internalAlias)

    Resource.make(IO.blocking(container.container.start()).as(container))(c => IO.blocking(c.stop()))
  }

  def enrich(
    network: Network,
    enrichKafka: EnrichKafka,
    waitLogMessage: String = "Enabled enrichments"
  ): Resource[IO, GenericContainer] =
    for {
      enrichmentsPath <- writeEnrichmentsConfigsToDisk(Nil)
      configPath = "modules/common-streams/kafka/src/it/resources/enrich-kafka.hocon"
      igluResolverPath = "modules/common-streams/core/src/it/resources/iglu_resolver.json"
      container = GenericContainer(
                    dockerImage = Images.Enrich.toStr,
                    env = Map(
                      "JDK_JAVA_OPTIONS" -> "-Dorg.slf4j.simpleLogger.defaultLogLevel=info",
                      "TOPIC_RAW" -> enrichKafka.rawTopic,
                      "TOPIC_ENRICHED" -> enrichKafka.enrichedTopic,
                      "TOPIC_FAILED" -> enrichKafka.failedTopic,
                      "TOPIC_BAD" -> enrichKafka.badTopic,
                      "KAFKA_ENDPOINT" -> s"${enrichKafka.kafkaConfig.internalAlias}:${enrichKafka.kafkaConfig.internalPort}"
                    ),
                    fileSystemBind = Seq(
                      GenericContainer.FileSystemBind(
                        configPath,
                        "/snowplow/config/enrich.hocon",
                        BindMode.READ_ONLY
                      ),
                      GenericContainer.FileSystemBind(
                        igluResolverPath,
                        "/snowplow/config/iglu_resolver.json",
                        BindMode.READ_ONLY
                      ),
                      GenericContainer.FileSystemBind(
                        enrichmentsPath.toAbsolutePath.toString,
                        "/snowplow/config/enrichments/",
                        BindMode.READ_WRITE
                      )
                    ),
                    command = Seq(
                      "--config",
                      "/snowplow/config/enrich.hocon",
                      "--iglu-config",
                      "/snowplow/config/iglu_resolver.json",
                      "--enrichments",
                      "/snowplow/config/enrichments/"
                    ),
                    waitStrategy = Wait.forLogMessage(s".*$waitLogMessage.*", 1)
                  )
      _ <- Resource.eval(IO(container.container.withNetwork(network)))
      enrich <- Resource.make(IO.blocking(container.start()).as(container))(c => IO.blocking(c.stop()))
    } yield enrich

  private def createTopics(
    container: FixedHostPortGenericContainer,
    enrichKafka: EnrichKafka
  ): IO[Unit] =
    List(enrichKafka.rawTopic, enrichKafka.enrichedTopic, enrichKafka.failedTopic, enrichKafka.badTopic).traverse_ { topic =>
      IO.blocking(
        container.execInContainer(
          "kafka-topics",
          "--bootstrap-server",
          s"${enrichKafka.kafkaConfig.host}:${enrichKafka.kafkaConfig.internalPort}",
          "--create",
          "--if-not-exists",
          "--topic",
          topic,
          "--replication-factor",
          "1",
          "--partitions",
          "1"
        )
      )
    }
}
