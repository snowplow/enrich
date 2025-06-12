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

import com.dimafeng.testcontainers.{FixedHostPortGenericContainer, GenericContainer}

import org.testcontainers.containers.{BindMode, Network}
import org.testcontainers.containers.wait.strategy.Wait

import com.snowplowanalytics.snowplow.enrich.streams.common.Utils

case class Resources(kafka: Resources.KafkaContainer, network: Network)

object Resources {
  object Images {
    case class DockerImage(image: String, tag: String) {
      def toStr = s"$image:$tag"
    }
    val Kafka = DockerImage("apache/kafka", "4.0.0")
    val Enrich = DockerImage("snowplow/snowplow-enrich-kafka", s"${BuildInfo.version}-next-distroless")
  }

  case class KafkaContainer(
    containerStop: IO[Unit],
    alias: String,
    internalPort: Int,
    externalPort: Int
  )

  def createResources: Resource[IO, Resources] =
    for {
      network <- Resource.make(IO(Network.newNetwork()))(network => IO(network.close()))
      kafka <- createKafka(network)
    } yield Resources(kafka, network)

  def createEnrich(
    network: Network,
    kafkaContainer: KafkaContainer,
    waitLogMessage: String = "Enabled enrichments"
  ): Resource[IO, GenericContainer] =
    Utils.writeEnrichmentsConfigsToDisk(Nil).flatMap { enrichmentsPath =>
      Resource.make {
        val configPath = "modules/common-streams/kafka/src/it/resources/enrich-kafka.hocon"
        val igluResolverPath = "modules/common-streams/kafka/src/it/resources/iglu_resolver.json"
        val topics = KafkaConfig.getTopics
        val container = GenericContainer(
          dockerImage = Images.Enrich.toStr,
          env = Map(
            "TOPIC_RAW" -> topics.raw,
            "TOPIC_ENRICHED" -> topics.enriched,
            "TOPIC_FAILED" -> topics.failed,
            "TOPIC_BAD" -> topics.bad,
            "KAFKA_ENDPOINT" -> s"${kafkaContainer.alias}:${kafkaContainer.internalPort}"
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
        container.underlyingUnsafeContainer.withNetwork(network)

        IO.blocking(container.start()) *> IO(container)
      } { container => IO(container.stop()) }
    }

  private def createKafka(network: Network): Resource[IO, KafkaContainer] = {
    Resource.make {
      val alias = "broker"
      val externalPort = 9092
      val internalPort = 29092
      val container = FixedHostPortGenericContainer(
        imageName = Images.Kafka.toStr,
        env = Map(
          "KAFKA_PROCESS_ROLES" -> "broker,controller",
          "KAFKA_BROKER_ID" -> "1",
          "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP" -> "PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
          "KAFKA_LISTENERS" -> s"PLAINTEXT://:$externalPort,PLAINTEXT_INTERNAL://:$internalPort,CONTROLLER://:9093",
          "KAFKA_ADVERTISED_LISTENERS" -> s"PLAINTEXT://localhost:$externalPort,PLAINTEXT_INTERNAL://$alias:$internalPort",
          "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR" -> "1",
          "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR" -> "1",
          "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR" -> "1",
          "KAFKA_CONTROLLER_LISTENER_NAMES" -> "CONTROLLER",
          "KAFKA_CONTROLLER_QUORUM_VOTERS" -> "1@broker:9093"
        ),
        waitStrategy = Wait.forLogMessage(".*started.*", 1),
        exposedPorts = Seq(externalPort),
        exposedHostPort = externalPort,
        exposedContainerPort = externalPort
      )
      container.container.withFixedExposedPort(externalPort, externalPort)
      container.underlyingUnsafeContainer.withNetwork(network)
      container.underlyingUnsafeContainer.withNetworkAliases(alias)

      IO.blocking(container.start()) *>
        createTopics(container, s"localhost:$internalPort", KafkaConfig.getTopics) *>
        IO(
          KafkaContainer(
            IO(container.stop()),
            alias,
            internalPort,
            externalPort
          )
        )
    } { _.containerStop }
  }

  private def createTopics(
    container: FixedHostPortGenericContainer,
    kafkaAddress: String,
    topics: KafkaConfig.Topics
  ): IO[Unit] =
    List(topics.raw, topics.enriched, topics.failed, topics.bad).traverse_ { topic =>
      IO(
        container.execInContainer(
          "kafka-topics",
          "--bootstrap-server",
          kafkaAddress,
          "--create" ,
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
