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
package com.snowplowanalytics.snowplow.enrich.nsq

import cats.effect.IO
import cats.effect.kernel.Resource

import org.http4s.client.{JavaNetClientBuilder, Client => Http4sClient}
import org.http4s.{Method, Request, Uri}

import org.testcontainers.containers.{BindMode, Network}
import org.testcontainers.containers.wait.strategy.Wait

import com.dimafeng.testcontainers.{FixedHostPortGenericContainer, GenericContainer}

import com.snowplowanalytics.snowplow.enrich.core.Utils._

case class EnrichNsq(
  nsqConfig: NsqConfig,
  rawTopic: String,
  enrichedTopic: String,
  failedTopic: String,
  badTopic: String
)

case class NsqConfig(
  lookup1: NetworkInfo,
  nsqd1: NetworkInfo,
  lookup2: NetworkInfo,
  nsqd2: NetworkInfo
)

case class NetworkInfo(
  networkAlias: String,
  broadcastAddress: String,
  httpPort: Int,
  tcpPort: Int
)

object Containers {
  object Images {
    case class DockerImage(image: String, tag: String) {
      def toStr = s"$image:$tag"
    }
    val Nsq = DockerImage("nsqio/nsq", "v1.2.1")
    val Enrich = DockerImage("snowplow/snowplow-enrich-nsq", s"${BuildInfo.version}-distroless")
  }

  /**
   * Integration test tries to access nsqd from outside of docker network
   * therefore it can't access it with docker service name.
   * Since nsqd broadcasts its address through nsqlookup, it is impossible to
   * broadcast its address as localhost and docker service name at the same time
   * therefore we've created second nsqd and nsqlookup instances that will be used by
   * integration tests and NSQ messages sent to first nsqd instance will be replicated
   * to second nsqd instance with nsq_to_nsq tool.
   */
  def resource: Resource[IO, EnrichNsq] =
    for {
      network <- Resource.fromAutoCloseable(IO(Network.newNetwork()))
      nsqConfig = NsqConfig(
                    lookup1 = NetworkInfo(networkAlias = "nsqlookupd", broadcastAddress = "nsqlookupd", httpPort = 4161, tcpPort = 4160),
                    lookup2 = NetworkInfo(networkAlias = "nsqlookupd2", broadcastAddress = "nsqlookupd2", httpPort = 4261, tcpPort = 4260),
                    nsqd1 = NetworkInfo(networkAlias = "nsqd", broadcastAddress = "nsqd", httpPort = 4151, tcpPort = 4150),
                    nsqd2 = NetworkInfo(networkAlias = "nsqd2", broadcastAddress = "127.0.0.1", httpPort = 4251, tcpPort = 4250)
                  )
      enrichNsq = EnrichNsq(
                    nsqConfig = nsqConfig,
                    rawTopic = "Raw",
                    enrichedTopic = "Enriched",
                    failedTopic = "Failed",
                    badTopic = "Bad"
                  )
      _ <- nsqlookupd(network, nsqConfig.lookup1)
      _ <- nsqlookupd(network, nsqConfig.lookup2)
      _ <- nsqd(
             network,
             nsqConfig.nsqd1,
             lookupAddress = s"${nsqConfig.lookup1.networkAlias}:${nsqConfig.lookup1.tcpPort}"
           )
      _ <- nsqd(
             network,
             nsqConfig.nsqd2,
             lookupAddress = s"${nsqConfig.lookup2.networkAlias}:${nsqConfig.lookup2.tcpPort}"
           )
      _ <- Resource.eval(createTopics(enrichNsq))
      _ <- nsqToNsq(
             network,
             sourceAddress = s"${nsqConfig.nsqd1.networkAlias}:${nsqConfig.nsqd1.tcpPort}",
             destinationAddress = s"${nsqConfig.nsqd2.networkAlias}:${nsqConfig.nsqd2.tcpPort}",
             sourceTopic = enrichNsq.enrichedTopic,
             destinationTopic = enrichNsq.enrichedTopic
           )
      _ <- nsqToNsq(
             network,
             sourceAddress = s"${nsqConfig.nsqd1.networkAlias}:${nsqConfig.nsqd1.tcpPort}",
             destinationAddress = s"${nsqConfig.nsqd2.networkAlias}:${nsqConfig.nsqd2.tcpPort}",
             sourceTopic = enrichNsq.failedTopic,
             destinationTopic = enrichNsq.failedTopic
           )
      _ <- nsqToNsq(
             network,
             sourceAddress = s"${nsqConfig.nsqd1.networkAlias}:${nsqConfig.nsqd1.tcpPort}",
             destinationAddress = s"${nsqConfig.nsqd2.networkAlias}:${nsqConfig.nsqd2.tcpPort}",
             sourceTopic = enrichNsq.badTopic,
             destinationTopic = enrichNsq.badTopic
           )
      _ <- enrich(network, enrichNsq)
    } yield enrichNsq

  private def nsqlookupd(
    network: Network,
    networkInfo: NetworkInfo
  ): Resource[IO, FixedHostPortGenericContainer] = {
    val container = FixedHostPortGenericContainer(
      imageName = Images.Nsq.toStr,
      command = Seq(
        "/nsqlookupd",
        s"--broadcast-address=${networkInfo.broadcastAddress}",
        s"--http-address=0.0.0.0:${networkInfo.httpPort}",
        s"--tcp-address=0.0.0.0:${networkInfo.tcpPort}"
      ),
      exposedPorts = List(networkInfo.httpPort, networkInfo.tcpPort),
      exposedContainerPort = networkInfo.httpPort,
      exposedHostPort = networkInfo.httpPort
    )
    container.container.withFixedExposedPort(networkInfo.tcpPort, networkInfo.tcpPort)
    container.container.withNetwork(network)
    container.container.withNetworkAliases(networkInfo.networkAlias)

    Resource.make(IO.blocking(container.container.start()).as(container))(c => IO.blocking(c.stop()))
  }

  private def nsqd(
    network: Network,
    networkInfo: NetworkInfo,
    lookupAddress: String
  ): Resource[IO, FixedHostPortGenericContainer] = {
    val container = FixedHostPortGenericContainer(
      imageName = Images.Nsq.toStr,
      command = Seq(
        "/nsqd",
        s"--broadcast-address=${networkInfo.broadcastAddress}",
        s"--broadcast-http-port=${networkInfo.httpPort}",
        s"--broadcast-tcp-port=${networkInfo.tcpPort}",
        s"--http-address=0.0.0.0:${networkInfo.httpPort}",
        s"--tcp-address=0.0.0.0:${networkInfo.tcpPort}",
        s"--lookupd-tcp-address=$lookupAddress"
      ),
      exposedPorts = List(networkInfo.httpPort, networkInfo.tcpPort),
      exposedContainerPort = networkInfo.httpPort,
      exposedHostPort = networkInfo.httpPort
    )
    container.container.withFixedExposedPort(networkInfo.tcpPort, networkInfo.tcpPort)
    container.container.withNetwork(network)
    container.container.withNetworkAliases(networkInfo.networkAlias)

    Resource.make(IO.blocking(container.start()).as(container))(c => IO.blocking(c.stop()))
  }

  private def nsqToNsq(
    network: Network,
    sourceAddress: String,
    sourceTopic: String,
    destinationAddress: String,
    destinationTopic: String
  ): Resource[IO, GenericContainer] = {
    val container = GenericContainer(
      dockerImage = Images.Nsq.toStr,
      command = Seq(
        "/nsq_to_nsq",
        s"--nsqd-tcp-address=$sourceAddress",
        s"--topic=$sourceTopic",
        s"--destination-nsqd-tcp-address=$destinationAddress",
        s"--destination-topic=$destinationTopic"
      )
    )
    container.container.withNetwork(network)

    Resource.make(IO.blocking(container.start()).as(container))(c => IO.blocking(c.stop()))
  }

  private def enrich(
    network: Network,
    enrichNsq: EnrichNsq,
    waitLogMessage: String = "Enabled enrichments"
  ): Resource[IO, GenericContainer] =
    for {
      enrichmentsPath <- writeEnrichmentsConfigsToDisk(Nil)
      configPath = "modules/nsq/src/it/resources/enrich-nsq.hocon"
      igluResolverPath = "modules/core/src/it/resources/iglu_resolver.json"
      container = GenericContainer(
                    dockerImage = Images.Enrich.toStr,
                    env = Map(
                      "JDK_JAVA_OPTIONS" -> "-Dorg.slf4j.simpleLogger.defaultLogLevel=info",
                      "INPUT_TOPIC" -> enrichNsq.rawTopic,
                      "LOOKUP_HOST" -> enrichNsq.nsqConfig.lookup1.networkAlias,
                      "LOOKUP_PORT" -> enrichNsq.nsqConfig.lookup1.httpPort.toString,
                      "GOOD_OUTPUT_TOPIC" -> enrichNsq.enrichedTopic,
                      "FAILED_OUTPUT_TOPIC" -> enrichNsq.failedTopic,
                      "BAD_OUTPUT_TOPIC" -> enrichNsq.badTopic,
                      "NSQD_HOST" -> enrichNsq.nsqConfig.nsqd1.networkAlias,
                      "NSQD_PORT" -> enrichNsq.nsqConfig.nsqd1.tcpPort.toString
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
                      "/snowplow/config/enrichments"
                    ),
                    waitStrategy = Wait.forLogMessage(s".*$waitLogMessage.*", 1)
                  )
      _ <- Resource.eval(IO(container.container.withNetwork(network)))
      enrich <- Resource.make(IO.blocking(container.start()).as(container))(c => IO.blocking(c.stop()))
    } yield enrich

  private def createTopics(enrichNsq: EnrichNsq): IO[Unit] = {
    val client = JavaNetClientBuilder[IO].create
    for {
      _ <- createTopic(client, enrichNsq.rawTopic, enrichNsq.nsqConfig.nsqd1.httpPort)
      _ <- createTopic(client, enrichNsq.enrichedTopic, enrichNsq.nsqConfig.nsqd1.httpPort)
      _ <- createTopic(client, enrichNsq.failedTopic, enrichNsq.nsqConfig.nsqd1.httpPort)
      _ <- createTopic(client, enrichNsq.badTopic, enrichNsq.nsqConfig.nsqd1.httpPort)
      _ <- createTopic(client, enrichNsq.enrichedTopic, enrichNsq.nsqConfig.nsqd2.httpPort)
      _ <- createTopic(client, enrichNsq.failedTopic, enrichNsq.nsqConfig.nsqd2.httpPort)
      _ <- createTopic(client, enrichNsq.badTopic, enrichNsq.nsqConfig.nsqd2.httpPort)
    } yield ()
  }

  private def createTopic(
    client: Http4sClient[IO],
    topic: String,
    port: Int
  ): IO[Unit] = {
    val request = Request[IO](
      method = Method.POST,
      Uri.unsafeFromString(s"http://127.0.0.1:$port/topic/create?topic=$topic")
    )
    client.expect[Unit](request)
  }
}
