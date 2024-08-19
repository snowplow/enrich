/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.kinesis

import java.util.UUID

import scala.concurrent.duration._

import org.slf4j.LoggerFactory

import retry.syntax.all._
import retry.RetryPolicies

import cats.implicits._

import cats.effect.{IO, Resource}

import cats.effect.testing.specs2.CatsEffect

import org.testcontainers.containers.{BindMode, Network}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.output.Slf4jLogConsumer

import com.dimafeng.testcontainers.GenericContainer

import com.snowplowanalytics.snowplow.enrich.kinesis.enrichments.{Enrichment, Enrichments}
import com.snowplowanalytics.snowplow.enrich.kinesis.generated.BuildInfo

object Containers extends CatsEffect {

  object Images {
    case class DockerImage(image: String, tag: String) {
      def toStr = s"$image:$tag"
    }
    val Localstack = DockerImage("localstack/localstack", "3.4.0")
    val Enrich = DockerImage("snowplow/snowplow-enrich-kinesis", s"${BuildInfo.version}-distroless")
    val MySQL = DockerImage("mysql", "8.0.31")
    val HTTP = DockerImage("nginx", "1.23.2")
    val Statsd = DockerImage("dblworks/statsd", "v0.10.2") // the official statsd/statsd size is monstrous
  }

  case class Localstack(
    container: GenericContainer,
    alias: String,
    internalPort: Int,
    mappedPort: Int
  )

  private val network = Network.newNetwork()

  def localstack: Resource[IO, Localstack] = Resource.make {
    val port = 4566
    val container = GenericContainer(
      dockerImage = Images.Localstack.toStr,
      env = Map(
        "AWS_ACCESS_KEY_ID" -> "foo",
        "AWS_SECRET_ACCESS_KEY" -> "bar"
      ),
      waitStrategy = Wait.forLogMessage(".*Ready.*", 1),
      exposedPorts = Seq(port)
    )
    container.underlyingUnsafeContainer.withNetwork(network)
    val alias = "localstack"
    container.underlyingUnsafeContainer.withNetworkAliases(alias)

    IO.blocking(container.start()) *>
      IO(
        Localstack(
          container,
          alias,
          port,
          container.container.getMappedPort(port)
        )
      )
  } {
    l => IO.blocking(l.container.stop())
  }

  def enrich(
    localstack: Localstack,
    configPath: String,
    testName: String,
    enrichments: List[Enrichment],
    uuid: String = UUID.randomUUID().toString,
    waitLogMessage: String = "Running Enrich"
  ): Resource[IO, GenericContainer] = {
    val streams = KinesisConfig.getStreams(uuid)

    val container = GenericContainer(
      dockerImage = Images.Enrich.toStr,
      env = Map(
        "AWS_REGION" -> KinesisConfig.region,
        "AWS_ACCESS_KEY_ID" -> "foo",
        "AWS_SECRET_ACCESS_KEY" -> "bar",
        "JDK_JAVA_OPTIONS" -> "-Dorg.slf4j.simpleLogger.defaultLogLevel=info",
        // appName must be unique in enrich config so that Kinesis consumers in tests don't interfere
        "APP_NAME" -> s"${testName}_$uuid",
        "REGION" -> KinesisConfig.region,
        "STREAM_RAW" -> streams.raw,
        "STREAM_ENRICHED" -> streams.enriched,
        "STREAM_BAD" -> streams.bad,
        "STREAM_INCOMPLETE" -> streams.incomplete,
        "LOCALSTACK_ENDPOINT" -> s"http://${localstack.alias}:${localstack.internalPort}"
      ),
      fileSystemBind = Seq(
        GenericContainer.FileSystemBind(
          configPath,
          "/snowplow/config/enrich.hocon",
          BindMode.READ_ONLY
        ),
        GenericContainer.FileSystemBind(
          "modules/kinesis/src/it/resources/enrich/iglu_resolver.json",
          "/snowplow/config/iglu_resolver.json",
          BindMode.READ_ONLY
        )
      ),
      command = Seq(
        "--config",
        "/snowplow/config/enrich.hocon",
        "--iglu-config",
        "/snowplow/config/iglu_resolver.json",
        "--enrichments",
        Enrichments.mkJson(enrichments.map(_.config))
      ),
      waitStrategy = Wait.forLogMessage(s".*$waitLogMessage.*", 1)
    )
    container.container.withNetwork(network)
    Resource.make (
      createStreams(localstack, KinesisConfig.region, streams) *>
        startContainerWithLogs(container, testName)
    )(
      e => IO.blocking(e.stop())
    )
  }

  def mysqlServer: Resource[IO, GenericContainer] = Resource.make {
    val container = GenericContainer(
      dockerImage = Images.MySQL.toStr,
      fileSystemBind = Seq(
        GenericContainer.FileSystemBind(
          "modules/kinesis/src/it/resources/mysql",
          "/docker-entrypoint-initdb.d",
          BindMode.READ_ONLY
        )
      ),
      env = Map(
        "MYSQL_RANDOM_ROOT_PASSWORD" -> "yes",
        "MYSQL_DATABASE" -> "snowplow",
        "MYSQL_USER" -> "enricher",
        "MYSQL_PASSWORD" -> "supersecret1"
      ),
      waitStrategy = Wait.forLogMessage(".*ready for connections.*", 1)
    )
    container.underlyingUnsafeContainer.withNetwork(network)
    container.underlyingUnsafeContainer.withNetworkAliases("mysql")
    IO(container.start()) *> IO.pure(container)
  } {
    c => IO(c.stop())
  }

  def httpServer: Resource[IO, GenericContainer] = Resource.make {
    val container = GenericContainer(
      dockerImage = Images.HTTP.toStr,
      fileSystemBind = Seq(
        GenericContainer.FileSystemBind(
          "modules/kinesis/src/it/resources/nginx/default.conf",
          "/etc/nginx/conf.d/default.conf",
          BindMode.READ_ONLY
        ),
        GenericContainer.FileSystemBind(
          "modules/kinesis/src/it/resources/nginx/.htpasswd",
          "/etc/.htpasswd",
          BindMode.READ_ONLY
        ),
        GenericContainer.FileSystemBind(
          "modules/kinesis/src/it/resources/nginx/www",
          "/usr/share/nginx/html",
          BindMode.READ_ONLY
        )
      ),
      waitStrategy = Wait.forLogMessage(".*start worker processes.*", 1)
    )
    container.underlyingUnsafeContainer.withNetwork(network)
    container.underlyingUnsafeContainer.withNetworkAliases("api")
    IO.blocking(container.start()) *> IO.pure(container)
  } {
    c => IO.blocking(c.stop())
  }

  def statsdServer: Resource[IO, GenericContainer] = Resource.make {
    val container = GenericContainer(Images.Statsd.toStr)
    container.underlyingUnsafeContainer.withNetwork(network)
    container.underlyingUnsafeContainer.withNetworkAliases("statsd")
    container.underlyingUnsafeContainer.addExposedPort(8126)
    IO.blocking(container.start()) *> IO.pure(container)
  } {
    c => IO.blocking(c.stop())
  }

  private def startContainerWithLogs(
    container: GenericContainer,
    loggerName: String
  ): IO[GenericContainer] = {
    val logger = LoggerFactory.getLogger(loggerName)
    val logs = new Slf4jLogConsumer(logger)
    IO.blocking(container.start()) *>
      IO(container.container.followOutput(logs)).as(container)
  }

  def waitUntilStopped(container: GenericContainer): IO[Boolean] = {
    val retryPolicy = RetryPolicies.limitRetriesByCumulativeDelay(
      5.minutes,
      RetryPolicies.capDelay[IO](
        2.second,
        RetryPolicies.fullJitter[IO](1.second)
      )
    )

    IO(container.container.isRunning()).retryingOnFailures(
      _ => IO.pure(false),
      retryPolicy,
      (_, _) => IO.unit
    )
  }

  private def createStreams(
    localstack: Localstack,
    region: String,
    streams: KinesisConfig.Streams
  ): IO[Unit] =
    List(streams.raw, streams.enriched, streams.bad, streams.incomplete).traverse_ { stream =>
      IO.blocking(
        localstack.container.execInContainer(
          "aws",
          s"--endpoint-url=http://127.0.0.1:${localstack.internalPort}",
          "kinesis",
          "create-stream",
          "--stream-name",
          stream,
          "--shard-count",
          "1",
          "--region",
          region
        )
      )
    }
}
