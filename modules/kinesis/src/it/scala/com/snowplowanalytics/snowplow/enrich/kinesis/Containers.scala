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
package com.snowplowanalytics.snowplow.enrich.kinesis

import java.util.UUID

import scala.concurrent.duration._

import org.slf4j.LoggerFactory

import retry.syntax.all._
import retry.RetryPolicies

import cats.effect.{IO, Resource}

import cats.effect.testing.specs2.CatsEffect

import org.testcontainers.containers.{BindMode, GenericContainer => JGenericContainer, Network}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.output.Slf4jLogConsumer

import com.dimafeng.testcontainers.GenericContainer

import com.snowplowanalytics.snowplow.enrich.kinesis.enrichments.{Enrichment, Enrichments}
import com.snowplowanalytics.snowplow.enrich.kinesis.generated.BuildInfo

object Containers extends CatsEffect {

  private val network = Network.newNetwork()

  private val localstackPort = 4566
  private val localstackAlias = "localstack"

  val localstack = {
    val container = GenericContainer(
      dockerImage = "localstack/localstack-light:1.2.0",
      fileSystemBind = Seq(
        GenericContainer.FileSystemBind(
          "modules/kinesis/src/it/resources/localstack",
          "/docker-entrypoint-initaws.d",
          BindMode.READ_ONLY
        )
      ),
      env = Map(
        "AWS_ACCESS_KEY_ID" -> "foo",
        "AWS_SECRET_ACCESS_KEY" -> "bar"
      ),
      waitStrategy = Wait.forLogMessage(".*Ready.*", 1),
      exposedPorts = Seq(localstackPort)
    )
    container.underlyingUnsafeContainer.withNetwork(network)
    container.underlyingUnsafeContainer.withNetworkAliases(localstackAlias)
    container.container
  }

  def localstackMappedPort = localstack.getMappedPort(localstackPort)

  def enrich(
    configPath: String,
    testName: String,
    needsLocalstack: Boolean,
    enrichments: List[Enrichment],
    uuid: String = UUID.randomUUID().toString,
    waitLogMessage: String = "Running Enrich"
  ): Resource[IO, JGenericContainer[_]] = {
    val streams = KinesisConfig.getStreams(uuid)

    val container = GenericContainer(
      dockerImage = s"snowplow/snowplow-enrich-kinesis:${BuildInfo.version}-distroless",
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
        "LOCALSTACK_ENDPOINT" -> s"http://$localstackAlias:$localstackPort"
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
      IO(startLocalstack(needsLocalstack, KinesisConfig.region, streams)) >>
        IO(startContainerWithLogs(container.container, testName))
    )(
      e => IO(e.stop())
    )
  }

  def mysqlServer: Resource[IO, JGenericContainer[_]] = Resource.make {
    val container = GenericContainer(
      dockerImage = "mysql:8.0.31",
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
    IO(container.start()) >> IO.pure(container.container)
  } {
    c => IO(c.stop())
  }

  def httpServer: Resource[IO, JGenericContainer[_]] = Resource.make {
    val container = GenericContainer(
      dockerImage = "nginx:1.23.2",
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
    IO(container.start()) >> IO.pure(container.container)
  } {
    c => IO(c.stop())
  }

  private def startContainerWithLogs(
    container: JGenericContainer[_],
    loggerName: String
  ): JGenericContainer[_] = {
    val logger = LoggerFactory.getLogger(loggerName)
    val logs = new Slf4jLogConsumer(logger)
    container.start()
    container.followOutput(logs)
    container
  }

  def waitUntilStopped(container: JGenericContainer[_]): IO[Boolean] = {
    val retryPolicy = RetryPolicies.limitRetriesByCumulativeDelay(
      5.minutes,
      RetryPolicies.capDelay[IO](
        2.second,
        RetryPolicies.fullJitter[IO](1.second)
      )
    )

    IO(container.isRunning()).retryingOnFailures(
      _ => IO.pure(false),
      retryPolicy,
      (_, _) => IO.unit
    )
  }

  // synchronized so that start() isn't called by several threads at the same time.
  // start() is blocking.
  // Calling start() on an already started container has no effect.
  private def startLocalstack(
    needsLocalstack: Boolean,
    region: String,
    streams: KinesisConfig.Streams
  ): Unit = synchronized {
    if(needsLocalstack) {
      localstack.start()
      createStreams(
        localstack,
        localstackPort,
        region,
        streams
      )
    } else ()
  }

  private def createStreams(
    localstack: JGenericContainer[_],
    port: Int,
    region: String,
    streams: KinesisConfig.Streams
  ): Unit =
    List(streams.raw, streams.enriched, streams.bad, streams.incomplete).foreach { stream =>
      localstack.execInContainer(
        "aws",
        s"--endpoint-url=http://127.0.0.1:$port",
        "kinesis",
        "create-stream",
        "--stream-name",
        stream,
        "--shard-count",
        "1",
        "--region",
        region
      )
    }
}
