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

import retry.syntax.all._
import retry.RetryPolicies

import cats.implicits._

import cats.effect.{IO, Resource}

import cats.effect.testing.specs2.CatsEffect

import org.testcontainers.containers.{BindMode, Network}
import org.testcontainers.containers.wait.strategy.Wait

import com.dimafeng.testcontainers.GenericContainer

import com.snowplowanalytics.snowplow.enrich.core.Enrichment
import com.snowplowanalytics.snowplow.enrich.core.Utils._

case class EnrichKinesis(
  localstack: Localstack,
  rawStream: String,
  enrichedStream: String,
  failedStream: String,
  badStream: String,
  container: GenericContainer
)

case class Localstack(
  container: GenericContainer,
  alias: String,
  internalPort: Int,
  mappedPort: Int,
  host: String
)

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

  def localstack: Resource[IO, Localstack] =
    for {
      network <- Resource.fromAutoCloseable(IO(Network.newNetwork()))
      internalPort = 4566
      alias = "localstack"
      container = GenericContainer(
                    dockerImage = Images.Localstack.toStr,
                    env = Map(
                      "AWS_ACCESS_KEY_ID" -> "foo",
                      "AWS_SECRET_ACCESS_KEY" -> "bar"
                    ),
                    waitStrategy = Wait.forLogMessage(".*Ready.*", 1),
                    exposedPorts = Seq(internalPort)
                  )
      _ <- Resource.eval(IO(container.container.withNetwork(network)))
      _ <- Resource.eval(IO(container.container.withNetworkAliases(alias)))
      _ <- Resource.make(IO.blocking(container.start()).as(container))(c => IO.blocking(c.stop()))
      localstack = Localstack(
                     container = container,
                     alias = alias,
                     internalPort = internalPort,
                     mappedPort = container.container.getMappedPort(internalPort),
                     host = "localhost"
                   )
    } yield localstack

  def enrich(
    localstack: Localstack,
    configPath: String,
    testName: String,
    enrichments: List[Enrichment],
    uuid: String = UUID.randomUUID().toString,
    waitLogMessage: String = "Enabled enrichments"
  ): Resource[IO, EnrichKinesis] =
    for {
      enrichmentsPath <- writeEnrichmentsConfigsToDisk(enrichments)
      igluResolverPath = "modules/it/core/src/test/resources/iglu_resolver.json"
      region = "eu-central-1"
      rawStream = s"raw-$uuid"
      enrichedStream = s"enriched-$uuid"
      failedStream = s"failed-$uuid"
      badStream = s"bad-$uuid"
      container = GenericContainer(
                    dockerImage = Images.Enrich.toStr,
                    env = Map(
                      "AWS_REGION" -> region,
                      "AWS_ACCESS_KEY_ID" -> "foo",
                      "AWS_SECRET_ACCESS_KEY" -> "bar",
                      "JDK_JAVA_OPTIONS" -> "-Dorg.slf4j.simpleLogger.defaultLogLevel=info",
                      // appName must be unique in enrich config so that Kinesis consumers in tests don't interfere
                      "APP_NAME" -> s"${testName}_$uuid",
                      "STREAM_RAW" -> rawStream,
                      "STREAM_ENRICHED" -> enrichedStream,
                      "STREAM_FAILED" -> failedStream,
                      "STREAM_BAD" -> badStream,
                      "LOCALSTACK_ENDPOINT" -> s"http://${localstack.alias}:${localstack.internalPort}",
                      "STATSD_TAG" -> uuid
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
      _ <- Resource.eval(IO(container.container.withNetwork(localstack.container.network)))
      _ <- Resource.eval(createStreams(localstack, region, List(rawStream, enrichedStream, failedStream, badStream)))
      _ <- Resource.make(IO.blocking(container.start()).as(container))(c => IO.blocking(c.stop()))
      enrichKinesis = EnrichKinesis(
                        localstack,
                        rawStream,
                        enrichedStream,
                        failedStream,
                        badStream,
                        container
                      )
    } yield enrichKinesis

  def mysqlServer(network: Network): Resource[IO, GenericContainer] =
    Resource.make {
      val container = GenericContainer(
        dockerImage = Images.MySQL.toStr,
        fileSystemBind = Seq(
          GenericContainer.FileSystemBind(
            "modules/it/kinesis/src/test/resources/mysql",
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
    } { c =>
      IO(c.stop())
    }

  def httpServer(network: Network): Resource[IO, GenericContainer] =
    Resource.make {
      val container = GenericContainer(
        dockerImage = Images.HTTP.toStr,
        fileSystemBind = Seq(
          GenericContainer.FileSystemBind(
            "modules/it/kinesis/src/test/resources/nginx/default.conf",
            "/etc/nginx/conf.d/default.conf",
            BindMode.READ_ONLY
          ),
          GenericContainer.FileSystemBind(
            "modules/it/kinesis/src/test/resources/nginx/.htpasswd",
            "/etc/.htpasswd",
            BindMode.READ_ONLY
          ),
          GenericContainer.FileSystemBind(
            "modules/it/kinesis/src/test/resources/nginx/www",
            "/usr/share/nginx/html",
            BindMode.READ_ONLY
          )
        ),
        waitStrategy = Wait.forLogMessage(".*start worker processes.*", 1)
      )
      container.underlyingUnsafeContainer.withNetwork(network)
      container.underlyingUnsafeContainer.withNetworkAliases("api")
      IO.blocking(container.start()) *> IO.pure(container)
    } { c =>
      IO.blocking(c.stop())
    }

  def statsdServer(network: Network): Resource[IO, GenericContainer] =
    Resource.make {
      val container = GenericContainer(Images.Statsd.toStr)
      container.underlyingUnsafeContainer.withNetwork(network)
      container.underlyingUnsafeContainer.withNetworkAliases("statsd")
      container.underlyingUnsafeContainer.addExposedPort(8126)
      IO.blocking(container.start()) *> IO.pure(container)
    } { c =>
      IO.blocking(c.stop())
    }

  def waitUntilStopped(container: GenericContainer): IO[Boolean] = {
    val retryPolicy = RetryPolicies.limitRetriesByCumulativeDelay(
      2.minutes,
      RetryPolicies.constantDelay[IO](1.second)
    )

    IO(container.container.isRunning()).retryingOnFailures(
      running => IO.pure(running == false),
      retryPolicy,
      (_, _) => IO.unit
    )
  }

  private def createStreams(
    localstack: Localstack,
    region: String,
    streams: List[String]
  ): IO[Unit] =
    streams.traverse_ { stream =>
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
