/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.kinesis

import java.util.UUID

import scala.concurrent.duration._

import org.slf4j.LoggerFactory

import retry.syntax.all._
import retry.{RetryPolicies, Sleep}

import cats.syntax.flatMap._

import cats.effect.{Resource, Sync}

import org.testcontainers.containers.{BindMode, GenericContainer => JGenericContainer, Network}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.output.Slf4jLogConsumer

import com.dimafeng.testcontainers.GenericContainer

object Containers {

  private val network = Network.newNetwork()

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
      waitStrategy = Wait.forLogMessage(".*AWS kinesis.CreateStream.*", 3),
      exposedPorts = Seq(4566)
    )
    container.underlyingUnsafeContainer.withNetwork(network)
    container.underlyingUnsafeContainer.withNetworkAliases("localstack")
    container.container
  }

  def localStackPort = localstack.getMappedPort(4566)

  def enrich[F[_]: Sync](
    configPath: String,
    loggerName: String,
    needsLocalstack: Boolean,
    waitLogMessage: String = "Running Enrich"
  ): Resource[F, JGenericContainer[_]] = {
    val container = GenericContainer(
      dockerImage = "snowplow/snowplow-enrich-kinesis:latest",
      env = Map(
        "AWS_REGION" -> "eu-central-1",
        "AWS_ACCESS_KEY_ID" -> "foo",
        "AWS_SECRET_ACCESS_KEY" -> "bar",
        "JAVA_OPTS" -> "-Dorg.slf4j.simpleLogger.defaultLogLevel=info",
        // appName must be unique in enrich config so that Kinesis consumers in tests don't interfere
        "APP_NAME" -> UUID.randomUUID().toString
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
        ),
        GenericContainer.FileSystemBind(
          "modules/kinesis/src/it/resources/enrich/enrichments",
          "/snowplow/config/enrichments",
          BindMode.READ_ONLY
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
    container.container.withNetwork(network)
    Resource.make (
      Sync[F].delay(startLocalstack[F](needsLocalstack)) >>
        Sync[F].delay(startContainerWithLogs(container.container, loggerName))
    )(
      e => Sync[F].delay(e.stop())
    )
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

  def waitUntilStopped[F[_]: Sleep: Sync](container: JGenericContainer[_]): F[Boolean] = {
    val retryPolicy = RetryPolicies.limitRetriesByCumulativeDelay(
      5.minutes,
      RetryPolicies.capDelay[F](
        2.second,
        RetryPolicies.fullJitter[F](1.second)
      )
    )

    Sync[F].delay(container.isRunning()).retryingOnFailures(
      _ == false,
      retryPolicy,
      (_, _) => Sync[F].unit
    )
  }

  // synchronized so that start() isn't called by several threads at the same time.
  // start() is blocking.
  // Calling start() on an already started container has no effect.
  private def startLocalstack[F[_]: Sync](needsLocalstack: Boolean): Unit = synchronized {
    if(needsLocalstack)
      localstack.start()
    else
      ()
  }
}
