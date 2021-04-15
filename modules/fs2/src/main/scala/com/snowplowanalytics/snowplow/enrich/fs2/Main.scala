/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.fs2

import cats.syntax.flatMap._
import cats.effect.{ExitCode, IO, IOApp, Resource, SyncIO}

import _root_.io.sentry.SentryClient

import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.enrich.fs2.io.Metrics

import java.util.concurrent.{Executors, TimeUnit}

import scala.concurrent.ExecutionContext

object Main extends IOApp.WithContext {

  private implicit val logger: Logger[IO] =
    Slf4jLogger.getLogger[IO]

  /**
   * An execution context matching the cats effect IOApp default. We create it explicitly so we can
   * also use it for our Blaze client.
   */
  override protected val executionContextResource: Resource[SyncIO, ExecutionContext] = {
    val poolSize = math.max(2, Runtime.getRuntime().availableProcessors())
    Resource
      .make(SyncIO(Executors.newFixedThreadPool(poolSize)))(pool =>
        SyncIO {
          pool.shutdown()
          pool.awaitTermination(10, TimeUnit.SECONDS)
          ()
        }
      )
      .map(ExecutionContext.fromExecutorService)
  }

  def run(args: List[String]): IO[ExitCode] =
    config.CliConfig.command.parse(args) match {
      case Right(cfg) =>
        for {
          _ <- logger.info("Initialising resources for Enrich job")
          environment <- Environment.make[IO](cfg, executionContext).value
          exit <- environment match {
                    case Right(e) =>
                      e.use { env =>
                        val log = logger.info("Running enrichment stream")
                        val enrich = Enrich.run[IO](env)
                        val updates = Assets.run[IO](env)
                        val reporting = Metrics.run[IO](env)
                        val flow = enrich.merge(updates).merge(reporting)
                        log >> flow.compile.drain.attempt.flatMap {
                          case Left(exception) =>
                            unsafeSendSentry(exception, env.sentry)
                            IO.raiseError[ExitCode](exception).as(ExitCode.Error)
                          case Right(_) =>
                            IO.pure(ExitCode.Success)
                        }
                      }
                    case Left(error) =>
                      logger.error(s"Cannot initialise enrichment resources\n$error").as(ExitCode.Error)
                  }
        } yield exit
      case Left(error) =>
        IO(System.err.println(error)).as(ExitCode.Error)
    }

  /** Last attempt to notify about an exception (possibly just interruption) */
  private def unsafeSendSentry(error: Throwable, sentry: Option[SentryClient]): Unit = {
    sentry match {
      case Some(client) =>
        client.sendException(error)
      case None => ()
    }
    logger.error(s"The Enrich job has stopped ${sentry.fold("")(_ => "Sentry report has been sent")}").unsafeRunSync()
  }
}
