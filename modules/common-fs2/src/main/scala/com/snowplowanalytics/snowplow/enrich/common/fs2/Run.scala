/*
 * Copyright (c) 2021-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.fs2

import cats.Parallel
import cats.implicits._

import _root_.io.sentry.SentryClient

import fs2.{Pipe, Stream}

import scala.concurrent.ExecutionContext

import cats.effect.{Blocker, Clock, Concurrent, ConcurrentEffect, ContextShift, ExitCode, Resource, Sync, Timer}

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.badrows.Processor

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Input, Monitoring, Output}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.{CliConfig, ParsedConfigs}
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.{Sink, Source}
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Clients.Client

object Run {
  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def run[F[_]: Clock: ConcurrentEffect: ContextShift: Parallel: Timer, A](
    args: List[String],
    name: String,
    version: String,
    description: String,
    ec: ExecutionContext,
    updateCliConfig: (Blocker, CliConfig) => F[CliConfig],
    mkSource: (Blocker, Input, Option[Monitoring]) => Resource[F, (Stream[F, A], Pipe[F, A, Unit])],
    mkGoodSink: (Blocker, Output, Option[Monitoring]) => Resource[F, AttributedByteSink[F]],
    mkPiiSink: (Blocker, Output, Option[Monitoring]) => Resource[F, AttributedByteSink[F]],
    mkBadSink: (Blocker, Output, Option[Monitoring]) => Resource[F, ByteSink[F]],
    mkClients: List[Blocker => Client[F]],
    getPayload: A => Array[Byte],
    ordered: Boolean,
    maxRecordSize: Int,
    cloud: Option[Telemetry.Cloud],
    getRegion: => Option[String]
  ): F[ExitCode] =
    CliConfig.command(name, version, description).parse(args) match {
      case Right(cli) =>
        Blocker[F].use { blocker =>
          updateCliConfig(blocker, cli).flatMap { cfg =>
            ParsedConfigs
              .parse[F](cfg)
              .fold(
                err => Logger[F].error(s"CLI arguments valid but some of the configuration is not correct. Error: $err").as[ExitCode](ExitCode.Error),
                parsed =>
                  for {
                    _ <- Logger[F].info(s"Initialising resources for $name $version")
                    processor = Processor(name, version)
                    file = parsed.configFile
                    goodSink = initAttributedSink(blocker, file.output.good, file.monitoring, mkGoodSink)
                    piiSink = file.output.pii.map(out => initAttributedSink(blocker, out, file.monitoring, mkPiiSink))
                    badSink = file.output.bad match {
                                case f: Output.FileSystem =>
                                  Sink.fileSink[F](f, blocker)
                                case _ =>
                                  mkBadSink(blocker, file.output.bad, file.monitoring)
                              }
                    clients = mkClients.map(mk => mk(blocker))
                    exit <- file.input match {
                              case p: Input.FileSystem =>
                                Source.filesystem[F](blocker, p.dir).use {
                                  case (source, checkpointer) =>
                                    val env = Environment
                                      .make[F, Array[Byte]](
                                        blocker,
                                        ec,
                                        parsed,
                                        source,
                                        goodSink,
                                        piiSink,
                                        badSink,
                                        clients,
                                        checkpointer,
                                        identity,
                                        processor,
                                        maxRecordSize,
                                        cloud,
                                        getRegion
                                      )
                                    runEnvironment[F, Array[Byte]](env, false)
                                }
                              case _ =>
                                mkSource(blocker, file.input, file.monitoring).use {
                                  case (source, checkpointer) =>
                                    val env = Environment
                                      .make[F, A](
                                        blocker,
                                        ec,
                                        parsed,
                                        source,
                                        goodSink,
                                        piiSink,
                                        badSink,
                                        clients,
                                        checkpointer,
                                        getPayload,
                                        processor,
                                        maxRecordSize,
                                        cloud,
                                        getRegion
                                      )
                                    runEnvironment[F, A](env, ordered)
                                }
                            }
                  } yield exit
              )
              .flatten
            }
        }
      case Left(error) =>
        Logger[F].error(s"CLI arguments are invalid. Error: $error") >> Sync[F].pure(ExitCode.Error)
    }

  private def initAttributedSink[F[_]: Concurrent: ContextShift: Timer](
    blocker: Blocker,
    output: Output,
    monitoring: Option[Monitoring],
    mkGoodSink: (Blocker, Output, Option[Monitoring]) => Resource[F, AttributedByteSink[F]]
  ): Resource[F, AttributedByteSink[F]] =
    output match {
      case f: Output.FileSystem =>
        Sink.fileSink[F](f, blocker).map(sink => row => sink(row.data))
      case _ =>
        mkGoodSink(blocker, output, monitoring)
    }

  private def runEnvironment[F[_]: ConcurrentEffect: ContextShift: Parallel: Timer, A](
    environment: Resource[F, Environment[F, A]],
    ordered: Boolean
  ): F[ExitCode] =
    environment.use { env =>
      val log = Logger[F].info("Running enrichment stream")
      val enrich = Enrich.run[F, A](env, ordered)
      val updates = Assets.run[F, A](env)
      val telemetry = Telemetry.run[F, A](env)
      val reporting = env.metrics.report
      val flow = enrich.merge(updates).merge(reporting).merge(telemetry)
      log >> flow.compile.drain.as(ExitCode.Success).recoverWith {
        case exception: Throwable =>
          sendToSentry(exception, env.sentry) >>
            Logger[F].error(s"The Enrich job has stopped") >>
            Sync[F].raiseError[ExitCode](exception)
      }
    }

  private def sendToSentry[F[_]: Sync](error: Throwable, sentry: Option[SentryClient]): F[Unit] =
    sentry match {
      case Some(client) =>
        Sync[F].delay(client.sendException(error)) >> Logger[F].info("Sentry report has been sent")
      case None =>
        Sync[F].unit
    }
}
