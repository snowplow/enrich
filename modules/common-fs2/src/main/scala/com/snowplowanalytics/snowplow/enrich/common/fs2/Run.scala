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

import fs2.Stream

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
    mkSource: (Blocker, Input, Monitoring) => Stream[F, A],
    mkSinkGood: (Blocker, Output, Monitoring) => Resource[F, AttributedByteSink[F]],
    mkSinkPii: (Blocker, Output, Monitoring) => Resource[F, AttributedByteSink[F]],
    mkSinkBad: (Blocker, Output, Monitoring) => Resource[F, ByteSink[F]],
    checkpoint: List[A] => F[Unit],
    mkClients: List[Blocker => Resource[F, Client[F]]],
    getPayload: A => Array[Byte],
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
                err =>
                  Logger[F]
                    .error(s"CLI arguments valid but some of the configuration is not correct. Error: $err")
                    .as[ExitCode](ExitCode.Error),
                parsed =>
                  for {
                    _ <- Logger[F].info(s"Initialising resources for $name $version")
                    processor = Processor(name, version)
                    file = parsed.configFile
                    sinkGood = initAttributedSink(blocker, file.output.good, file.monitoring, mkSinkGood)
                    sinkPii = file.output.pii.map(out => initAttributedSink(blocker, out, file.monitoring, mkSinkPii))
                    sinkBad = file.output.bad match {
                                case f: Output.FileSystem =>
                                  Sink.fileSink[F](f, blocker)
                                case _ =>
                                  mkSinkBad(blocker, file.output.bad, file.monitoring)
                              }
                    clients = mkClients.map(mk => mk(blocker)).sequence
                    exit <- file.input match {
                              case p: Input.FileSystem =>
                                val env = Environment
                                  .make[F, Array[Byte]](
                                    blocker,
                                    ec,
                                    parsed,
                                    Source.filesystem[F](blocker, p.dir),
                                    sinkGood,
                                    sinkPii,
                                    sinkBad,
                                    clients,
                                    _ => Sync[F].unit,
                                    identity,
                                    processor,
                                    maxRecordSize,
                                    cloud,
                                    getRegion,
                                    file.featureFlags.acceptInvalid
                                  )
                                runEnvironment[F, Array[Byte]](env)
                              case _ =>
                                val env = Environment
                                  .make[F, A](
                                    blocker,
                                    ec,
                                    parsed,
                                    mkSource(blocker, file.input, file.monitoring),
                                    sinkGood,
                                    sinkPii,
                                    sinkBad,
                                    clients,
                                    checkpoint,
                                    getPayload,
                                    processor,
                                    maxRecordSize,
                                    cloud,
                                    getRegion,
                                    file.featureFlags.acceptInvalid
                                  )
                                runEnvironment[F, A](env)
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
    monitoring: Monitoring,
    mkSinkGood: (Blocker, Output, Monitoring) => Resource[F, AttributedByteSink[F]]
  ): Resource[F, AttributedByteSink[F]] =
    output match {
      case f: Output.FileSystem =>
        Sink.fileSink[F](f, blocker).map(sink => row => sink(row.data))
      case _ =>
        mkSinkGood(blocker, output, monitoring)
    }

  private def runEnvironment[F[_]: ConcurrentEffect: ContextShift: Parallel: Timer, A](
    environment: Resource[F, Environment[F, A]]
  ): F[ExitCode] =
    environment.use { env =>
      val log = Logger[F].info("Running enrichment stream")
      val enrich = Enrich.run[F, A](env)
      val updates = Assets.run[F, A](env.blocker, env.semaphore, env.assetsUpdatePeriod, env.assetsState, env.enrichments)
      val telemetry = Telemetry.run[F, A](env)
      val reporting = env.metrics.report
      val metadata = env.metadata.report
      val flow = enrich.merge(updates).merge(reporting).merge(telemetry).merge(metadata)
      log >> flow.compile.drain.as(ExitCode.Success).recoverWith {
        case exception: Throwable =>
          Logger[F].error(s"The Enrich job has stopped") >>
            Sync[F].raiseError[ExitCode](exception)
      }
    }
}
