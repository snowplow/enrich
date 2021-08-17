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

import cats.effect.{Blocker, Clock, Concurrent, ContextShift, ConcurrentEffect, ExitCode, Resource, Sync, Timer}

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.badrows.Processor

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Authentication, Input, Output}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.{CliConfig, ParsedConfigs}
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.{Sink, Source}

object Run {
  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def run[F[_]: Clock: ConcurrentEffect: ContextShift: Parallel: Timer, A](
    args: List[String],
    name: String,
    version: String,
    description: String,
    ec: ExecutionContext,
    mkSource: (Blocker, Authentication, Input) => Stream[F, A],
    mkGoodSink: (Blocker, Authentication, Output) => Resource[F, AttributedByteSink[F]],
    mkPiiSink: (Blocker, Authentication, Output) => Resource[F, AttributedByteSink[F]],
    mkBadSink: (Blocker, Authentication, Output) => Resource[F, ByteSink[F]],
    checkpointer: Pipe[F, A, Unit],
    getPayload: A => Array[Byte],
    ordered: Boolean
  ): F[ExitCode] = 
    CliConfig.command(name, version, description).parse(args) match {
      case Right(cfg) =>

        ParsedConfigs.parse[F](cfg).fold (
          err => Sync[F].delay(System.err.println(err)).as[ExitCode](ExitCode.Error),
          parsed => 
            Blocker[F].use { blocker =>
              for {
                _ <- Logger[F].info(s"Initialising resources for $name $version")
                processor = Processor(name, version)
                file = parsed.configFile
                goodSink = initAttributedSink(blocker, file.auth, file.good, mkGoodSink)
                piiSink = file.pii.map(out => initAttributedSink(blocker, file.auth, out, mkPiiSink))
                badSink = file.bad match {
                  case Output.FileSystem(path) =>
                    Sink.fileSink[F](path, blocker)
                  case _ =>
                    mkBadSink(blocker, file.auth, file.bad)
                }
                exit <- 
                  (file.auth, file.input) match {
                    case (_, p: Input.FileSystem) => 
                      val env = Environment
                        .make[F, Array[Byte]](
                          blocker,
                          ec,
                          parsed,
                          Source.filesystem[F](blocker, p.dir),
                          goodSink,
                          piiSink,
                          badSink,
                          _.as(()),
                          identity,
                          processor
                        )
                      runEnvironment[F, Array[Byte]](env, false)
                    case _ =>
                      val env = Environment
                        .make[F, A](
                          blocker,
                          ec,
                          parsed,
                          mkSource(blocker, file.auth, file.input),
                          goodSink,
                          piiSink,
                          badSink,
                          checkpointer,
                          getPayload,
                          processor
                        )
                      runEnvironment[F, A](env, ordered)
                  }
              } yield exit
            }
        ).flatten
      case Left(error) =>
        Sync[F].delay(System.err.println(error)) >> Sync[F].pure(ExitCode.Error)
    }

  private def initAttributedSink[F[_]: Concurrent: ContextShift: Timer](
    blocker: Blocker,
    auth: Authentication,
    output: Output,
    mkGoodSink: (Blocker, Authentication, Output) => Resource[F, AttributedByteSink[F]],
  ): Resource[F, AttributedByteSink[F]] =
    output match {
      case Output.FileSystem(path) =>
        Sink.fileSink[F](path, blocker).map(sink => row => sink(row.data))
      case _ =>
        mkGoodSink(blocker, auth, output)
    }

  private def runEnvironment[F[_]: ConcurrentEffect: ContextShift: Parallel: Timer, A](
    environment: Resource[F, Environment[F, A]],
    ordered: Boolean
  ): F[ExitCode] =
    environment.use { env =>
      val log = Logger[F].info("Running enrichment stream")
      val enrich = Enrich.run[F, A](env, ordered)
      val updates = Assets.run[F, A](env)
      val reporting = env.metrics.report
      val flow = enrich.merge(updates).merge(reporting)
      log >> flow.compile.drain.attempt.flatMap {
        case Left(exception) =>
          sendToSentry(exception, env.sentry) >>
          Logger[F].error(s"The Enrich job has stopped") >>
          Sync[F].raiseError[ExitCode](exception)
        case Right(_) =>
          Sync[F].pure(ExitCode.Success)
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
