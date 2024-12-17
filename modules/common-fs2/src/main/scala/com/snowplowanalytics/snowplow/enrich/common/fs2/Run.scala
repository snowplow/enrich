/*
 * Copyright (c) 2021-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

import cats.implicits._

import fs2.Stream

import scala.concurrent.ExecutionContext

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.ExitCode

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import retry.syntax.all._

import com.snowplowanalytics.snowplow.badrows.Processor

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{
  BackoffPolicy,
  BlobStorageClients,
  Cloud,
  Input,
  Monitoring,
  Output,
  RetryCheckpointing
}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.{CliConfig, ParsedConfigs}
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.{FileSink, Retries, Source}
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Clients.Client

object Run {
  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def run[F[_]: Async, A](
    args: List[String],
    name: String,
    version: String,
    description: String,
    updateCliConfig: CliConfig => F[CliConfig],
    mkSource: (Input, Monitoring) => Stream[F, A],
    mkSinkGood: Output => Resource[F, AttributedByteSink[F]],
    mkSinkPii: Output => Resource[F, AttributedByteSink[F]],
    mkSinkBad: Output => Resource[F, ByteSink[F]],
    mkSinkIncomplete: Output => Resource[F, AttributedByteSink[F]],
    checkpoint: List[A] => F[Unit],
    mkClients: BlobStorageClients => List[Resource[F, Client[F]]],
    getPayload: A => Array[Byte],
    maxRecordSize: Int,
    cloud: Option[Cloud],
    getRegion: => Option[String]
  ): F[ExitCode] =
    CliConfig.command(name, version, description).parse(args) match {
      case Right(cli) =>
        updateCliConfig(cli).flatMap { cfg =>
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
                  blockingEC = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool)
                  processor = Processor(name, version)
                  file = parsed.configFile
                  _ <- checkLicense(file.license.accept)
                  sinkGood = initAttributedSink(file.output.good, mkSinkGood)
                  sinkPii = file.output.pii.map(out => initAttributedSink(out, mkSinkPii))
                  sinkBad = file.output.bad match {
                              case f: Output.FileSystem =>
                                FileSink.fileSink[F](f)
                              case _ =>
                                mkSinkBad(file.output.bad)
                            }
                  sinkIncomplete = file.output.incomplete.map(out => initAttributedSink(out, mkSinkIncomplete))
                  clients = mkClients(file.blobStorage).sequence
                  exit <- file.input match {
                            case p: Input.FileSystem =>
                              val env = Environment
                                .make[F, Array[Byte]](
                                  blockingEC,
                                  parsed,
                                  Source.filesystem[F](p.dir),
                                  sinkGood,
                                  sinkPii,
                                  sinkBad,
                                  sinkIncomplete,
                                  clients,
                                  _ => Sync[F].unit,
                                  identity,
                                  processor,
                                  maxRecordSize,
                                  cloud,
                                  getRegion,
                                  file.featureFlags,
                                  file.validation.atomicFieldsLimits
                                )
                              runEnvironment[F, Array[Byte]](env)
                            case input =>
                              val checkpointing = input match {
                                case retrySettings: RetryCheckpointing =>
                                  withRetries(
                                    retrySettings.checkpointBackoff,
                                    "Checkpointing failed",
                                    checkpoint
                                  )
                                case _ =>
                                  checkpoint
                              }
                              val env = Environment
                                .make[F, A](
                                  blockingEC,
                                  parsed,
                                  mkSource(file.input, file.monitoring),
                                  sinkGood,
                                  sinkPii,
                                  sinkBad,
                                  sinkIncomplete,
                                  clients,
                                  checkpointing,
                                  getPayload,
                                  processor,
                                  maxRecordSize,
                                  cloud,
                                  getRegion,
                                  file.featureFlags,
                                  file.validation.atomicFieldsLimits
                                )
                              runEnvironment[F, A](env)
                          }
                } yield exit
            )
            .flatten
        }
      case Left(error) =>
        Logger[F].error(s"CLI arguments are invalid. Error: $error") >> Sync[F].pure(ExitCode.Error)
    }

  private def initAttributedSink[F[_]: Async](
    output: Output,
    mkSinkGood: Output => Resource[F, AttributedByteSink[F]]
  ): Resource[F, AttributedByteSink[F]] =
    output match {
      case f: Output.FileSystem =>
        FileSink.fileSink[F](f).map(sink => records => sink(records.map(_.data)))
      case _ =>
        mkSinkGood(output)
    }

  private def runEnvironment[F[_]: Async, A](
    environment: Resource[F, Environment[F, A]]
  ): F[ExitCode] =
    environment.use { env =>
      val enrich = Enrich.run[F, A](env)
      val updates = Assets.run[F, A](env.blockingEC, env.shifter, env.semaphore, env.assetsUpdatePeriod, env.assetsState, env.enrichments)
      val telemetry = Telemetry.run[F, A](env)
      val reporting = env.metrics.report
      val metadata = env.metadata.report
      val flow = enrich.merge(updates).merge(reporting).merge(telemetry).merge(metadata)
      flow.compile.drain.as(ExitCode.Success).recoverWith {
        case exception: Throwable =>
          Logger[F].error(s"An error happened") >>
            Sync[F].raiseError[ExitCode](exception)
      }
    }

  private def withRetries[F[_]: Async, A, B](
    config: BackoffPolicy,
    errorMessage: String,
    f: A => F[B]
  ): A => F[B] = { a =>
    val retryPolicy = Retries.fullJitter[F](config)

    f(a)
      .retryingOnAllErrors(
        policy = retryPolicy,
        onError = (exception, retryDetails) =>
          Logger[F]
            .error(exception)(s"$errorMessage (${retryDetails.retriesSoFar} retries)")
      )
  }

  private def checkLicense[F[_]: Sync](acceptLicense: Boolean): F[Unit] =
    if (acceptLicense)
      Sync[F].unit
    else
      Sync[F].raiseError(
        new IllegalStateException(
          "Please accept the terms of the Snowplow Limited Use License Agreement to proceed. See https://docs.snowplow.io/docs/pipeline-components-and-applications/enrichment-components/configuration-reference/#license for more information on the license and how to configure this."
        )
      )
}
