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
package com.snowplowanalytics.snowplow.enrich.streams.common

import java.nio.file.Path

import cats.data.EitherT
import cats.implicits._

import cats.effect.ExitCode
import cats.effect.kernel.{Async, Resource, Sync}

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import io.circe.Decoder

import com.monovore.decline.Opts

import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.runtime.{AppInfo, ConfigParser, LogUtils, Telemetry}

import com.snowplowanalytics.snowplow.enrich.cloudutils.core.BlobClient

object Run {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def fromCli[F[_]: Async, SourceConfig: Decoder, SinkConfig: Decoder: OptionalDecoder, BlobClientsConfig: Decoder](
    appInfo: AppInfo,
    toSource: SourceConfig => F[SourceAndAck[F]],
    toSink: SinkConfig => Resource[F, Sink[F]],
    toBlobClients: BlobClientsConfig => List[BlobClient[F]]
  ): Opts[F[ExitCode]] = {
    val configPathOpt = Opts.option[Path]("config", help = "path to config file")
    val igluPathOpt = Opts.option[Path]("iglu-config", help = "path to iglu resolver config file")
    val enrichmentsPathOpt = Opts.option[Path]("enrichments", help = "path to dir with enrichments config files")
    (configPathOpt, igluPathOpt, enrichmentsPathOpt).mapN {
      case (configPath, igluPath, enrichmentsPath) =>
        fromConfigPaths(appInfo, toSource, toSink, toBlobClients, configPath, igluPath, enrichmentsPath)
    }
  }

  def fromConfigPaths[F[_]: Async, SourceConfig: Decoder, SinkConfig: Decoder: OptionalDecoder, BlobClientsConfig: Decoder](
    appInfo: AppInfo,
    toSource: SourceConfig => F[SourceAndAck[F]],
    toSink: SinkConfig => Resource[F, Sink[F]],
    toBlobClients: BlobClientsConfig => List[BlobClient[F]],
    pathToConfig: Path,
    pathToResolver: Path,
    pathToEnrichments: Path
  ): F[ExitCode] = {

    val eitherT = for {
      config <- ConfigParser.configFromFile[F, Config[SourceConfig, SinkConfig, BlobClientsConfig]](pathToConfig)
      resolver <- ConfigParser.igluResolverFromFile(pathToResolver)
      enrichments <- Config.mkEnrichmentsJson(pathToEnrichments)

      fullConfig = Config.Full(config, resolver, enrichments)
      _ <- EitherT.right[String](fromConfig(appInfo, toSource, toSink, toBlobClients, fullConfig))
    } yield ExitCode.Success

    eitherT
      .leftSemiflatMap { s: String =>
        Logger[F].error(s).as(ExitCode.Error)
      }
      .merge
      .handleErrorWith { e =>
        Logger[F].error(e)("Exiting") >>
          LogUtils.prettyLogException(e).as(ExitCode.Error)
      }
  }

  private def fromConfig[F[_]: Async, SourceConfig, SinkConfig, BlobClientsConfig](
    appInfo: AppInfo,
    toSource: SourceConfig => F[SourceAndAck[F]],
    toSink: SinkConfig => Resource[F, Sink[F]],
    toBlobClients: BlobClientsConfig => List[BlobClient[F]],
    config: Config.Full[SourceConfig, SinkConfig, BlobClientsConfig]
  ): F[ExitCode] =
    Environment.fromConfig(config, appInfo, toSource, toSink, toBlobClients).use { env =>
      Processing
        .stream(env)
        .concurrently(Telemetry.stream(config.main.telemetry, env.appInfo, env.httpClient))
        .concurrently(env.metrics.report)
        .compile
        .drain
        .as(ExitCode.Success)
    }
}
