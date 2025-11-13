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
package com.snowplowanalytics.snowplow.enrich.core

import scala.concurrent.duration.DurationInt

import cats.effect.{ExitCode, IO, Resource}
import cats.effect.metrics.CpuStarvationWarningMetrics

import io.circe.Decoder

import com.monovore.decline.Opts
import com.monovore.decline.effect.CommandIOApp

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.streams.Factory
import com.snowplowanalytics.snowplow.runtime.AppInfo

import com.snowplowanalytics.snowplow.enrich.cloudutils.core.BlobClientFactory

abstract class EnrichApp[FactoryConfig: Decoder, SourceConfig: Decoder, SinkConfig: Decoder: OptionalDecoder, BlobClientsConfig: Decoder](
  info: AppInfo
) extends CommandIOApp(name = EnrichApp.helpCommand(info), header = info.dockerAlias, version = info.version) {

  override def runtimeConfig =
    super.runtimeConfig.copy(cpuStarvationCheckInterval = 10.seconds)

  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  override def onCpuStarvationWarn(metrics: CpuStarvationWarningMetrics): IO[Unit] =
    Logger[IO].debug(s"Cats Effect measured responsiveness in excess of ${metrics.starvationInterval * metrics.starvationThreshold}")

  type FactoryProvider = FactoryConfig => Resource[IO, Factory[IO, SourceConfig, SinkConfig]]
  type BlobClientsProvider = BlobClientsConfig => List[BlobClientFactory[IO]]

  def toFactory: FactoryProvider
  def toBlobClients: BlobClientsProvider

  final def main: Opts[IO[ExitCode]] = Run.fromCli(info, toFactory, toBlobClients)
}

object EnrichApp {
  private def helpCommand(appInfo: AppInfo) = s"docker run ${appInfo.dockerAlias}"
}
