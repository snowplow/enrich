/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.nsq

import scala.concurrent.duration._

import cats.Parallel
import cats.implicits._

import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.kernel.{Resource, Sync}
import cats.effect.metrics.CpuStarvationWarningMetrics

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.enrich.common.fs2.Run
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.BlobStorageClients
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Clients.Client

import com.snowplowanalytics.snowplow.enrich.aws.S3Client

import com.snowplowanalytics.snowplow.enrich.gcp.GcsClient

import com.snowplowanalytics.snowplow.enrich.azure.AzureStorageClient

import com.snowplowanalytics.snowplow.enrich.nsq.generated.BuildInfo

object Main extends IOApp {

  override def runtimeConfig =
    super.runtimeConfig.copy(cpuStarvationCheckInterval = 10.seconds)

  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  override def onCpuStarvationWarn(metrics: CpuStarvationWarningMetrics): IO[Unit] =
    Logger[IO].debug(s"Cats Effect measured responsiveness in excess of ${metrics.starvationInterval * metrics.starvationThreshold}")

  // Nsq records must not exceed 1MB
  private val MaxRecordSize = 1000000

  def run(args: List[String]): IO[ExitCode] =
    Run.run[IO, Record[IO]](
      args,
      BuildInfo.name,
      """(\d.\d.\d(-\w*\d*)?)""".r.findFirstIn(BuildInfo.version).getOrElse(BuildInfo.version),
      BuildInfo.description,
      IO.pure,
      (input, _) => Source.init(input),
      out => Sink.initAttributed(out),
      out => Sink.initAttributed(out),
      out => Sink.init(out),
      checkpoint,
      createBlobStorageClient,
      _.data,
      MaxRecordSize,
      None,
      None
    )

  private def checkpoint[F[_]: Parallel: Sync](records: List[Record[F]]): F[Unit] =
    records.parTraverse_(_.ack)

  private def createBlobStorageClient(conf: BlobStorageClients): List[Resource[IO, Client[IO]]] = {
    val gcs = if (conf.gcs) Some(Resource.eval(GcsClient.mk[IO])) else None
    val aws = if (conf.s3) Some(S3Client.mk[IO]) else None
    val azure = conf.azureStorage.map(s => AzureStorageClient.mk[IO](s))
    List(gcs, aws, azure).flatten
  }
}
