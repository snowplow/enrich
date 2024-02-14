/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.pubsub

import scala.concurrent.duration._

import cats.Parallel
import cats.implicits._

import cats.effect.kernel.Resource
import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.metrics.CpuStarvationWarningMetrics

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.permutive.pubsub.consumer.ConsumerRecord

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Cloud
import com.snowplowanalytics.snowplow.enrich.common.fs2.Run

import com.snowplowanalytics.snowplow.enrich.gcp.GcsClient

import com.snowplowanalytics.snowplow.enrich.pubsub.generated.BuildInfo

object Main extends IOApp {

  override def runtimeConfig =
    super.runtimeConfig.copy(cpuStarvationCheckInterval = 10.seconds)

  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  override def onCpuStarvationWarn(metrics: CpuStarvationWarningMetrics): IO[Unit] =
    Logger[IO].debug(s"Cats Effect measured responsiveness in excess of ${metrics.starvationInterval * metrics.starvationThreshold}")

  /**
   * The maximum size of a serialized payload that can be written to pubsub.
   *
   *  Equal to 6.9 MB. The message will be base64 encoded by the underlying library, which brings the
   *  encoded message size to near 10 MB, which is the maximum allowed for PubSub.
   */
  private val MaxRecordSize = 6900000

  def run(args: List[String]): IO[ExitCode] =
    Run.run[IO, ConsumerRecord[IO, Array[Byte]]](
      args,
      BuildInfo.name,
      BuildInfo.version,
      BuildInfo.description,
      cliConfig => IO.pure(cliConfig),
      (input, _) => Source.init(input),
      out => Sink.initAttributed(out),
      out => Sink.initAttributed(out),
      out => Sink.init(out),
      checkpoint,
      _ => List(Resource.eval(GcsClient.mk[IO])),
      _.value,
      MaxRecordSize,
      Some(Cloud.Gcp),
      None
    )

  private def checkpoint[F[_]: Parallel](records: List[ConsumerRecord[F, Array[Byte]]]): F[Unit] =
    records.parTraverse_(_.ack)
}
