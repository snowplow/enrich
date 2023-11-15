/*
 * Copyright (c) 2023-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.nsq

import cats.Parallel

import cats.implicits._

import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.kernel.{Resource, Sync}

import com.snowplowanalytics.snowplow.enrich.common.fs2.Run
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.BlobStorageClients
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Clients.Client

import com.snowplowanalytics.snowplow.enrich.aws.S3Client

import com.snowplowanalytics.snowplow.enrich.gcp.GcsClient

import com.snowplowanalytics.snowplow.enrich.azure.AzureStorageClient

import com.snowplowanalytics.snowplow.enrich.nsq.generated.BuildInfo

object Main extends IOApp {

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
    val azure = conf.azureStorage.map(s => AzureStorageClient.mk[IO](s.storageAccountName))
    List(gcs, aws, azure).flatten
  }
}
