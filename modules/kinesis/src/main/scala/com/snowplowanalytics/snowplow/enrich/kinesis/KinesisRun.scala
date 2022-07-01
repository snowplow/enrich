/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.kinesis

import cats.effect.{Clock, ConcurrentEffect, ContextShift, ExitCode, Sync, Timer}

import cats.Parallel
import cats.implicits._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import fs2.aws.kinesis.CommittableRecord

import software.amazon.kinesis.exceptions.ShutdownException

import com.snowplowanalytics.snowplow.enrich.common.fs2.Run
import com.snowplowanalytics.snowplow.enrich.common.fs2.Telemetry

import com.snowplowanalytics.snowplow.enrich.kinesis.generated.BuildInfo

object KinesisRun {

  // Kinesis records must not exceed 1MB
  private val MaxRecordSize = 1000000

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def run[F[_]: Clock: ConcurrentEffect: ContextShift: Parallel: Timer](args: List[String], ec: ExecutionContext): F[ExitCode] =
    Run.run[F, CommittableRecord](
      args,
      BuildInfo.name,
      BuildInfo.version,
      BuildInfo.description,
      ec,
      DynamoDbConfig.updateCliConfig[F],
      Source.init[F],
      (blocker, out) => Sink.initAttributed(blocker, out),
      (blocker, out) => Sink.initAttributed(blocker, out),
      (blocker, out) => Sink.init(blocker, out),
      checkpoint[F],
      List(_ => S3Client.mk[F]),
      getPayload,
      MaxRecordSize,
      Some(Telemetry.Cloud.Aws),
      getRuntimeRegion
    )

  def getPayload(record: CommittableRecord): Array[Byte] = {
    val data = record.record.data
    val buffer = ArrayBuffer[Byte]()
    while (data.hasRemaining())
      buffer.append(data.get)
    buffer.toArray
  }

  /** For each shard, the record with the biggest sequence number is found, and checkpointed. */
  private def checkpoint[F[_]: Parallel: Sync: Timer](records: List[CommittableRecord]): F[Unit] =
    records
      .groupBy(_.shardId)
      .foldLeft(List.empty[CommittableRecord]) { (acc, shardRecords) =>
        shardRecords._2
          .reduceLeft[CommittableRecord] { (biggest, record) =>
            if (record.sequenceNumber > biggest.sequenceNumber)
              record
            else
              biggest
          } :: acc
      }
      .parTraverse_ { record =>
        Sync[F]
          .delay(record.checkpointer.checkpoint)
          .recoverWith {
            // The ShardRecordProcessor instance has been shutdown. This just means another KCL
            // worker has stolen our lease. It is expected during autoscaling of instances, and is
            // safe to ignore.
            case _: ShutdownException =>
              Logger[F].warn(s"Skipping checkpointing of shard ${record.shardId} because this worker no longer owns the lease")
          }
      }
}
