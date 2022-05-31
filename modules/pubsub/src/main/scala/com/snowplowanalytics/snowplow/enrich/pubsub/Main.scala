/*
 * Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.pubsub

import cats.Parallel
import cats.implicits._

import cats.effect.{ExitCode, IO, IOApp, Resource, Sync}

import com.permutive.pubsub.consumer.ConsumerRecord

import com.snowplowanalytics.snowplow.enrich.common.fs2.Run
import com.snowplowanalytics.snowplow.enrich.common.fs2.Telemetry

import com.snowplowanalytics.snowplow.enrich.pubsub.generated.BuildInfo

object Main extends IOApp {

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
      (_, cliConfig) => IO(cliConfig),
      (blocker, input, _) => Source.init(blocker, input),
      (_, out, _) => Sink.initAttributed(out),
      (_, out, _) => Sink.initAttributed(out),
      (_, out, _) => Sink.init(out),
      checkpoint,
      List(b => Resource.eval(GcsClient.mk[IO](b))),
      _.value,
      MaxRecordSize,
      Some(Telemetry.Cloud.Gcp),
      None
    )

  private def checkpoint[F[_]: Parallel: Sync](records: List[ConsumerRecord[F, Array[Byte]]]): F[Unit] =
    records.parTraverse_(_.ack)
}
