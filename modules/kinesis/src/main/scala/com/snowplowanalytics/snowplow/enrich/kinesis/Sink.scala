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
package com.snowplowanalytics.snowplow.enrich.pubsub

import scala.concurrent.duration._

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.implicits._

import cats.effect.{Concurrent, ContextShift, Resource, Sync, Timer}

import com.snowplowanalytics.snowplow.enrich.common.fs2.{AttributedByteSink, AttributedData, ByteSink}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Authentication, Output}

object Sink {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def init[F[_]: Concurrent: ContextShift: Timer](
    auth: Authentication,
    output: Output
  ): Resource[F, ByteSink[F]] =
    (auth, output) match {
      case (Authentication.Aws, o: Output.Kinesis) =>
        ???
        //pubsubSink[F, Array[Byte]](o).map(sink => bytes => sink(AttributedData(bytes, Map.empty)))
      case (auth, output) =>
        throw new IllegalArgumentException(s"Auth $auth is not GCP and/or output $output is not PubSub")
    }

  def initAttributed[F[_]: Concurrent: ContextShift: Timer](
    auth: Authentication,
    output: Output
  ): Resource[F, AttributedByteSink[F]] =
    (auth, output) match {
      case (Authentication.Aws, o: Output.Kinesis) =>
        ???
        //pubsubSink[F, Array[Byte]](o)
      case (auth, output) =>
        throw new IllegalArgumentException(s"Auth $auth is not GCP and/or output $output is not PubSub")
    }
}
