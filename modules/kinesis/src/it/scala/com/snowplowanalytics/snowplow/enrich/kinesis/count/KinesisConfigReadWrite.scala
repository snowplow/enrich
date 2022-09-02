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
package com.snowplowanalytics.snowplow.enrich.kinesis.count

import java.net.URI
import java.util.UUID

import scala.concurrent.duration._

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{BackoffPolicy, Input, MetricsReporters, Monitoring, Output}

object KinesisConfigReadWrite {

  val region = "eu-central-1"
  val rawStream = "raw"
  val enrichedStream = "enriched"
  val badStream = "bad-1"
  val endpoint = "localhost"

  def enrichedStreamConfig(port: Int) = Input.Kinesis(
    UUID.randomUUID().toString,
    enrichedStream,
    Some(region),
    Input.Kinesis.InitPosition.TrimHorizon,
    Input.Kinesis.Retrieval.Polling(1000),
    1000,
    BackoffPolicy(10.millis, 10.seconds, Some(10)),
    Some(URI.create(getEndpoint(port))),
    Some(URI.create(getEndpoint(port))),
    Some(URI.create(getEndpoint(port)))
  )

  def badStreamConfig(port: Int) = Input.Kinesis(
    UUID.randomUUID().toString,
    badStream,
    Some(region),
    Input.Kinesis.InitPosition.TrimHorizon,
    Input.Kinesis.Retrieval.Polling(1000),
    1000,
    BackoffPolicy(10.millis, 10.seconds, Some(10)),
    Some(URI.create(getEndpoint(port))),
    Some(URI.create(getEndpoint(port))),
    Some(URI.create(getEndpoint(port)))
  )

  def rawStreamConfig(port: Int) = Output.Kinesis(
    rawStream,
    Some(region),
    None,
    BackoffPolicy(10.millis, 10.seconds, Some(10)),
    BackoffPolicy(100.millis, 1.second, None),
    500,
    5242880,
    Some(URI.create(getEndpoint(port)))
  )

  val monitoring = Monitoring(
    None,
    MetricsReporters(None, None, false)
  )

  private def getEndpoint(port: Int): String =
    s"http://$endpoint:$port"
}
