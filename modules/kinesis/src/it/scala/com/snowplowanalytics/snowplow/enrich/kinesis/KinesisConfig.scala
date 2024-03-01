/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.kinesis

import java.net.URI
import java.util.UUID

import scala.concurrent.duration._

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{BackoffPolicy, Input, MetricsReporters, Monitoring, Output}

object KinesisConfig {

  val region = "eu-central-1"
  val endpoint = "localhost"

  def enrichedStreamConfig(localstackPort: Int, streamName: String) = Input.Kinesis(
    UUID.randomUUID().toString,
    streamName,
    Some(region),
    Input.Kinesis.InitPosition.TrimHorizon,
    Input.Kinesis.Retrieval.Polling(1000),
    1000,
    BackoffPolicy(10.millis, 10.seconds, Some(10)),
    Some(URI.create(getEndpoint(localstackPort))),
    Some(URI.create(getEndpoint(localstackPort))),
    Some(URI.create(getEndpoint(localstackPort)))
  )

  def badStreamConfig(localstackPort: Int, streamName: String) = Input.Kinesis(
    UUID.randomUUID().toString,
    streamName,
    Some(region),
    Input.Kinesis.InitPosition.TrimHorizon,
    Input.Kinesis.Retrieval.Polling(1000),
    1000,
    BackoffPolicy(10.millis, 10.seconds, Some(10)),
    Some(URI.create(getEndpoint(localstackPort))),
    Some(URI.create(getEndpoint(localstackPort))),
    Some(URI.create(getEndpoint(localstackPort)))
  )

  def rawStreamConfig(localstackPort: Int, streamName: String) = Output.Kinesis(
    streamName,
    Some(region),
    None,
    BackoffPolicy(10.millis, 10.seconds, Some(10)),
    BackoffPolicy(100.millis, 1.second, None),
    500,
    5242880,
    Some(URI.create(getEndpoint(localstackPort)))
  )

  val monitoring = Monitoring(
    None,
    MetricsReporters(None, None, false)
  )

  private def getEndpoint(localstackPort: Int): String =
    s"http://$endpoint:$localstackPort"

  case class Streams(raw: String, enriched: String, bad: String)

  def getStreams(uuid: String): Streams =
    Streams(s"raw-$uuid", s"enriched-$uuid", s"bad-1-$uuid")
}
