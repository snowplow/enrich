/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.streams.kinesis

import java.net.URI
import java.util.UUID

import scala.concurrent.duration._

import cats.Id

import com.snowplowanalytics.snowplow.streams.kinesis.{BackoffPolicy, KinesisSinkConfig, KinesisSinkConfigM, KinesisSourceConfig}

object KinesisConfig {

  val region = "eu-central-1"
  val endpoint = "localhost"

  def sourceConfig(localstackPort: Int, streamName: String) =
    KinesisSourceConfig(
      appName = UUID.randomUUID().toString,
      streamName = streamName,
      workerIdentifier = "test-worker",
      initialPosition = KinesisSourceConfig.InitialPosition.TrimHorizon,
      retrievalMode = KinesisSourceConfig.Retrieval.Polling(1000),
      customEndpoint = Some(URI.create(getLocalstackEndpoint(localstackPort))),
      dynamodbCustomEndpoint = Some(URI.create(getLocalstackEndpoint(localstackPort))),
      cloudwatchCustomEndpoint = Some(URI.create(getLocalstackEndpoint(localstackPort))),
      leaseDuration = 10.seconds,
      maxLeasesToStealAtOneTimeFactor = BigDecimal(2),
      checkpointThrottledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
      debounceCheckpoints = 10.seconds
    )

  def sinkConfig(localstackPort: Int, streamName: String): KinesisSinkConfig =
    KinesisSinkConfigM[Id](
      streamName = streamName,
      throttledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
      recordLimit = 500,
      byteLimit = 1024 * 1024 * 1024,
      customEndpoint = Some(URI.create(getLocalstackEndpoint(localstackPort)))
    )

  private def getLocalstackEndpoint(localstackPort: Int): String =
    s"http://$endpoint:$localstackPort"

  case class Streams(
    raw: String,
    enriched: String,
    failed: String,
    bad: String
  )

  def getStreams(uuid: String): Streams =
    Streams(s"raw-$uuid", s"enriched-$uuid", s"failed-$uuid", s"bad-1-$uuid")
}
