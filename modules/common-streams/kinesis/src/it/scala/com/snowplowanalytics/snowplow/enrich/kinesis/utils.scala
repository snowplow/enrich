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

import java.io._
import java.net._
import java.net.URI
import java.util.UUID

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import cats.Id

import cats.effect.IO
import cats.effect.kernel.Resource

import cats.effect.testing.specs2.CatsEffect

import com.snowplowanalytics.snowplow.streams.kinesis.KinesisFactory

import com.snowplowanalytics.snowplow.enrich.streams.common.Utils

import com.snowplowanalytics.snowplow.streams.kinesis.{BackoffPolicy, KinesisSinkConfig, KinesisSinkConfigM, KinesisSourceConfig}

object utils extends CatsEffect {

  def run(
    enrichKinesis: EnrichKinesis,
    nbEnriched: Long,
    nbBad: Long = 0L,
    nbGoodDrop: Long = 0L,
    nbBadDrop: Long = 0L
  ): IO[Utils.Output] = {
    val rawSinkConfig = sinkConfig(enrichKinesis.localstack.host, enrichKinesis.localstack.mappedPort, enrichKinesis.rawStream)
    val enrichedSourceConfig =
      sourceConfig(enrichKinesis.localstack.host, enrichKinesis.localstack.mappedPort, enrichKinesis.enrichedStream)
    val failedSourceConfig = sourceConfig(enrichKinesis.localstack.host, enrichKinesis.localstack.mappedPort, enrichKinesis.failedStream)
    val badSourceConfig = sourceConfig(enrichKinesis.localstack.host, enrichKinesis.localstack.mappedPort, enrichKinesis.badStream)

    KinesisFactory.resource[IO].use { factory =>
      Utils.runEnrichPipe(
        factory,
        rawSinkConfig,
        enrichedSourceConfig,
        failedSourceConfig,
        badSourceConfig,
        nbEnriched,
        nbBad,
        nbGoodDrop,
        nbBadDrop
      )
    }
  }

  def sourceConfig(
    localstackHost: String,
    localstackPort: Int,
    streamName: String
  ) =
    KinesisSourceConfig(
      appName = UUID.randomUUID().toString,
      streamName = streamName,
      workerIdentifier = "test-worker",
      initialPosition = KinesisSourceConfig.InitialPosition.TrimHorizon,
      retrievalMode = KinesisSourceConfig.Retrieval.Polling(1000),
      customEndpoint = Some(URI.create(s"http://$localstackHost:$localstackPort")),
      dynamodbCustomEndpoint = Some(URI.create(s"http://$localstackHost:$localstackPort")),
      cloudwatchCustomEndpoint = Some(URI.create(s"http://$localstackHost:$localstackPort")),
      leaseDuration = 10.seconds,
      maxLeasesToStealAtOneTimeFactor = BigDecimal(2),
      checkpointThrottledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
      debounceCheckpoints = 10.seconds
    )

  def sinkConfig(
    localstackHost: String,
    localstackPort: Int,
    streamName: String
  ): KinesisSinkConfig =
    KinesisSinkConfigM[Id](
      streamName = streamName,
      throttledBackoffPolicy = BackoffPolicy(minBackoff = 100.millis, maxBackoff = 1.second),
      recordLimit = 500,
      byteLimit = 1024 * 1024 * 1024,
      customEndpoint = Some(URI.create(s"http://$localstackHost:$localstackPort"))
    )

  trait StatsdAdmin {
    def get(metricType: String): IO[String]
    def getCounters = get("counters")
    def getGauges = get("gauges")
  }

  def mkStatsdAdmin(host: String, port: Int): Resource[IO, StatsdAdmin] =
    for {
      socket <- Resource.make(IO.blocking(new Socket(host, port)))(s => IO(s.close()))
      toStatsd <- Resource.make(IO(new PrintWriter(socket.getOutputStream(), true)))(pw => IO(pw.close()))
      fromStatsd <- Resource.make(IO(new BufferedReader(new InputStreamReader(socket.getInputStream()))))(br => IO(br.close()))
    } yield new StatsdAdmin {
      def get(metricType: String): IO[String] =
        for {
          _ <- IO.blocking(toStatsd.println(metricType))
          stats <- IO.blocking(fromStatsd.lines().iterator().asScala.takeWhile(!_.toLowerCase().contains("end")).mkString("\n"))
        } yield stats
    }
}
