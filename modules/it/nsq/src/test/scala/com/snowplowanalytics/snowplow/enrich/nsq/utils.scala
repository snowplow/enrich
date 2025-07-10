/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.nsq

import scala.concurrent.duration._

import cats.Id

import cats.effect.IO

import com.snowplowanalytics.snowplow.streams.nsq.{BackoffPolicy, NsqFactory, NsqSinkConfigM, NsqSourceConfig}

import com.snowplowanalytics.snowplow.enrich.core.Utils

object utils {

  def run(
    enrichNsq: EnrichNsq,
    nbEnriched: Long,
    nbBad: Long
  ): IO[Utils.Output] = {
    val rawSinkConfig = sinkConfig(enrichNsq.rawTopic, enrichNsq.nsqConfig.nsqd2.broadcastAddress, enrichNsq.nsqConfig.nsqd1.tcpPort)
    val enrichedSourceConfig =
      sourceConfig(enrichNsq.enrichedTopic,
                   "EnrichedChannel",
                   enrichNsq.nsqConfig.nsqd2.broadcastAddress,
                   enrichNsq.nsqConfig.lookup2.httpPort
      )
    val failedSourceConfig =
      sourceConfig(enrichNsq.failedTopic, "FailedChannel", enrichNsq.nsqConfig.nsqd2.broadcastAddress, enrichNsq.nsqConfig.lookup2.httpPort)
    val badSourceConfig =
      sourceConfig(enrichNsq.badTopic, "BadChannel", enrichNsq.nsqConfig.nsqd2.broadcastAddress, enrichNsq.nsqConfig.lookup2.httpPort)

    NsqFactory.resource[IO].use { factory =>
      Utils.runEnrichPipe(
        factory,
        rawSinkConfig,
        enrichedSourceConfig,
        failedSourceConfig,
        badSourceConfig,
        nbEnriched,
        nbBad
      )
    }
  }

  private def sourceConfig(
    topic: String,
    channel: String,
    host: String,
    port: Int
  ) =
    NsqSourceConfig(
      topic = topic,
      channel = channel,
      lookupHost = host,
      lookupPort = port,
      maxBufferQueueSize = 3000
    )

  private def sinkConfig(
    topic: String,
    host: String,
    port: Int
  ) =
    NsqSinkConfigM[Id](
      topic = topic,
      nsqdHost = host,
      nsqdPort = port,
      byteLimit = 1000000,
      backoffPolicy = BackoffPolicy(minBackoff = 100.milliseconds, maxBackoff = 10.seconds, maxRetries = Some(10))
    )
}
