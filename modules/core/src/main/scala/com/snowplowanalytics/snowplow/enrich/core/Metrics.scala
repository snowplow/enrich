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
package com.snowplowanalytics.snowplow.enrich.core

import scala.concurrent.duration.{DurationInt, FiniteDuration}

import cats.implicits._

import cats.effect.kernel.{Async, Resource}

import fs2.Stream

import com.snowplowanalytics.snowplow.streams.SourceAndAck
import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}
import com.snowplowanalytics.snowplow.runtime.Metrics.Destination.PrometheusAndStatsd

trait Metrics[F[_]] {
  def addRaw(count: Int): F[Unit]
  def addEnriched(count: Int): F[Unit]
  def addFailed(count: Int): F[Unit]
  def addBad(count: Int): F[Unit]
  def addDropped(count: Int): F[Unit]
  def addInvalid(count: Int): F[Unit]
  def addEnrichedBytes(count: Long): F[Unit]
  def setLatency(latency: FiniteDuration): F[Unit]
  def setE2ELatency(latency: FiniteDuration): F[Unit]

  def scrape: F[String]
  def report: Stream[F, Nothing]
}

object Metrics {

  def build[F[_]: Async](config: Config.Metrics, sourceAndAck: SourceAndAck[F]): Resource[F, Metrics[F]] =
    CommonMetrics.build(config.statsd, config.prometheus).evalMap { entries =>
      for {
        raw <- entries.counter("raw", PrometheusAndStatsd)
        enriched <- entries.counter("good", PrometheusAndStatsd)
        failed <- entries.counter("failed", PrometheusAndStatsd)
        bad <- entries.counter("bad", PrometheusAndStatsd)
        dropped <- entries.counter("dropped", PrometheusAndStatsd)
        invalid <- entries.counter("invalid_enriched", PrometheusAndStatsd)
        enrichedBytes <- entries.counter("good_bytes", PrometheusAndStatsd)
        latency <- entries.lagGauge("latency", sourceAndAck.currentStreamLatency, PrometheusAndStatsd)
        e2eLatency <- entries.timer("e2e_latency", 1.second, PrometheusAndStatsd)
      } yield new Metrics[F] {
        def addRaw(count: Int): F[Unit] = raw.add(count.toLong)
        def addEnriched(count: Int): F[Unit] = enriched.add(count.toLong)
        def addFailed(count: Int): F[Unit] = failed.add(count.toLong)
        def addBad(count: Int): F[Unit] = bad.add(count.toLong)
        def addDropped(count: Int): F[Unit] = dropped.add(count.toLong)
        def addInvalid(count: Int): F[Unit] = invalid.add(count.toLong)
        def addEnrichedBytes(count: Long): F[Unit] = enrichedBytes.add(count)
        def setLatency(l: FiniteDuration): F[Unit] = latency.record(l)
        def setE2ELatency(l: FiniteDuration): F[Unit] = e2eLatency.record(l)

        def scrape: F[String] = entries.scrape
        def report: Stream[F, Nothing] = entries.report
      }
    }
}
