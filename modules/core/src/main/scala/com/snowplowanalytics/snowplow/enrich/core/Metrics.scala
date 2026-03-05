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

import scala.concurrent.duration.FiniteDuration

import cats.implicits._

import cats.effect.kernel.{Async, Resource, Sync}

import fs2.Stream

import com.snowplowanalytics.snowplow.streams.SourceAndAck
import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}

trait Metrics[F[_]] {
  def addRaw(count: Int): F[Unit]
  def addEnriched(count: Int): F[Unit]
  def addFailed(count: Int): F[Unit]
  def addBad(count: Int): F[Unit]
  def addDropped(count: Int): F[Unit]
  def addInvalid(count: Int): F[Unit]
  def setLatency(latency: FiniteDuration): F[Unit]
  def setE2ELatency(latency: FiniteDuration): F[Unit]

  def scrape: F[String]
  def report: Stream[F, Nothing]
}

object Metrics {

  def build[F[_]: Async](config: Config.Metrics, sourceAndAck: SourceAndAck[F]): Resource[F, Metrics[F]] =
    CommonMetrics.build(config.statsd, config.prometheus).evalMap { entries =>
      for {
        raw <- entries.counter("raw")
        enriched <- entries.counter("good")
        failed <- entries.counter("failed")
        bad <- entries.counter("bad")
        dropped <- entries.counter("dropped")
        invalid <- entries.counter("invalid_enriched")
        latency <- entries.timer("latency_millis", sourceAndAck.currentStreamLatency)
        e2eLatency <- entries.timer("e2e_latency_millis", Sync[F].pure(None))
      } yield new Metrics[F] {
        def addRaw(count: Int): F[Unit] = raw.add(count.toLong)
        def addEnriched(count: Int): F[Unit] = enriched.add(count.toLong)
        def addFailed(count: Int): F[Unit] = failed.add(count.toLong)
        def addBad(count: Int): F[Unit] = bad.add(count.toLong)
        def addDropped(count: Int): F[Unit] = dropped.add(count.toLong)
        def addInvalid(count: Int): F[Unit] = invalid.add(count.toLong)
        def setLatency(l: FiniteDuration): F[Unit] = latency.record(l)
        def setE2ELatency(l: FiniteDuration): F[Unit] = e2eLatency.record(l)

        def scrape: F[String] = entries.scrape
        def report: Stream[F, Nothing] = entries.report
      }
    }
}
