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
package com.snowplowanalytics.snowplow.enrich.streams.common

import scala.concurrent.duration.{Duration, FiniteDuration}

import cats.implicits._

import cats.effect.kernel.{Async, Ref, Sync}

import fs2.Stream

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

  def report: Stream[F, Nothing]
}

object Metrics {

  def build[F[_]: Async](config: Config.Metrics): F[Metrics[F]] =
    Ref[F].of(State.empty).map(impl(config, _))

  private case class State(
    raw: Int,
    enriched: Int,
    failed: Int,
    bad: Int,
    dropped: Int,
    invalid: Option[Int], // optional because `validation.acceptInvalid` can be false
    latency: FiniteDuration,
    e2eLatency: Option[FiniteDuration]
  ) extends CommonMetrics.State {
    def toKVMetrics: List[CommonMetrics.KVMetric] =
      List(
        KVMetric.CountRaw(raw),
        KVMetric.CountEnriched(enriched),
        KVMetric.CountFailed(failed),
        KVMetric.CountIncomplete(failed),
        KVMetric.CountBad(bad),
        KVMetric.CountDropped(dropped),
        KVMetric.Latency(latency)
      ) ++ invalid.map(KVMetric.CountInvalid(_)) ++
        e2eLatency.map(KVMetric.E2ELatency(_)) ++
        e2eLatency.map(KVMetric.LegacyLatency(_))
  }

  private object State {
    def empty: State = State(0, 0, 0, 0, 0, None, Duration.Zero, None)
  }

  private def impl[F[_]: Async](config: Config.Metrics, ref: Ref[F, State]): Metrics[F] =
    new CommonMetrics[F, State](ref, Sync[F].pure(State.empty), config.statsd) with Metrics[F] {
      def addRaw(count: Int): F[Unit] =
        ref.update(s => s.copy(raw = s.raw + count))
      def addEnriched(count: Int): F[Unit] =
        ref.update(s => s.copy(enriched = s.enriched + count))
      def addFailed(count: Int): F[Unit] =
        ref.update(s => s.copy(failed = s.failed + count))
      def addBad(count: Int): F[Unit] =
        ref.update(s => s.copy(bad = s.bad + count))
      def addDropped(count: Int): F[Unit] =
        ref.update(s => s.copy(dropped = s.dropped + count))
      def addInvalid(count: Int): F[Unit] =
        ref.update(s => s.copy(invalid = s.invalid |+| Some(count)))
      def setLatency(latency: FiniteDuration): F[Unit] =
        ref.update(s => s.copy(latency = s.latency.max(latency)))
      def setE2ELatency(e2eLatency: FiniteDuration): F[Unit] =
        ref.update { s =>
          val newLatency = s.e2eLatency.fold(e2eLatency)(_.max(e2eLatency))
          s.copy(e2eLatency = Some(newLatency))
        }
    }

  private object KVMetric {

    final case class CountRaw(v: Int) extends CommonMetrics.KVMetric {
      val key = "raw"
      val value = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class CountEnriched(v: Int) extends CommonMetrics.KVMetric {
      val key = "good"
      val value = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class CountFailed(v: Int) extends CommonMetrics.KVMetric {
      val key = "failed"
      val value = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class CountBad(v: Int) extends CommonMetrics.KVMetric {
      val key = "bad"
      val value = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class CountDropped(v: Int) extends CommonMetrics.KVMetric {
      val key = "dropped"
      val value = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class CountInvalid(v: Int) extends CommonMetrics.KVMetric {
      val key = "invalid_enriched"
      val value = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class Latency(d: FiniteDuration) extends CommonMetrics.KVMetric {
      val key = "latency_millis"
      val value = d.toMillis.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

    final case class E2ELatency(d: FiniteDuration) extends CommonMetrics.KVMetric {
      val key = "e2e_latency_millis"
      val value = d.toMillis.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

    // Legacy metrics

    final case class CountIncomplete(v: Int) extends CommonMetrics.KVMetric {
      val key = "incomplete"
      val value = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class LegacyLatency(d: FiniteDuration) extends CommonMetrics.KVMetric {
      val key = "latency"
      val value = d.toMillis.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }
  }
}
