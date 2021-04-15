/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.fs2.io

import cats.implicits._
import cats.Applicative
import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Clock, ConcurrentEffect, ContextShift, Effect, Resource, Sync, Timer}

import fs2.Stream

import com.codahale.metrics.{Counter, Gauge, MetricRegistry, Slf4jReporter}

import org.slf4j.LoggerFactory

import scala.concurrent.duration.MILLISECONDS

import com.snowplowanalytics.snowplow.enrich.fs2.config.io.MetricsReporter

trait Metrics[F[_]] {

  /** Send latest metrics to reporter */
  def report: Stream[F, Unit]

  /**
   * Track latency between collector hit and enrichment
   * This function gets current timestamp by itself
   */
  def enrichLatency(collectorTstamp: Option[Long]): F[Unit]

  /** Increment raw payload count */
  def rawCount: F[Unit]

  /** Increment good enriched events */
  def goodCount: F[Unit]

  /** Increment bad events */
  def badCount: F[Unit]
}

object Metrics {

  val LoggerName = "enrich.metrics"
  val LatencyGaugeName = "latency"
  val RawCounterName = "raw"
  val GoodCounterName = "good"
  val BadCounterName = "bad"

  def build[F[_]: ContextShift: ConcurrentEffect: Timer](
    blocker: Blocker,
    config: MetricsReporter
  ): F[Metrics[F]] =
    for {
      registry <- Sync[F].delay((new MetricRegistry()))
      latencyRef <- Ref.of[F, Option[(Long, Long)]](None)
      rawCounter <- Sync[F].delay(registry.counter(RawCounterName))
      goodCounter <- Sync[F].delay(registry.counter(GoodCounterName))
      badCounter <- Sync[F].delay(registry.counter(BadCounterName))
      latencySupplier = latencyGauge(latencyRef)
      rep = reporterStream(blocker, config, registry)
    } yield ofMetrics(rawCounter,
                      goodCounter,
                      badCounter,
                      latencyRef,
                      rep,
                      Sync[F].delay(registry.gauge(LatencyGaugeName, latencySupplier)).void
    )

  trait Reporter[F[_]] {
    def report: F[Unit]
  }

  /**
   * A stream which reports metrics and immediately resets the latency gauge after each report.
   * This is because latency must not get "stuck" on a value when enrich is no longer receiving new
   * events.
   */
  def reporterStream[F[_]: Sync: Timer: ContextShift, A](
    blocker: Blocker,
    config: MetricsReporter,
    registry: MetricRegistry
  ): Stream[F, Unit] =
    for {
      rep <- Stream.resource(makeReporter(blocker, config, registry))
      _ <- Stream.fixedDelay[F](config.period)
      _ <- Stream.eval(rep.report)
      _ <- Stream.eval(Sync[F].delay(registry.remove(LatencyGaugeName)))
    } yield ()

  def makeReporter[F[_]: Sync: ContextShift: Timer](
    blocker: Blocker,
    config: MetricsReporter,
    registry: MetricRegistry
  ): Resource[F, Reporter[F]] =
    config match {
      case MetricsReporter.Stdout(_, prefix) =>
        for {
          logger <- Resource.liftF(Sync[F].delay(LoggerFactory.getLogger(LoggerName)))
          slf4jReporter <-
            Resource.fromAutoCloseable(
              Sync[F].delay(
                Slf4jReporter.forRegistry(registry).outputTo(logger).prefixedWith(prefix.getOrElse(MetricsReporter.DefaultPrefix)).build
              )
            )
        } yield new Reporter[F] {
          def report: F[Unit] = Sync[F].delay(slf4jReporter.report())
        }
      case statsd: MetricsReporter.StatsD =>
        StatsDReporter.make(blocker, statsd, registry)
    }

  private def ofMetrics[F[_]: Sync: Clock](
    rawCounter: Counter,
    goodCounter: Counter,
    badCounter: Counter,
    latencyRef: Ref[F, Option[(Long, Long)]],
    reporter: Stream[F, Unit],
    initLatencyGauge: F[Unit]
  ): Metrics[F] =
    new Metrics[F] {
      def report: Stream[F, Unit] = reporter

      def enrichLatency(collectorTstamp: Option[Long]): F[Unit] =
        collectorTstamp match {
          case Some(tstamp) =>
            for {
              now <- Clock[F].realTime(MILLISECONDS)
              _ <- latencyRef.set(Some((now, tstamp)))
              _ <- initLatencyGauge // re-initialize the gauge, because it is deleted after each report.
            } yield ()
          case None =>
            Sync[F].unit
        }

      def rawCount: F[Unit] =
        Sync[F].delay(rawCounter.inc())

      def goodCount: F[Unit] =
        Sync[F].delay(goodCounter.inc())

      def badCount: F[Unit] =
        Sync[F].delay(badCounter.inc())
    }

  def latencyGauge[F[_]: Effect](ref: Ref[F, Option[(Long, Long)]]): MetricRegistry.MetricSupplier[Gauge[_]] =
    effectGauge(ref.get) {
      case Some((acked, collectorTstamp)) => acked - collectorTstamp
      case None => 0L
    }.asInstanceOf[MetricRegistry.MetricSupplier[Gauge[_]]]

  def effectGauge[F[_]: Effect, E, O](effect: F[E])(transform: E => O): MetricRegistry.MetricSupplier[Gauge[O]] =
    new MetricRegistry.MetricSupplier[Gauge[O]] {
      def newMetric(): Gauge[O] =
        new Gauge[O] {
          def getValue: O = Effect[F].toIO(effect.map(transform)).unsafeRunSync()
        }
    }

  def noop[F[_]: Applicative]: Metrics[F] =
    new Metrics[F] {
      def report: Stream[F, Unit] = Stream.empty.covary[F]
      def enrichLatency(collectorTstamp: Option[Long]): F[Unit] = Applicative[F].unit
      def rawCount: F[Unit] = Applicative[F].unit
      def goodCount: F[Unit] = Applicative[F].unit
      def badCount: F[Unit] = Applicative[F].unit
    }

}
