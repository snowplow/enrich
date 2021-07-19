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
package com.snowplowanalytics.snowplow.enrich.common.fs2.io

import cats.implicits._
import cats.{Applicative, Monad}
import cats.effect.concurrent.Ref
import cats.effect.{Async, Blocker, Clock, ConcurrentEffect, ContextShift, Resource, Sync, Timer}

import fs2.Stream

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.MetricsReporters

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

  final case class MetricSnapshot(
    enrichLatency: Option[Long],
    rawCount: Int,
    goodCount: Int,
    badCount: Int
  )

  trait Reporter[F[_]] {
    def report(snapshot: MetricSnapshot): F[Unit]
  }

  def build[F[_]: ContextShift: ConcurrentEffect: Timer](
    blocker: Blocker,
    config: MetricsReporters
  ): F[Metrics[F]] =
    config match {
      case MetricsReporters(None, None) => noop[F].pure[F]
      case MetricsReporters(statsd, stdout) => impl[F](blocker, statsd, stdout)
    }

  private def impl[F[_]: ContextShift: ConcurrentEffect: Timer](
    blocker: Blocker,
    statsd: Option[MetricsReporters.StatsD],
    stdout: Option[MetricsReporters.Stdout]
  ): F[Metrics[F]] =
    for {
      refs <- MetricRefs.init[F]
    } yield new Metrics[F] {
      def report: Stream[F, Unit] = {

        val rep1 = statsd
          .map { config =>
            reporterStream(StatsDReporter.make[F](blocker, config), refs, config.period)
          }
          .getOrElse(Stream.never[F])

        val rep2 = stdout
          .map { config =>
            reporterStream(Resource.eval(stdoutReporter(config)), refs, config.period)
          }
          .getOrElse(Stream.never[F])

        rep1.concurrently(rep2)
      }

      def enrichLatency(collectorTstamp: Option[Long]): F[Unit] =
        collectorTstamp match {
          case Some(tstamp) =>
            for {
              now <- Clock[F].realTime(MILLISECONDS)
              _ <- refs.enrichTimestamps.set(Some(now -> tstamp))
            } yield ()
          case None =>
            Sync[F].unit
        }

      def rawCount: F[Unit] =
        refs.rawCount.update(_ + 1)

      def goodCount: F[Unit] =
        refs.goodCount.update(_ + 1)

      def badCount: F[Unit] =
        refs.badCount.update(_ + 1)
    }

  private final case class MetricRefs[F[_]](
    enrichTimestamps: Ref[F, Option[(Long, Long)]], // A ref of when event was last processed, and its collector timestamp
    rawCount: Ref[F, Int],
    goodCount: Ref[F, Int],
    badCount: Ref[F, Int]
  )

  private object MetricRefs {
    def init[F[_]: Sync]: F[MetricRefs[F]] =
      for {
        enrichTimestamps <- Ref.of[F, Option[(Long, Long)]](None)
        rawCounter <- Ref.of[F, Int](0)
        goodCounter <- Ref.of[F, Int](0)
        badCounter <- Ref.of[F, Int](0)
      } yield MetricRefs(enrichTimestamps, rawCounter, goodCounter, badCounter)

    def snapshot[F[_]: Monad](refs: MetricRefs[F], minTime: Long): F[MetricSnapshot] =
      for {
        timestampsOpt <- refs.enrichTimestamps.get
        rawCount <- refs.rawCount.get
        goodCount <- refs.goodCount.get
        badCount <- refs.badCount.get
      } yield {
        // Only report the latency if it was recorded more recently than minTime
        val latency = timestampsOpt match {
          case Some((enrichTstamp, collectorTstamp)) if enrichTstamp > minTime => Some(enrichTstamp - collectorTstamp)
          case _ => None
        }
        MetricSnapshot(latency, rawCount, goodCount, badCount)
      }
  }

  def reporterStream[F[_]: Sync: Timer: ContextShift](
    reporter: Resource[F, Reporter[F]],
    metrics: MetricRefs[F],
    period: FiniteDuration
  ): Stream[F, Unit] =
    for {
      rep <- Stream.resource(reporter)
      lastReportRef <- Stream.eval(Clock[F].realTime(MILLISECONDS)).evalMap(Ref.of(_))
      _ <- Stream.fixedDelay[F](period)
      lastReport <- Stream.eval(Clock[F].realTime(MILLISECONDS)).evalMap(lastReportRef.getAndSet(_))
      snapshot <- Stream.eval(MetricRefs.snapshot(metrics, lastReport))
      _ <- Stream.eval(rep.report(snapshot))
    } yield ()

  def stdoutReporter[F[_]: Sync: ContextShift: Timer](
    config: MetricsReporters.Stdout
  ): F[Reporter[F]] =
    for {
      logger <- Slf4jLogger.fromName[F](LoggerName)
    } yield new Reporter[F] {
      def report(snapshot: MetricSnapshot): F[Unit] =
        for {
          _ <- logger.info(s"${MetricsReporters.normalizeMetric(config.prefix, RawCounterName)} = ${snapshot.rawCount}")
          _ <- logger.info(s"${MetricsReporters.normalizeMetric(config.prefix, GoodCounterName)} = ${snapshot.goodCount}")
          _ <- logger.info(s"${MetricsReporters.normalizeMetric(config.prefix, BadCounterName)} = ${snapshot.badCount}")
          _ <- snapshot.enrichLatency
                 .map(latency => logger.info(s"${MetricsReporters.normalizeMetric(config.prefix, LatencyGaugeName)} = $latency"))
                 .getOrElse(Sync[F].unit)
        } yield ()
    }

  def noop[F[_]: Async]: Metrics[F] =
    new Metrics[F] {
      def report: Stream[F, Unit] = Stream.never[F]
      def enrichLatency(collectorTstamp: Option[Long]): F[Unit] = Applicative[F].unit
      def rawCount: F[Unit] = Applicative[F].unit
      def goodCount: F[Unit] = Applicative[F].unit
      def badCount: F[Unit] = Applicative[F].unit
    }

}
