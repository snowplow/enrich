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
import cats.{Applicative, Monad}
import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Clock, ConcurrentEffect, ContextShift, Resource, Sync, Timer}

import fs2.Stream

import scala.concurrent.duration.MILLISECONDS

import com.snowplowanalytics.snowplow.enrich.fs2.config.io.MetricsReporter

import _root_.io.chrisdavenport.log4cats.slf4j.Slf4jLogger

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
    config: MetricsReporter
  ): F[Metrics[F]] =
    for {
      refs <- MetricRefs.init[F]
      rep = reporterStream(blocker, config, refs)
    } yield new Metrics[F] {
      def report: Stream[F, Unit] = rep

      def enrichLatency(collectorTstamp: Option[Long]): F[Unit] =
        collectorTstamp match {
          case Some(tstamp) =>
            for {
              now <- Clock[F].realTime(MILLISECONDS)
              _ <- refs.enrichLatency.set(Some((now - tstamp)))
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
    enrichLatency: Ref[F, Option[Long]],
    rawCount: Ref[F, Int],
    goodCount: Ref[F, Int],
    badCount: Ref[F, Int]
  )

  private object MetricRefs {
    def init[F[_]: Sync]: F[MetricRefs[F]] =
      for {
        enrichLatency <- Ref.of[F, Option[Long]](None)
        rawCounter <- Ref.of[F, Int](0)
        goodCounter <- Ref.of[F, Int](0)
        badCounter <- Ref.of[F, Int](0)
      } yield MetricRefs(enrichLatency, rawCounter, goodCounter, badCounter)

    /**
     * Gets a snapshot and resets the latency gauge.
     * This is because latency must not get "stuck" on a value when enrich is no longer receiving new
     * events.
     */
    def snapshot[F[_]: Monad](refs: MetricRefs[F]): F[MetricSnapshot] =
      for {
        latency <- refs.enrichLatency.getAndUpdate(_ => None)
        rawCount <- refs.rawCount.get
        goodCount <- refs.goodCount.get
        badCount <- refs.badCount.get
      } yield MetricSnapshot(latency, rawCount, goodCount, badCount)
  }

  def reporterStream[F[_]: Sync: Timer: ContextShift](
    blocker: Blocker,
    config: MetricsReporter,
    metrics: MetricRefs[F]
  ): Stream[F, Unit] =
    for {
      rep <- Stream.resource(makeReporter(blocker, config))
      _ <- Stream.fixedDelay[F](config.period)
      snapshot <- Stream.eval(MetricRefs.snapshot(metrics))
      _ <- Stream.eval(rep.report(snapshot))
    } yield ()

  def makeReporter[F[_]: Sync: ContextShift: Timer](
    blocker: Blocker,
    config: MetricsReporter
  ): Resource[F, Reporter[F]] =
    config match {
      case stdout: MetricsReporter.Stdout =>
        for {
          logger <- Resource.eval(Slf4jLogger.fromName[F](LoggerName))
          prefix = stdout.prefix.getOrElse(MetricsReporter.DefaultPrefix)
        } yield new Reporter[F] {
          def report(snapshot: MetricSnapshot): F[Unit] =
            for {
              _ <- logger.info(s"$prefix$RawCounterName = ${snapshot.rawCount}")
              _ <- logger.info(s"$prefix$GoodCounterName = ${snapshot.goodCount}")
              _ <- logger.info(s"$prefix$BadCounterName = ${snapshot.badCount}")
              _ <- snapshot.enrichLatency.map(latency => logger.info(s"$prefix$LatencyGaugeName = $latency")).getOrElse(Sync[F].unit)
            } yield ()
        }
      case statsd: MetricsReporter.StatsD =>
        StatsDReporter.make[F](blocker, statsd)
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
