/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
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

import cats.effect.{Sync, Resource}
import cats.implicits._

import com.codahale.metrics.{Slf4jReporter, MetricRegistry, Gauge}
import org.slf4j.LoggerFactory

trait Metrics[F[_]] {
  def report: F[Unit]
  /** Update latency */
  def enrichLatency(collectorTstamp: Option[Long]): F[Unit]

  def rawCount: F[Unit]
  def goodCount: F[Unit]
  def badCount: F[Unit]
}

object Metrics {

  val LoggerName = "enrich.metrics"
  val LatencyGaugeName = "enrich.metrics.latency"
  val RawCounterName = "enrich.metrics.raw.count"
  val GoodCounterName = "enrich.metrics.good.count"
  val BadCounterName = "enrich.metrics.bad.count"

  def initialise[F[_]: Sync] =
    for {
      registry <- Sync[F].delay(new MetricRegistry())
      logger <- Sync[F].delay(LoggerFactory.getLogger(LoggerName))
      reporter <- Sync[F].delay(Slf4jReporter.forRegistry(registry).outputTo(logger).build())
    } yield (reporter, registry)

  /**
   * Technically `Resource` doesn't give us much as we don't allocate a thread pool,
   * but it will make sure the last report is issued
   */
  def resource[F[_]: Sync]: Resource[F, Metrics[F]] =
    Resource
      .make(initialise) { case (reporter, _) => Sync[F].delay(reporter.close()) }
      .map { case (reporter, registry) =>
        new Metrics[F] {
          private val rawCounter = registry.counter(RawCounterName)
          private val goodCounter = registry.counter(GoodCounterName)
          private val badCounter = registry.counter(BadCounterName)

          def report: F[Unit] =
            Sync[F].delay(reporter.report())

          def enrichLatency(collectorTstamp: Option[Long]): F[Unit] = {
            collectorTstamp match {
              case Some(delay) =>
                Sync[F].delay {
                  val current = System.currentTimeMillis() - delay
                  registry.remove(LatencyGaugeName)
                  val _ = registry.register(LatencyGaugeName, new Gauge[Long] {
                    def getValue: Long = current
                  })
                }
              case None =>
                Sync[F].unit
            }
          }

          def rawCount: F[Unit] =
            Sync[F].delay(rawCounter.inc())
          def goodCount: F[Unit] =
            Sync[F].delay(goodCounter.inc())
          def badCount: F[Unit] =
            Sync[F].delay(badCounter.inc())
        }
      }
}
