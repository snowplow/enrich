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

import java.util.concurrent.TimeUnit
import java.util.SortedMap

import cats.effect.{Resource, Sync}
import com.codahale.metrics._
import com.timgroup.statsd.{NonBlockingStatsDClient, NonBlockingStatsDClientBuilder}
import scala.jdk.CollectionConverters._

import com.snowplowanalytics.snowplow.enrich.fs2.config.io.MetricsReporter

/**
 * A reporter that sends metrics to a statsd server.
 *
 * The reporter is backed by a third-party StatsD client, which is non-blocking and runs on a dedicated thread.
 *
 * We use the DogStatsD extension to the StatsD protocol, which adds arbitrary key-value tags to the metric, e.g:
 * `snowplow.enrich.good.count:20000|g|#app_id:12345,env:prod`
 *
 * @param registry The metric registry we report on
 * @param client The statsD client
 * @param tags Arbitrary tags to add to each metric
 */
class StatsDReporter(
  registry: MetricRegistry,
  client: NonBlockingStatsDClient,
  tags: List[String]
) extends ScheduledReporter(registry, "statsd", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS) {

  override def report(
    gauges: SortedMap[String, Gauge[_]],
    counters: SortedMap[String, Counter],
    histograms: SortedMap[String, Histogram],
    meters: SortedMap[String, Meter],
    timers: SortedMap[String, Timer]
  ): Unit = {
    reportGauges(gauges)
    reportCounters(counters)
    reportHistograms(histograms)
    reportMeters(meters)
    reportTimers(timers)
  }

  /* Handlers for the five DropWizard metric classes */

  def reportGauges(gauges: SortedMap[String, Gauge[_]]): Unit =
    gauges.asScala
      .map { case (k, v) => k -> v.getValue }
      .foreach {
        case (k, v: Long) => client.gauge(k, v, tags: _*)
        case (k, v: Double) => client.gauge(k, v, tags: _*)
        case _ => ()
      }

  // Counters implement the `Counting` interface
  def reportCounters(counters: SortedMap[String, Counter]): Unit =
    counters.asScala.foreach { case (k, v) => reportCounting(k, v) }

  // Counters implement the `Counting` and `Sampling` interfaces
  def reportHistograms(histograms: SortedMap[String, Histogram]): Unit =
    histograms.asScala.foreach {
      case (k, v) =>
        reportCounting(k, v)
        reportSampling(k, v)
    }

  // Meters implement the `Counting` and `Metered` interfaces
  def reportMeters(meters: SortedMap[String, Meter]): Unit =
    meters.asScala.foreach {
      case (k, v) =>
        reportCounting(k, v)
        reportMetered(k, v)
    }

  // Timers implement the `Counting`, `Metered` and `Sampling` interfaces
  def reportTimers(timers: SortedMap[String, Timer]): Unit =
    timers.asScala.foreach {
      case (k, v) =>
        reportCounting(k, v)
        reportMetered(k, v)
        reportSampling(k, v)
    }

  /* Handlers for the traits implemented by the Metric classes */

  def reportCounting(key: String, counting: Counting): Unit =
    client.gauge(s"$key.count", counting.getCount, tags: _*)

  def reportMetered(key: String, metered: Metered): Unit = {
    client.gauge(s"$key.fifteenMinuteRate", metered.getFifteenMinuteRate, tags: _*)
    client.gauge(s"$key.fiveMinuteRate", metered.getFiveMinuteRate, tags: _*)
    client.gauge(s"$key.oneMinuteRate", metered.getOneMinuteRate, tags: _*)
    client.gauge(s"$key.meanRate", metered.getMeanRate, tags: _*)
  }

  def reportSampling(key: String, sampling: Sampling): Unit = {
    val snapshot = sampling.getSnapshot
    client.gauge(s"$key.min", snapshot.getMin, tags: _*)
    client.gauge(s"$key.max", snapshot.getMax, tags: _*)
    client.gauge(s"$key.mean", snapshot.getMean, tags: _*)
    client.gauge(s"$key.median", snapshot.getMedian, tags: _*)
    client.gauge(s"$key.stdDev", snapshot.getStdDev, tags: _*)
    client.gauge(s"$key.size", snapshot.size.toLong, tags: _*)
    client.gauge(s"$key.75thPercentile", snapshot.get75thPercentile, tags: _*)
    client.gauge(s"$key.95thPercentile", snapshot.get95thPercentile, tags: _*)
    client.gauge(s"$key.98thPercentile", snapshot.get98thPercentile, tags: _*)
    client.gauge(s"$key.99thPercentile", snapshot.get99thPercentile, tags: _*)
    client.gauge(s"$key.999thPercentile", snapshot.get999thPercentile, tags: _*)
  }

}

object StatsDReporter {

  def resource[F[_]: Sync](config: MetricsReporter.StatsD, registry: MetricRegistry): Resource[F, StatsDReporter] =
    for {
      client <- clientResource(config)
      tags = (config.tags).toList.map { case (k, v) => s"$k:$v" }
      reporter <- Resource.fromAutoCloseable(Sync[F].delay(new StatsDReporter(registry, client, tags)))
    } yield reporter

  private def clientResource[F[_]: Sync](config: MetricsReporter.StatsD): Resource[F, NonBlockingStatsDClient] =
    Resource.fromAutoCloseable {
      Sync[F]
        .delay {
          new NonBlockingStatsDClientBuilder()
            .hostname(config.hostname)
            .port(config.port)
            .enableTelemetry(false)
            .build
        }
    }

}
