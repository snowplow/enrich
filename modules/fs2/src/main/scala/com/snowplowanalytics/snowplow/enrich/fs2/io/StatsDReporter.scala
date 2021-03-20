/*
 * Copyright (c) 2021 Snowplow Analytics Ltd. All rights reserved.
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

import cats.syntax.show._

import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import java.nio.charset.StandardCharsets.UTF_8

import cats.effect.{Blocker, ContextShift, Resource, Sync, Timer => CatsTimer}
import fs2.{Pure, Stream}
import com.codahale.metrics._
import scala.jdk.CollectionConverters._

import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.enrich.fs2.config.io.MetricsReporter

/**
 * Reports metrics to a StatsD server over UDP
 *
 * We use the DogStatsD extension to the StatsD protocol, which adds arbitrary key-value tags to the metric, e.g:
 * `snowplow.enrich.good.count:20000|g|#app_id:12345,env:prod`
 */
object StatsDReporter {

  /**
   * A stream which periodically sends metrics from the registry to the StatsD server.
   *
   * The stream calls `InetAddress.getByName` each time there is a new batch of metrics. This allows
   * the run-time to resolve the address to a new IP address, in case DNS records change.  This is
   * necessary in dynamic container environments (Kubernetes) where the statsd server could get
   * restarted at a new IP address.
   *
   * Note, InetAddress caches name resolutions, (see https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/net/InetAddress.html)
   * so there could be a delay in following a DNS record change.  For the Docker image we release
   * the cache time is 30 seconds.
   */
  def stream[F[_]: Sync: ContextShift: CatsTimer](
    blocker: Blocker,
    config: MetricsReporter.StatsD,
    registry: MetricRegistry
  ): Stream[F, Unit] =
    for {
      logger <- Stream.eval(Slf4jLogger.create[F])
      socket <- Stream.resource(Resource.fromAutoCloseableBlocking(blocker)(Sync[F].delay(new DatagramSocket)))
      _ <- Stream.fixedDelay(config.period)
      inetAddr <- Stream.eval(blocker.delay(InetAddress.getByName(config.hostname))).handleErrorWith(logAndAbort(logger))
      _ <- serializedStream(registry, config)
             .covary[F]
             .evalMap(sendMetric[F](blocker, socket, inetAddr, config.port))
             .handleErrorWith(logAndAbort(logger))
    } yield ()

  def logAndAbort[F[_]: Sync](logger: Logger[F])(t: Throwable): Stream[F, Nothing] =
    Stream.eval(Sync[F].delay(logger.error(t)("Caught exception sending metrics"))).drain

  def serializedStream(registry: MetricRegistry, config: MetricsReporter.StatsD): Stream[Pure, String] =
    kvMetrics(registry).map(statsDFormat(config))

  def sendMetric[F[_]: ContextShift: Sync](
    blocker: Blocker,
    socket: DatagramSocket,
    addr: InetAddress,
    port: Int
  )(
    m: String
  ): F[Unit] = {
    val bytes = m.getBytes(UTF_8)
    val packet = new DatagramPacket(bytes, bytes.length, addr, port)
    blocker.delay(socket.send(packet))
  }

  final case class KVMetric(key: String, value: String)

  def kvMetrics(registry: MetricRegistry): Stream[Pure, KVMetric] =
    kvGauges(registry.getGauges.asScala.toSeq) ++
      kvCounters(registry.getCounters.asScala.toSeq) ++
      kvHistograms(registry.getHistograms.asScala.toSeq) ++
      kvMeters(registry.getMeters.asScala.toSeq) ++
      kvTimers(registry.getTimers.asScala.toSeq)

  /* Handlers for the five DropWizard metric classes */

  def kvGauges(gauges: Seq[(String, Gauge[_])]): Stream[Pure, KVMetric] =
    Stream
      .emits(gauges)
      .map { case (k, v) => k -> v.getValue }
      .collect {
        case (k, v: Int) => KVMetric(k, v.toLong.show)
        case (k, v: Long) => KVMetric(k, v.show)
        case (k, v: Double) => KVMetric(k, v.show)
        case (k, v: String) => KVMetric(k, v)
      }

  // Counters implement the `Counting` interface
  def kvCounters(counters: Seq[(String, Counter)]): Stream[Pure, KVMetric] =
    Stream.emits(counters).flatMap {
      case (k, v) =>
        kvCounting(k, v)
    }

  // Counters implement the `Counting` and `Sampling` interfaces
  def kvHistograms(histograms: Seq[(String, Histogram)]): Stream[Pure, KVMetric] =
    Stream.emits(histograms).flatMap {
      case (k, v) =>
        kvCounting(k, v) ++ kvSampling(k, v)
    }

  // Meters implement the `Counting` and `Metered` interfaces
  def kvMeters(meters: Seq[(String, Meter)]): Stream[Pure, KVMetric] =
    Stream.emits(meters).flatMap {
      case (k, v) =>
        kvCounting(k, v) ++ kvMetered(k, v)
    }

  // Timers implement the `Counting`, `Metered` and `Sampling` interfaces
  def kvTimers(timers: Seq[(String, Timer)]): Stream[Pure, KVMetric] =
    Stream.emits(timers).flatMap {
      case (k, v) =>
        kvCounting(k, v) ++ kvMetered(k, v) ++ kvSampling(k, v)
    }

  /* Handlers for the traits implemented by the Metric classes */

  def kvCounting(key: String, counting: Counting): Stream[Pure, KVMetric] =
    Stream.emit(KVMetric(s"$key.count", counting.getCount.show))

  def kvMetered(key: String, metered: Metered): Stream[Pure, KVMetric] =
    Stream(
      KVMetric(s"$key.fifteenMinuteRate", metered.getFifteenMinuteRate.show),
      KVMetric(s"$key.fiveMinuteRate", metered.getFiveMinuteRate.show),
      KVMetric(s"$key.oneMinuteRate", metered.getOneMinuteRate.show),
      KVMetric(s"$key.meanRate", metered.getMeanRate.show)
    )

  def kvSampling(key: String, sampling: Sampling): Stream[Pure, KVMetric] = {
    val snapshot = sampling.getSnapshot
    Stream(
      KVMetric(s"$key.min", snapshot.getMin.show),
      KVMetric(s"$key.max", snapshot.getMax.show),
      KVMetric(s"$key.mean", snapshot.getMean.show),
      KVMetric(s"$key.median", snapshot.getMedian.show),
      KVMetric(s"$key.stdDev", snapshot.getStdDev.show),
      KVMetric(s"$key.size", snapshot.size.toLong.show),
      KVMetric(s"$key.75thPercentile", snapshot.get75thPercentile.show),
      KVMetric(s"$key.95thPercentile", snapshot.get95thPercentile.show),
      KVMetric(s"$key.98thPercentile", snapshot.get98thPercentile.show),
      KVMetric(s"$key.99thPercentile", snapshot.get99thPercentile.show),
      KVMetric(s"$key.999thPercentile", snapshot.get999thPercentile.show)
    )
  }

  private def statsDFormat(config: MetricsReporter.StatsD): KVMetric => String = {
    val tagStr = config.tags.map { case (k, v) => s"$k:$v" }.mkString(",")
    val prefix = config.prefix.getOrElse(MetricsReporter.DefaultPrefix)
    kv => s"${prefix}${kv.key}:${kv.value}|g|#$tagStr"
  }

}
