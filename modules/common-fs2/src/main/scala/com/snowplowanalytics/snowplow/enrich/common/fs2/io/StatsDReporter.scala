/*
 * Copyright (c) 2021-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.io

import cats.implicits._

import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import java.nio.charset.StandardCharsets.UTF_8

import cats.effect.kernel.{Resource, Sync}

import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.MetricsReporters

/**
 * Reports metrics to a StatsD server over UDP
 *
 * We use the DogStatsD extension to the StatsD protocol, which adds arbitrary key-value tags to the metric, e.g:
 * `snowplow.enrich.good.count:20000|g|#app_id:12345,env:prod`
 */
object StatsDReporter {

  /**
   * A reporter which sends metrics from the registry to the StatsD server.
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
  def make[F[_]: Sync](
    config: MetricsReporters.StatsD
  ): Resource[F, Metrics.Reporter[F]] =
    Resource.fromAutoCloseable(Sync[F].delay(new DatagramSocket)).map(impl[F](config, _))

  def impl[F[_]: Sync](
    config: MetricsReporters.StatsD,
    socket: DatagramSocket
  ): Metrics.Reporter[F] =
    new Metrics.Reporter[F] {
      def report(snapshot: Metrics.MetricSnapshot): F[Unit] =
        (for {
          inetAddr <- Sync[F].blocking(InetAddress.getByName(config.hostname))
          _ <- serializedMetrics(snapshot, config).traverse_(sendMetric[F](socket, inetAddr, config.port))
        } yield ()).handleErrorWith { t =>
          for {
            logger <- Slf4jLogger.create[F]
            _ <- Sync[F].delay(logger.error(t)("Caught exception sending metrics"))
          } yield ()
        }
    }

  type KeyValueMetric = (String, String)

  def serializedMetrics(snapshot: Metrics.MetricSnapshot, config: MetricsReporters.StatsD): List[String] =
    keyValues(snapshot).map(statsDFormat(config))

  def keyValues(snapshot: Metrics.MetricSnapshot): List[KeyValueMetric] =
    List(
      Metrics.RawCounterName -> snapshot.rawCount.toString,
      Metrics.GoodCounterName -> snapshot.goodCount.toString,
      Metrics.BadCounterName -> snapshot.badCount.toString,
      Metrics.InvalidCounterName -> snapshot.invalidCount.toString
    ) ++ snapshot.enrichLatency.map(l => Metrics.LatencyGaugeName -> l.toString) ++
      snapshot.incompleteCount.map(cnt => Metrics.IncompleteCounterName -> cnt.toString) ++
      snapshot.remoteAdaptersSuccessCount.map(cnt => Metrics.RemoteAdaptersSuccessCounterName -> cnt.toString) ++
      snapshot.remoteAdaptersFailureCount.map(cnt => Metrics.RemoteAdaptersFailureCounterName -> cnt.toString) ++
      snapshot.remoteAdaptersTimeoutCount.map(cnt => Metrics.RemoteAdaptersTimeoutCounterName -> cnt.toString)

  def sendMetric[F[_]: Sync](
    socket: DatagramSocket,
    addr: InetAddress,
    port: Int
  )(
    m: String
  ): F[Unit] = {
    val bytes = m.getBytes(UTF_8)
    val packet = new DatagramPacket(bytes, bytes.length, addr, port)
    Sync[F].blocking(socket.send(packet))
  }

  private def statsDFormat(config: MetricsReporters.StatsD)(metric: KeyValueMetric): String = {
    val tagStr = config.tags.map { case (k, v) => s"$k:$v" }.mkString(",")
    val metricType = if (metric._1.contains("latency")) "g" else "c"
    s"${MetricsReporters.normalizeMetric(config.prefix, metric._1)}:${metric._2}|$metricType|#$tagStr"
  }
}
