/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.test

import cats.Monad
import cats.syntax.flatMap._

import cats.effect.kernel.{Clock, Ref, Sync}

import fs2.Stream

import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Metrics

/** Metrics container for testing */
case class Counter(
  raw: Long,
  good: Long,
  bad: Long,
  dropped: Long,
  incomplete: Long,
  latency: Option[Long],
  invalid: Long,
  remoteAdaptersSuccessCount: Option[Long],
  remoteAdaptersFailureCount: Option[Long],
  remoteAdaptersTimeoutCount: Option[Long]
)

object Counter {
  val empty: Counter = Counter(0L, 0L, 0L, 0L, 0L, None, 0L, None, None, None)

  def make[F[_]: Sync]: F[Ref[F, Counter]] =
    Ref.of[F, Counter](empty)

  /** Create a pure metrics with mutable state */
  def mkCounterMetrics[F[_]: Monad: Clock](ref: Ref[F, Counter]): Metrics[F] =
    new Metrics[F] {
      def report: Stream[F, Unit] = Stream.empty.covary[F]

      def enrichLatency(collectorTstamp: Option[Long]): F[Unit] =
        Clock[F].realTime.flatMap { now =>
          ref.update(_.copy(latency = collectorTstamp.map(ct => now.toMillis - ct)))
        }

      def rawCount(nb: Int): F[Unit] =
        ref.update(cnt => cnt.copy(raw = cnt.raw + nb))

      def goodCount(nb: Int): F[Unit] =
        ref.update(cnt => cnt.copy(good = cnt.good + nb))

      def badCount(nb: Int): F[Unit] =
        ref.update(cnt => cnt.copy(bad = cnt.bad + nb))

      def droppedCount(nb: Int): F[Unit] =
        ref.update(cnt => cnt.copy(dropped = cnt.dropped + nb))

      def incompleteCount(nb: Int): F[Unit] =
        ref.update(cnt => cnt.copy(incomplete = cnt.incomplete + nb))

      def invalidCount: F[Unit] =
        ref.update(cnt => cnt.copy(invalid = cnt.invalid + 1))

      def remoteAdaptersSuccessCount: F[Unit] =
        ref.update(cnt => cnt.copy(remoteAdaptersSuccessCount = cnt.remoteAdaptersSuccessCount.map(_ + 1)))

      def remoteAdaptersFailureCount: F[Unit] =
        ref.update(cnt => cnt.copy(remoteAdaptersFailureCount = cnt.remoteAdaptersFailureCount.map(_ + 1)))

      def remoteAdaptersTimeoutCount: F[Unit] =
        ref.update(cnt => cnt.copy(remoteAdaptersTimeoutCount = cnt.remoteAdaptersTimeoutCount.map(_ + 1)))
    }
}
