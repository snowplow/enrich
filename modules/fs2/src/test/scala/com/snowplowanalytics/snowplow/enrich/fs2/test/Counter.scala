package com.snowplowanalytics.snowplow.enrich.fs2.test

import java.util.concurrent.TimeUnit

import cats.Monad
import cats.syntax.flatMap._

import cats.effect.concurrent.Ref
import cats.effect.{Clock, Sync}

import com.snowplowanalytics.snowplow.enrich.fs2.io.Metrics

/** Metrics container for testing */
case class Counter(
  raw: Long,
  good: Long,
  bad: Long,
  latency: Option[Long]
)

object Counter {
  val empty: Counter = Counter(0L, 0L, 0L, None)

  def make[F[_]: Sync]: F[Ref[F, Counter]] =
    Ref.of[F, Counter](empty)

  /** Create a pure metrics with mutable state */
  def mkCounterMetrics[F[_]: Monad: Clock](ref: Ref[F, Counter]): Metrics[F] =
    new Metrics[F] {
      def report: F[Unit] =
        Monad[F].unit

      def enrichLatency(collectorTstamp: Option[Long]): F[Unit] =
        Clock[F].realTime(TimeUnit.MILLISECONDS).flatMap { now =>
          ref.update(_.copy(latency = collectorTstamp.map(ct => now - ct)))
        }

      def rawCount: F[Unit] =
        ref.update(cnt => cnt.copy(raw = cnt.raw + 1))

      def goodCount: F[Unit] =
        ref.update(cnt => cnt.copy(good = cnt.good + 1))

      def badCount: F[Unit] =
        ref.update(cnt => cnt.copy(bad = cnt.bad + 1))
    }
}
