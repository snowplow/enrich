package com.snowplowanalytics.snowplow.enrich.fs2

import scala.concurrent.duration.FiniteDuration

import cats.effect.{Concurrent, IO, Timer}

import _root_.fs2.Stream

package object test {

  implicit class StreamOps[F[_], A](s: Stream[F, A]) {

    /** Halting a stream after specified period of time */
    def haltAfter(after: FiniteDuration)(implicit T: Timer[F], C: Concurrent[F]): Stream[F, A] =
      Stream.eval_(Timer[F].sleep(after)).mergeHaltL(s)
  }

  implicit class StreamIoOps[A](s: Stream[IO, A]) {

    /** Run test [[HttpServer]] in parallel with the stream */
    def withHttp(implicit C: Concurrent[IO]): Stream[IO, A] =
      s.concurrently(HttpServer.run)
  }
}
