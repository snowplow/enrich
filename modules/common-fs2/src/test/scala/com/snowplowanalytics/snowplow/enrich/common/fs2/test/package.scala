/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2

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
    def withHttp: Stream[IO, A] =
      Stream.resource(HttpServer.resource).flatMap(_ => s)
  }
}
