/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
