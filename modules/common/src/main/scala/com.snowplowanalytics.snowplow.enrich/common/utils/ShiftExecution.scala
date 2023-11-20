/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import cats.effect.{ContextShift, Resource, Sync}

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

/**
 * Shifts execution to a dedicated thread pool
 *
 * This is needed for running blocking JDBC operations. Inspired by Doobie's threading model:
 *
 * > The reason for having separate pools for awaiting connections and executing JDBC operations is
 * > liveness - we must avoid the situation where all the threads in the pool are blocked on
 * > acquiring a JDBC connection, meaning that no logical threads are able to make progress and
 * > release the connection they’re currently holding.
 *
 * See https://tpolecat.github.io/doobie/docs/14-Managing-Connections.html#about-threading
 *
 * This typeclass is a bit of a hack while we are supporting Id and IO instances. It should be
 * removed when we deprecate stream-enrich.
 */
trait ShiftExecution[F[_]] {

  def shift[A](f: F[A]): F[A]

}

object ShiftExecution {

  // A single thread is adequate because our Hikari connection pool has a single connection
  def ofSingleThread[F[_]: ContextShift: Sync]: Resource[F, ShiftExecution[F]] =
    for {
      es <- Resource.make(Sync[F].delay(Executors.newSingleThreadExecutor))(e => Sync[F].delay(e.shutdown))
      ec <- Resource.eval(Sync[F].delay(ExecutionContext.fromExecutorService(es)))
    } yield new ShiftExecution[F] {
      def shift[A](f: F[A]): F[A] =
        ContextShift[F].evalOn(ec)(f)
    }
}
