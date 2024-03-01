/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import cats.effect.kernel.{Async, Resource, Sync}

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
  def ofSingleThread[F[_]: Async]: Resource[F, ShiftExecution[F]] =
    for {
      es <- Resource.make(Sync[F].delay(Executors.newSingleThreadExecutor))(e => Sync[F].delay(e.shutdown))
      ec <- Resource.eval(Sync[F].delay(ExecutionContext.fromExecutorService(es)))
    } yield new ShiftExecution[F] {
      def shift[A](f: F[A]): F[A] =
        Async[F].evalOn(f, ec)
    }
}
