/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery

import cats.effect.kernel.{Resource, Sync}

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

/**
 * This is needed for running blocking JDBC operations. Inspired by Doobie's threading model:
 *
 * > The reason for having separate pools for awaiting connections and executing JDBC operations is
 * > liveness - we must avoid the situation where all the threads in the pool are blocked on
 * > acquiring a JDBC connection, meaning that no logical threads are able to make progress and
 * > release the connection they’re currently holding.
 *
 * See https://tpolecat.github.io/doobie/docs/14-Managing-Connections.html#about-threading
 */
object SqlExecutionContext {

  // A single thread is adequate because our Hikari connection pool has a single connection
  def mk[F[_]: Sync]: Resource[F, ExecutionContext] =
    for {
      es <- Resource.make(Sync[F].delay(Executors.newSingleThreadExecutor))(e => Sync[F].delay(e.shutdown))
      ec <- Resource.eval(Sync[F].delay(ExecutionContext.fromExecutorService(es)))
    } yield ec
}
