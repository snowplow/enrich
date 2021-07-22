/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.fs2.io

import java.nio.ByteBuffer
import java.nio.file.{Path, StandardOpenOption}
import java.nio.channels.FileChannel

import cats.implicits._

import cats.effect.{Blocker, Concurrent, ContextShift, Resource, Sync}
import cats.effect.concurrent.Semaphore

import com.snowplowanalytics.snowplow.enrich.common.fs2.ByteSink

object Sink {

  def fileSink[F[_]: Concurrent: ContextShift](path: Path, blocker: Blocker): ByteSink[F] = {
    val resource =
      for {
        channel <- Resource.fromAutoCloseableBlocking(blocker)(
                     Sync[F].delay(FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))
                   )
        sem <- Resource.eval(Semaphore(1L))
      } yield (channel, sem)

    bytes =>
      resource.use {
        case (channel, sem) =>
          sem.withPermit {
            blocker.delay {
              channel.write(ByteBuffer.wrap(bytes))
              channel.write(ByteBuffer.wrap(Array('\n'.toByte)))
            }.void
          }
      }
  }
}
