/*
 * Copyright (c) 2019-2021 Snowplow Analytics Ltd. All rights reserved.
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

import cats.effect.{Blocker, ContextShift, Sync}

import cats.implicits._

import fs2.Stream
import fs2.io.file.{directoryStream, readAll}

import java.nio.file.{Files, Path}

import com.snowplowanalytics.snowplow.enrich.common.fs2.{Payload, RawSource}

object Source {

  def filesystem[F[_]: Sync: ContextShift](blocker: Blocker, path: Path): RawSource[F] =
    recursiveDirectoryStream(blocker, path)
      .evalMap { file =>
        readAll[F](file, blocker, 4096).compile
          .to(Array)
          .map(bytes => Payload(bytes, Sync[F].unit))
      }

  private def recursiveDirectoryStream[F[_]: Sync: ContextShift](blocker: Blocker, path: Path): Stream[F, Path] =
    for {
      subPath <- directoryStream(blocker, path)
      isDir <- Stream.eval(blocker.delay(Files.isDirectory(subPath)))
      file <- if (isDir) recursiveDirectoryStream(blocker, subPath) else Stream.emit(subPath)
    } yield file
}
