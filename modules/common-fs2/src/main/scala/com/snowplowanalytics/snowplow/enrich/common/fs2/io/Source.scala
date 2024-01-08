/*
 * Copyright (c) 2019-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.io

import cats.effect.{Blocker, ContextShift, Sync}

import fs2.Stream
import fs2.io.file.{directoryStream, readAll}

import java.nio.file.{Files, Path}

object Source {

  def filesystem[F[_]: ContextShift: Sync](
    blocker: Blocker,
    path: Path
  ): Stream[F, Array[Byte]] =
    recursiveDirectoryStream(blocker, path)
      .evalMap { file =>
        readAll[F](file, blocker, 4096).compile
          .to(Array)
      }

  private def recursiveDirectoryStream[F[_]: ContextShift: Sync](blocker: Blocker, path: Path): Stream[F, Path] =
    for {
      subPath <- directoryStream(blocker, path)
      isDir <- Stream.eval(blocker.delay(Files.isDirectory(subPath)))
      file <- if (isDir) recursiveDirectoryStream(blocker, subPath) else Stream.emit(subPath)
    } yield file
}
