/*
 * Copyright (c) 2019-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.io

import cats.effect.kernel.Async

import fs2.Stream
import fs2.io.file.{Files, Path}

object Source {

  def filesystem[F[_]: Async](
    path: Path
  ): Stream[F, Array[Byte]] =
    recursiveDirectoryStream(path)
      .evalMap { file =>
        Files
          .forAsync[F]
          .readAll(file)
          .compile
          .to(Array)
      }

  private def recursiveDirectoryStream[F[_]: Async](path: Path): Stream[F, Path] =
    for {
      subPath <- Files.forAsync[F].list(path)
      isDir <- Stream.eval(Files.forAsync[F].isDirectory(subPath))
      file <- if (isDir) recursiveDirectoryStream(subPath) else Stream.emit(subPath)
    } yield file
}
