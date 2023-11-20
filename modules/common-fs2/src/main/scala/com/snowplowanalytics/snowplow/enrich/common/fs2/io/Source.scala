/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
