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

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import cats.data.EitherT

import cats.effect.Sync
import cats.implicits._

import fs2.Stream

import _root_.io.circe.Json
import _root_.io.circe.parser.parse

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object FileSystem {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def list[F[_]: Sync](dir: Path): Stream[F, Path] =
    for {
      paths <- Stream.eval(Sync[F].delay(Files.list(dir)))
      path <- Stream.fromIterator(paths.iterator().asScala)
    } yield path

  def readJson[F[_]: Sync](path: Path): EitherT[F, String, Json] =
    Sync[F]
      .delay[String](Files.readString(path))
      .attemptT
      .leftMap(e => show"Error reading ${path.toAbsolutePath.toString} JSON file from filesystem: ${e.getMessage}")
      .subflatMap(str => parse(str).leftMap(e => show"Cannot parse JSON in ${path.toAbsolutePath.toString}: ${e.getMessage()}"))

  def readJsonDir[F[_]: Sync](dir: Path): EitherT[F, String, List[Json]] =
    list(dir).compile.toList.attemptT
      .leftMap(e => show"Cannot list ${dir.toAbsolutePath.toString} directory with JSON: ${e.getMessage}")
      .flatMap { paths =>
        EitherT.liftF[F, String, Unit](Logger[F].info(s"Files found in $dir: ${paths.mkString(", ")}")) *>
          paths.filter(_.toString.endsWith(".json")).traverse(readJson[F])
      }
}
