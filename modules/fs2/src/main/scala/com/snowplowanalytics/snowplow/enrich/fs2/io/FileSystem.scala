/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.fs2.io

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import cats.data.EitherT

import cats.effect.Sync
import cats.implicits._

import fs2.Stream

import _root_.io.circe.Json
import _root_.io.circe.parser.parse

object FileSystem {

  def readJson[F[_]: Sync](path: Path): EitherT[F, String, Json] =
    EitherT(Sync[F].delay(Files.readString(path)).map(parse))
      .leftMap(e => show"Cannot read resolver config. ${e.getMessage()}")

  def readJsonDir[F[_]: Sync](dir: Path): EitherT[F, String, List[Json]] = {
    val files = for {
      paths <- Stream.eval(Sync[F].delay(Files.list(dir)))
      path <- Stream.fromIterator(paths.iterator().asScala)
    } yield path
    val action = files.compile.toList
      .flatMap(paths => paths.traverse(x => readJson(x).value))
      .map(_.sequence)
    EitherT(action)
  }

}
