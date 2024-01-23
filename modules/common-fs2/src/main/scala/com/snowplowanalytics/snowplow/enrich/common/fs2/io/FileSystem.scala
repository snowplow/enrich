/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.io

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._
import scala.io.{Source => SSource}

import cats.data.EitherT

import cats.effect.Sync
import cats.implicits._

import fs2.Stream

import _root_.io.circe.Decoder
import _root_.io.circe.config.syntax._

import com.typesafe.config.{Config => TSConfig, ConfigFactory}

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object FileSystem {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def list[F[_]: Sync](dir: Path): Stream[F, Path] =
    for {
      paths <- Stream.eval(Sync[F].delay(Files.list(dir)))
      path <- Stream.fromIterator(paths.iterator().asScala, 1)
    } yield path

  def readJson[F[_]: Sync, A: Decoder](path: Path, fallbacks: TSConfig => TSConfig): EitherT[F, String, A] =
    Sync[F]
      .delay(SSource.fromFile(path.toFile).mkString)
      .attemptT
      .leftMap(e => s"Cannot read file: ${e.getMessage}")
      .subflatMap { text =>
        val either = for {
          tsConfig <- Either.catchNonFatal(ConfigFactory.parseString(text)).leftMap(_.getMessage)
          tsConfig <- Either.catchNonFatal(tsConfig.resolve()).leftMap(e => s"Can't resolve config: ${e.getMessage}")
          tsConfig <- Either.catchNonFatal(fallbacks(tsConfig)).leftMap(_.getMessage)
          parsed <- tsConfig.as[A].leftMap(_.show)
        } yield parsed
        either.leftMap(reason => s"Cannot parse file $path: $reason")
      }

  def readJsonDir[F[_]: Sync, A: Decoder](dir: Path): EitherT[F, String, List[A]] =
    list(dir).compile.toList.attemptT
      .leftMap(e => show"Cannot list ${dir.toAbsolutePath.toString} directory with JSON: ${e.getMessage}")
      .map(_.filter { path =>
        val asStr = path.toString
        asStr.endsWith(".json") || asStr.endsWith(".hocon")
      })
      .flatMap { paths =>
        EitherT.liftF[F, String, Unit](Logger[F].info(s"Files found in $dir: ${paths.mkString(", ")}")) *>
          paths.traverse(p => readJson[F, A](p, identity))
      }
}
