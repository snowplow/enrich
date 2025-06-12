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
package com.snowplowanalytics.snowplow.enrich.streams.common

import scala.collection.JavaConverters._

import java.nio.file.{Files, Path, Paths}
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.PosixFilePermission

import cats.syntax.either._

import cats.implicits._

import cats.effect.{IO, Resource}

import io.circe.parser

import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.iglu.core.SelfDescribingData

object Utils {

  def parseBadRow(s: String): Either[String, BadRow] =
    for {
      json <- parser.parse(s).leftMap(_.message)
      sdj <- json.as[SelfDescribingData[BadRow]].leftMap(_.message)
    } yield sdj.data

  def writeEnrichmentsConfigsToDisk(enrichments: List[Enrichment]): Resource[IO, Path] =
    for {
      path <- Resource.make(IO.blocking(Files.createTempDirectory("")))(path => IO.blocking(Files.delete(path)))
      allPermissions = Set(
                         PosixFilePermission.OWNER_READ,
                         PosixFilePermission.OWNER_WRITE,
                         PosixFilePermission.OWNER_EXECUTE,
                         PosixFilePermission.GROUP_READ,
                         PosixFilePermission.GROUP_WRITE,
                         PosixFilePermission.GROUP_EXECUTE,
                         PosixFilePermission.OTHERS_READ,
                         PosixFilePermission.OTHERS_WRITE,
                         PosixFilePermission.OTHERS_EXECUTE
                       ).asJava
      _ <- Resource.eval(IO.blocking(Files.setPosixFilePermissions(path, allPermissions)))
      _ <- enrichments.traverse { enrichment =>
             val enrichmentPath = Paths.get(path.toAbsolutePath.toString, enrichment.fileName)
             Resource.make(
               IO.blocking(Files.write(enrichmentPath, enrichment.config.getBytes(StandardCharsets.UTF_8))) >>
                 IO.blocking(Files.setPosixFilePermissions(enrichmentPath, allPermissions))
             )(_ => IO.blocking(Files.delete(enrichmentPath)))
           }
    } yield path
}
