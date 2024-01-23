/*
 * Copyright (c) 2021-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.io

import cats.implicits._
import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.io.{Source => ScalaSource}

import org.specs2.mutable.Specification

class SinkSpec extends Specification with CatsEffect {

  "rotating file sink" should {

    "write to a single file if max bytes is not exceeded" in {
      val dir = Files.createTempDirectory("enrich-sink-spec")
      val maxBytes = 100L

      val write = FileSink.rotatingFileSink[IO](dir.resolve("out"), maxBytes).use { sink =>
        sink(List("AAAAA".getBytes, "BBBBB".getBytes, "CCCCC".getBytes))
      }

      for {
        _ <- write
        written <- filesInDir(dir)
        withContent <- zipWithContent(written)
      } yield {
        withContent must have size 1
        val (path, content) = withContent.head

        path.getFileName.toString must be_==("out.0001")
        content must_== (List("AAAAA", "BBBBB", "CCCCC"))
      }
    }

    "rotate files when max bytes is exceeded" in {
      val dir = Files.createTempDirectory("enrich-sink-spec")
      val maxBytes = 15L

      val write = FileSink.rotatingFileSink[IO](dir.resolve("out"), maxBytes).use { sink =>
        sink(List("AAAAA".getBytes, "BBBBB".getBytes, "CCCCC".getBytes, "DDDDD".getBytes, "EEEEE".getBytes))
      }

      for {
        _ <- write
        written <- filesInDir(dir)
        withContent <- zipWithContent(written)
      } yield {
        withContent must have size 3
        written.map(_.getFileName.toString) must_== (List("out.0001", "out.0002", "out.0003"))

        withContent.map(_._2) must_== (List(
          List("AAAAA", "BBBBB"),
          List("CCCCC", "DDDDD"),
          List("EEEEE")
        ))
      }
    }
  }

  def filesInDir(dir: Path): IO[List[Path]] =
    IO.delay {
      Files.list(dir)
    }.bracket { stream =>
      IO.delay(stream.iterator.asScala.toList)
    } { stream =>
      IO.delay(stream.close())
    }.map(_.sorted)

  def zipWithContent(files: List[Path]): IO[List[(Path, List[String])]] =
    files.traverse { path =>
      IO.delay {
        ScalaSource.fromFile(path.toFile)
      }.bracket { source =>
        IO.delay(source.getLines().toList).map(lines => path -> lines)
      } { source =>
        IO.delay(source.close())
      }
    }

}
