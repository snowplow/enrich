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
package com.snowplowanalytics.snowplow.enrich.common.fs2

import java.net.URI
import java.nio.file.Paths

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

import scala.concurrent.duration._

import fs2.Stream
import fs2.io.file.{exists, readAll}

import cats.effect.{Blocker, Concurrent, ContextShift, IO, Resource, Timer}

import cats.effect.testing.specs2.CatsIO

import com.snowplowanalytics.snowplow.enrich.common.utils.BlockerF

import com.snowplowanalytics.snowplow.enrich.common.fs2.test._
import com.snowplowanalytics.snowplow.enrich.common.fs2.Assets.Asset

class AssetsSpec extends Specification with CatsIO with ScalaCheck {

  sequential

  "updateStream" should {
    "not set stop signal if no updates required" in
      AssetsSpec.run(1.second) { (state, run) =>
        run(100.millis, List.empty) *> state.pauseEnrich.get.map { pause =>
          pause must beFalse
        }
      }

    "download an asset and leave pauseEnrich signal with false" in {
      val path = Paths.get("asset")
      val input = List(
        (URI.create("http://localhost:8080/asset"), path.toString)
      )
      AssetsSpec.run(1500.millis) { (state, run) =>
        for {
          assetExistsBefore <- Blocker[IO].use(b => exists[IO](b, path))
          _ <- run(100.millis, input)
          pauseEnrich <- state.pauseEnrich.get
          assetExists <- Blocker[IO].use(b => exists[IO](b, path))
        } yield {
          assetExistsBefore must beFalse // Otherwise previous execution left the file
          pauseEnrich must beFalse
          assetExists must beTrue
        }
      }
    }

    "set stop signal to true when long downloads are performed" in {
      val input = List(
        (URI.create("http://localhost:8080/slow"), "asset1"), // First sets stop to true
        (URI.create("http://localhost:8080/slow"), "asset2") // Second doesn't allow update to return prematurely
      )
      AssetsSpec.run(3.seconds) { (state, run) =>
        for {
          fiber <- (IO.sleep(2.seconds) *> state.pauseEnrich.get).start
          _ <- run(500.milliseconds, input)
          stop <- fiber.join
        } yield stop must beTrue
      }
    }

    "attempt to re-download non-existing file" in {
      val path = Paths.get("flaky-asset")
      val input = List(
        (URI.create("http://localhost:8080/flaky"), path.toString)
      )
      AssetsSpec.run(5.seconds) { (state, run) =>
        for {
          _ <- run(800.millis, input)
          stop <- state.pauseEnrich.get
          assetExists <- Blocker[IO].use { b =>
                           readAll[IO](path, b, 8).compile.to(Array).map(arr => new String(arr))
                         }
        } yield {
          stop must beFalse
          assetExists must beEqualTo("3")
        }
      }
    }
  }

  "Hash.fromStream" should {
    "always create a valid MD5 hash" in {
      prop { (bytes: Array[Byte]) =>
        val input = Stream.emits(bytes).covary[IO]
        Assets.Hash.fromStream(input).map { hash =>
          hash.s.matches("^[a-f0-9]{32}$") must beTrue
        }
      }
    }
  }
}

object AssetsSpec {

  /** Run assets refresh function with specified refresh interval and list of assets */
  type Run = (FiniteDuration, List[Asset]) => IO[Unit]

  /**
   * User-written function to test effects of [[Assets]] stream
   * * First argument - state initialised to empty, can be inspected after
   * * Second argument - [[Run]] function to specify custom refresh interval and list of assets
   */
  type Test[A] = (Assets.State[IO], Run) => IO[A]

  /**
   * Run a test with resources allocated specifically for it
   * It will allocate thread pool, empty state, HTTP server and will
   * automatically remove all files after the test is over
   *
   * @param time timeout after which the test will be forced to exit
   * @param test the actual test suite function
   */
  def run[A](
    time: FiniteDuration
  )(
    test: Test[A]
  )(
    implicit C: Concurrent[IO],
    T: Timer[IO],
    CS: ContextShift[IO]
  ): IO[A] = {
    val resources = for {
      blocker <- Blocker[IO]
      state <- SpecHelpers.refreshState(List(URI.create("http://localhost:8080") -> "index"))
      enrichments <- Environment.Enrichments.make[IO](List(), BlockerF.noop)
      path <- Resource.eval(Assets.getCurDir[IO])
      _ <- SpecHelpers.filesResource(blocker, TestFiles)
    } yield (blocker, state, enrichments, path)

    resources.use {
      case (blocker, state, enrichments, curDir) =>
        val testFunction: Run = Assets
          .updateStream[IO](blocker, state, enrichments, curDir, _, _)
          .withHttp
          .haltAfter(time)
          .compile
          .drain
        test(state, testFunction)
    }
  }

  /** List of local files that have to be deleted after every test */
  private val TestFiles = List(
    Paths.get("asset"),
    Paths.get("asset1"),
    Paths.get("asset2"),
    Paths.get("flaky-asset")
  )
}
