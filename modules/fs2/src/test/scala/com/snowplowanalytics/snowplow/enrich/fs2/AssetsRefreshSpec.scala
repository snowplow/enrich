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
package com.snowplowanalytics.snowplow.enrich.fs2

import java.net.URI
import java.nio.file.{NoSuchFileException, Path, Paths}

import scala.concurrent.duration._

import fs2.{Pipe, Stream}
import fs2.io.file.{deleteIfExists, exists, readAll}
import cats.implicits._

import cats.effect.{Blocker, IO, Resource, Timer}
import cats.effect.concurrent.Ref

import org.http4s.{HttpApp, HttpRoutes}
import org.http4s.dsl.io.{Path => _, _}
import org.http4s.implicits._
import org.http4s.Method.GET
import org.http4s.server.blaze.BlazeServerBuilder

import com.snowplowanalytics.snowplow.enrich.fs2.io.Clients
import com.snowplowanalytics.snowplow.enrich.fs2.AssetsRefresh.Hash

import org.specs2.mutable.Specification
import com.snowplowanalytics.snowplow.enrich.fs2.SpecHelpers.{CS, T}

class AssetsRefreshSpec extends Specification {

  sequential

  "updateStream" should {
    "not set stop signal if no updates required" in {
      val result = AssetsRefreshSpec.run(1.second) { (blocker, state, curDir, halt) =>
        val updateStream = AssetsRefresh.updateStream[IO](blocker, state, curDir, 100.millis, List.empty)
        halt(updateStream).compile.drain *> state.stop.get
      }
      result.unsafeRunSync() must beFalse
    }

    "download an asset and return stop signal into false" in {
      val path = Paths.get("asset")
      val input = List(
        (URI.create("http://localhost:8080/asset"), path.toString)
      )
      val result = AssetsRefreshSpec.run(1.second) { (blocker, state, curDir, halt) =>
        val updateStream = AssetsRefresh.updateStream[IO](blocker, state, curDir, 300.millis, input)
        for {
          _ <- halt(updateStream).compile.drain
          stop <- state.stop.get
          assetExists <- exists[IO](blocker, path)
        } yield (stop, assetExists)
      }

      val (stop, assetExists) = result.unsafeRunSync()
      (stop must beFalse) and (assetExists must beTrue)
    }

    "set stop signal to true when long downloads are performed" in {
      val input = List(
        (URI.create("http://localhost:8080/slow"), "asset1"), // First sets stop to true
        (URI.create("http://localhost:8080/slow"), "asset2") // Second doesn't allow update to return prematurely
      )
      val result = AssetsRefreshSpec.run(3.seconds) { (blocker, state, curDir, halt) =>
        val updateStream = AssetsRefresh.updateStream[IO](blocker, state, curDir, 500.milliseconds, input)

        for {
          fiber <- (IO.sleep(2.seconds) *> state.stop.get).start
          _ <- halt(updateStream).compile.drain
          stop <- fiber.join
        } yield stop
      }

      val stop = result.unsafeRunSync()
      stop must beTrue
    }

    "attempts to re-download non-existing file" in {
      val path = Paths.get("flaky-asset")
      val input = List(
        (URI.create("http://localhost:8080/flaky"), path.toString)
      )
      val result = AssetsRefreshSpec.run(4.seconds) { (blocker, state, curDir, halt) =>
        val updateStream = AssetsRefresh.updateStream[IO](blocker, state, curDir, 2.seconds, input)
        for {
          _ <- halt(updateStream).compile.drain
          stop <- state.stop.get
          assetExists <- readAll[IO](path, blocker, 8).compile.to(Array).map(b => new String(b))
        } yield (stop, assetExists)
      }

      val (stop, assetExists) = result.unsafeRunSync()
      (stop must beFalse) and (assetExists must beEqualTo("3"))
    }

  }
}

object AssetsRefreshSpec {

  /**
   * Test function with allocated resources
   * First argument - blocking thread pool
   * Second argument - empty state
   * Third argument - stream that will force the test to exit
   */
  type Test[A] = (Blocker, AssetsRefresh.State[IO], Path, Pipe[IO, Unit, Unit]) => IO[A]

  /**
   * Run a tests with resources allocated specifically for it
   * It will allocate thread pool, empty state, HTTP server and will
   * automatically remove all files after the test is over
   *
   * @param time timeout after which the test will be forced to exit
   * @param test the actual test suite function
   */
  def run[A](time: FiniteDuration)(test: Test[A]): IO[A] = {
    val resources = for {
      state <- AssetsRefreshSpec.mkState
      blocker <- SpecHelpers.blocker
    } yield (state, blocker)

    resources.use {
      case ((path, state), blocker) =>
        val haltStream = AssetsRefreshSpec.haltStream[IO](time)
        test(blocker, state, path, s => haltStream.mergeHaltL(s.merge(runServer))).flatTap(_ => filesCleanup(blocker))
    }
  }

  private val TestFiles = List(
    Paths.get("asset"),
    Paths.get("asset1"),
    Paths.get("asset2"),
    Paths.get("flaky-asset")
  )

  /** Allocate empty state with HTTP client */
  val mkState: Resource[IO, (Path, AssetsRefresh.State[IO])] =
    for {
      blocker <- Blocker[IO]
      clients <- Clients.initialize[IO](blocker, List(URI.create("http://localhost:8080")))
      map <- Resource.liftF(Ref.of[IO, Map[URI, Hash]](Map.empty))
      stop <- Resource.liftF(Ref.of[IO, Boolean](false))
      path = Paths.get("")
    } yield (path, AssetsRefresh.State(map, stop, clients))

  /** A stream that will halt after specified duration */
  def haltStream[F[_]: Timer](after: FiniteDuration): Stream[F, Unit] =
    Stream.eval(Timer[F].sleep(after))

  /** Clean-up predefined list of files */
  def filesCleanup(blocker: Blocker): IO[Unit] =
    TestFiles.traverse_ { path =>
      deleteIfExists[IO](blocker, path).recover {
        case _: NoSuchFileException => false
      }
    }

  /** Very dumb web-service that can count requests */
  def assetService(counter: Ref[IO, Int]): HttpApp[IO] =
    HttpRoutes
      .of[IO] {
        case GET -> Root / "asset" =>
          Ok("data")
        case GET -> Root / "slow" =>
          for {
            i <- counter.updateAndGet(_ + 1)
            _ <- if (i == 1) IO.sleep(100.milliseconds) else IO.sleep(10.seconds)
            res <- Ok(s"slow data $i")
          } yield res
        case GET -> Root / "counter" =>
          counter.updateAndGet(_ + 1).flatMap { i =>
            Ok(s"counter $i")
          }
        case GET -> Root / "flaky" =>
          counter.update(_ + 1) *>
            counter.get.flatMap { i =>
              val s = i.toString
              if (i == 1 || i == 2) NotFound(s)
              else if (i == 3) Ok(s)
              else NotFound(s)
            }
      }
      .orNotFound

  def runServer: Stream[IO, Unit] =
    for {
      counter <- Stream.eval(Ref.of[IO, Int](0))
      stream <- BlazeServerBuilder[IO](concurrent.ExecutionContext.global)
                  .bindHttp(8080)
                  .withHttpApp(assetService(counter))
                  .withoutBanner
                  .withoutSsl
                  .serve
                  .void
    } yield stream
}
