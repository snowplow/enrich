/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2

import java.nio.file.{NoSuchFileException, Path}
import java.util.concurrent.Executors

import scala.concurrent.duration.TimeUnit
import scala.concurrent.ExecutionContext

import cats.effect.{Blocker, Clock, IO, Resource}

import cats.implicits._

import fs2.io.file.deleteIfExists

import com.snowplowanalytics.iglu.client.{IgluCirceClient, Resolver}
import com.snowplowanalytics.iglu.client.resolver.registries.Registry

import com.snowplowanalytics.snowplow.enrich.common.fs2.test._
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Clients

import cats.effect.testing.specs2.CatsIO
import cats.effect.concurrent.Semaphore

object SpecHelpers extends CatsIO {
  implicit val ioClock: Clock[IO] =
    Clock.create[IO]

  val StaticTime = 1599750938180L

  val staticIoClock: Clock[IO] =
    new Clock[IO] {
      def realTime(unit: TimeUnit): IO[Long] = IO.pure(StaticTime)
      def monotonic(unit: TimeUnit): IO[Long] = IO.pure(StaticTime)
    }

  def refreshState(assets: List[Assets.Asset]): Resource[IO, Assets.State[IO]] =
    for {
      b <- TestEnvironment.ioBlocker
      sem <- Resource.eval(Semaphore[IO](1L))
      http <- Clients.mkHttp[IO](ec = SpecHelpers.blockingEC)
      clients = Clients.init[IO](http, Nil)
      state <- Resource.eval(Assets.State.make[IO](b, sem, clients, assets))
    } yield state

  /** Clean-up predefined list of files */
  def filesCleanup(blocker: Blocker, files: List[Path]): IO[Unit] =
    files.traverse_ { path =>
      deleteIfExists[IO](blocker, path).recover {
        case _: NoSuchFileException => false
      }
    }

  /** Make sure files don't exist before and after test starts */
  def filesResource(blocker: Blocker, files: List[Path]): Resource[IO, Unit] =
    Resource.make(filesCleanup(blocker, files))(_ => filesCleanup(blocker, files))

  def createIgluClient(registries: List[Registry]): IO[IgluCirceClient[IO]] =
    IgluCirceClient.fromResolver[IO](Resolver(registries, None), cacheSize = 0)

  val blockingEC = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool)
}
