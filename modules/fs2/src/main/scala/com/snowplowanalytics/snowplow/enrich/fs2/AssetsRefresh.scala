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
import java.nio.file.{Path, Paths}

import scala.concurrent.duration._
import scala.util.control.NonFatal

import cats.{Applicative, Parallel}
import cats.implicits._

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync, Timer}
import cats.effect.concurrent.Ref

import retry.{RetryDetails, RetryPolicies, RetryPolicy, retryingOnSomeErrors}

import fs2.Stream
import fs2.hash.md5
import fs2.io.file.{copy, deleteIfExists, tempFileResource, writeAll}

import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.enrich.fs2.io.Clients

/**
 * Functions responsible for periodic assets (such as MaxMind/IAB DBs) updates
 * The common logic is to periodically invoke a function that:
 * 1. Downloads a file (in background) to a temp location
 * 2. Compares file's checksum with existing one (stored in a mutable hashmap)
 * 3. If checksums match - delete the temp file, return
 * 4. If checksums don't match - send a signal to stop raw stream
 * (via `SignallingRef` in [[Environment]])
 * 5. Once raw stream is stopped - delete an old file and move
 * temp file to the old's file location
 * If any of those URIs been updated and stopped the raw stream, it will be
 * immediately resumed once the above procedure traversed all files
 */
object AssetsRefresh {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  /**
   * State of the [[updateStream]], containing information about tracked URIs
   * and `stop` signal from [[Environment]] as well as all clients necessary
   * to download URIs
   *
   * @param files mutable hash map of URIs and their latest known state
   * @param stop stop signal coming from [[Environment]] and that can be used
   *             to stop the raw stream consumption
   * @param clients HTTP, GCS, S3 clients if necessary
   */
  case class State[F[_]](
    files: Ref[F, Map[URI, Hash]],
    stop: Ref[F, Boolean],
    clients: Clients[F]
  )

  case class Hash private (s: String) extends AnyVal

  object Hash {
    def apply(bytes: Array[Byte]): Hash = {
      val bi = new java.math.BigInteger(1, bytes)
      Hash(String.format("%0" + (bytes.length << 1) + "x", bi))
    }
  }

  /** Pair of a tracker `URI` and destination path on local FS (`java.nio.file.Path` is not serializable) */
  type Asset = (URI, String)

  /** Initialise the [[updateStream]] with all necessary resources if refresh period is specified */
  def run[F[_]: ConcurrentEffect: ContextShift: Timer: Parallel](environment: Environment[F]): Stream[F, Unit] =
    environment.config.assetsUpdatePeriod match {
      case Some(duration) =>
        val init = for {
          files <- Ref.of(Map.empty[URI, Hash])
          curDir <- Sync[F].delay(Paths.get("").toAbsolutePath)
          _ <- Logger[F].info("Initializing AssetsRefresh stream")
          uris = environment.enrichments.configs.flatMap(_.filesToCache).map(_._1)
          stream = for {
                     clients <- Stream.resource(Clients.initialize[F](environment.blocker, uris))
                     state = State(files, environment.stop, clients)
                     assets = environment.enrichments.configs.flatMap(_.filesToCache)
                     _ <- updateStream[F](environment.blocker, state, curDir, duration, assets)
                   } yield ()
        } yield stream
        Stream.eval(init).flatten
      case None =>
        Stream.empty.covary[F]
    }

  /**
   * At the end of every update, the stop signal will be resumed to `false`
   * Create an update stream that ticks periodically and can invoke an update action,
   * which will download an URI and check if it has been update. If it has the
   * raw stream will be stopped via `stop` signal from [[Environment]] and assets updated
   */
  def updateStream[F[_]: ConcurrentEffect: ContextShift: Parallel: Timer](
    blocker: Blocker,
    state: State[F],
    curDir: Path,
    duration: FiniteDuration,
    assets: List[Asset]
  ): Stream[F, Unit] =
    Stream.awakeEvery[F](duration).evalMap { _ =>
      val log = Logger[F].debug(show"Ticking with ${assets.map(_._2)}")
      val updates = assets.parTraverse {
        case (uri, path) =>
          update(blocker, state, curDir)(uri, Paths.get(path))
      }
      log *> updates.map(_.contains(true)).flatMap { stopped =>
        if (stopped) Logger[F].info("Resuming signalling ref") *> state.stop.set(false) else Sync[F].unit
      }
    }

  /**
   * Update a file in current directory if it has been updated on remote storage
   * If a new file has been discovered - stops the enriching streams (signal in `state`)
   * Do nothing if file hasn't been updated
   *
   * Note: this function has a potential to be thread-unsafe if download time
   * exceeds tick period. We assume that no two threads will be downloading the same URI
   *
   * @param blocker a thread pool to execute download/copy operations
   * @param state a map of URI to MD5 hash to keep track latest state of remote files
   * @param curDir a local FS destination for temporary files
   * @param uri a remote file (S3, GCS or HTTP), the URI is used as an identificator
   * @param path a static file name that enrich clients will access
   *             file itself is placed in current dir (`dir`)
   * @return true if file has been updated
   */
  def update[F[_]: ConcurrentEffect: ContextShift: Timer](
    blocker: Blocker,
    state: State[F],
    curDir: Path
  )(
    uri: URI,
    path: Path
  ): F[Boolean] =
    tempFileResource[F](blocker, curDir).use { tmp =>
      // Set stop signal and replace old file with temporary
      def stopAndCopy(hash: Hash, delete: Boolean): F[Unit] =
        for {
          _ <- Logger[F].info(s"Discovered new data at $uri, stopping signalling ref")
          _ <- state.stop.set(true)
          _ <- if (delete) {
                 val deleted = Logger[F].info(s"Deleted outdated $path")
                 val notDeleted = Logger[F].warn(s"Couldn't delete $path, file didn't exist")
                 deleteIfExists(blocker, path).ifM(deleted, notDeleted)
               } else Sync[F].unit
          _ <- copy(blocker, tmp, path)
          _ <- state.files.update(_.updated(uri, hash))
        } yield ()

      val data = state.clients.download(uri).observe(writeAll(tmp, blocker)).through(md5)
      for {
        _ <- Logger[F].info(s"Downloading $uri")
        hash <- retryDownload(data.compile.to(Array)).map(Hash.apply)
        localFiles <- state.files.get
        updated <- localFiles.get(uri) match {
                     case Some(known) if known == hash =>
                       Sync[F].pure(false)
                     case Some(_) =>
                       stopAndCopy(hash, true).as(true)
                     case None =>
                       stopAndCopy(hash, false).as(true)
                   }
      } yield updated
    }

  def retryDownload[F[_]: Sync: Timer, A](download: F[A]): F[A] =
    retryingOnSomeErrors[A](retryPolicy[F], worthRetrying, onError[F])(download)

  def retryPolicy[F[_]: Applicative]: RetryPolicy[F] =
    RetryPolicies.fullJitter[F](1500.milliseconds).join(RetryPolicies.limitRetries[F](5))

  def worthRetrying(e: Throwable): Boolean =
    e match {
      case _: Clients.DownloadingFailure => true
      case _: IllegalArgumentException => false
      case NonFatal(_) => false
    }

  def onError[F[_]: Sync](error: Throwable, details: RetryDetails): F[Unit] =
    if (details.givingUp)
      Logger[F].error(show"Failed to download an asset after ${details.retriesSoFar}. ${error.getMessage}. Aborting the job")
    else if (details.retriesSoFar == 0)
      Logger[F].warn(show"Failed to download an asset. ${error.getMessage}. Keep retrying")
    else
      Logger[F].warn(
        show"Failed to download an asset after ${details.retriesSoFar} retries, " +
          show"waiting for ${details.cumulativeDelay.toMillis} ms. ${error.getMessage}. " +
          show"Keep retrying"
      )
}
