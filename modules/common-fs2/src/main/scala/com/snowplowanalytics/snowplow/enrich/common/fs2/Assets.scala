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
import java.nio.file.{Path, Paths}

import scala.concurrent.duration._
import scala.util.control.NonFatal

import cats.{Applicative, Parallel}
import cats.implicits._

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.effect.concurrent.Ref

import retry.{RetryDetails, RetryPolicies, RetryPolicy, retryingOnSomeErrors}

import fs2.Stream
import fs2.hash.md5
import fs2.io.file.{copy, deleteIfExists, exists, readAll, tempFileResource, writeAll}

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.enrich.common.utils.BlockerF

import com.snowplowanalytics.snowplow.enrich.common.fs2.inout.Clients

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
object Assets {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  /**
   * State of the [[updateStream]], containing information about tracked URIs
   * and `stop` signal from [[Environment]] as well as all clients necessary
   * to download URIs
   *
   * @param files mutable hash map of URIs and their latest known state
   * @param pauseEnrich stop signal coming from [[Environment]] and that can be used
   *             to stop the raw stream consumption
   * @param clients HTTP, GCS, S3 clients if necessary
   */
  final case class State[F[_]](
    files: Ref[F, Map[URI, Hash]],
    pauseEnrich: Ref[F, Boolean],
    clients: Clients[F]
  )

  object State {

    /** Test pair is used in tests to initialize HTTP client, will be ignored during initialization */
    private val TestPair: Asset = URI.create("http://localhost:8080") -> "index"

    /**
     * Initialize an assets state. Try to find them on local FS
     * or download if they're missing. Also initializes all necessary
     * clients (S3, GCP etc)
     * @param blocker thread pool for downloading and reading files
     * @param stop global stop signal from [[Environment]]
     * @param assets all assets that have to be tracked
     * @param http the HTTP client that we share with other parts of the application
     */
    def make[F[_]: ConcurrentEffect: Timer: ContextShift](
      blocker: Blocker,
      stop: Ref[F, Boolean],
      assets: List[Asset],
      clients: Clients[F]
    ): Resource[F, State[F]] =
      for {
        map <- Resource.eval(build[F](blocker, clients, assets.filterNot(asset => asset == TestPair)))
        files <- Resource.eval(Ref.of[F, Map[URI, Hash]](map))
      } yield State(files, stop, clients)

    def build[F[_]: Concurrent: Timer: ContextShift](
      blocker: Blocker,
      clients: Clients[F],
      assets: List[Asset]
    ): F[Map[URI, Hash]] =
      Logger[F].info("Preparing enrichment assets") *>
        buildFromLocal(blocker, assets)
          .flatMap { hashes =>
            hashes.traverse {
              case (uri, path, Some(hash)) =>
                Logger[F].info(s"Asset from $uri is found on local system at $path").as(uri -> hash)
              case (uri, path, None) =>
                downloadAndHash[F](clients, blocker, uri, Paths.get(path)).map(hash => uri -> hash)
            }
          }
          .map(_.toMap)

    def buildFromLocal[F[_]: Sync: ContextShift](blocker: Blocker, assets: List[Asset]): F[List[(URI, String, Option[Hash])]] =
      assets.traverse { case (uri, path) => local[F](blocker, path).map(hash => (uri, path, hash)) }

    /** Check if file already exists */
    def local[F[_]: Sync: ContextShift](blocker: Blocker, path: String): F[Option[Hash]] = {
      val fpath = Paths.get(path)
      exists(blocker, fpath).ifM(
        Hash.fromStream(readAll(fpath, blocker, 1024)).map(_.some),
        Sync[F].pure(none)
      )
    }
  }

  /** Valid MD5 hash */
  final case class Hash private (s: String) extends AnyVal

  object Hash {
    private[this] def fromBytes(bytes: Array[Byte]): Hash = {
      val bi = new java.math.BigInteger(1, bytes)
      Hash(String.format("%0" + (bytes.length << 1) + "x", bi))
    }

    def fromStream[F[_]: Sync](stream: Stream[F, Byte]): F[Hash] =
      stream.through(md5).compile.to(Array).map(fromBytes)
  }

  /** Pair of a tracked `URI` and destination path on local FS (`java.nio.file.Path` is not serializable) */
  type Asset = (URI, String)

  /** Initialise the [[updateStream]] with all necessary resources if refresh period is specified */
  def run[F[_]: ConcurrentEffect: ContextShift: Timer: Parallel, A](env: Environment[F, A]): Stream[F, Unit] =
    env.assetsUpdatePeriod match {
      case Some(duration) =>
        val init = for {
          curDir <- getCurDir
          _ <- Logger[F].info(s"Initializing assets refresh stream in $curDir, ticking every $duration")
          assets <- env.enrichments.get.map(_.configs.flatMap(_.filesToCache))
        } yield updateStream[F](env.blocker, env.assetsState, env.enrichments, curDir, duration, assets)
        Stream.eval(init).flatten
      case None =>
        Stream.empty.covary[F]
    }

  def getCurDir[F[_]: Sync]: F[Path] =
    Sync[F].delay(Paths.get("").toAbsolutePath)

  /**
   * At the end of every update, the stop signal will be resumed to `false`
   * Create an update stream that ticks periodically and can invoke an update action,
   * which will download an URI and check if it has been update. If it has the
   * raw stream will be stopped via `stop` signal from [[Environment]] and assets updated
   */
  def updateStream[F[_]: ConcurrentEffect: ContextShift: Parallel: Timer](
    blocker: Blocker,
    state: State[F],
    enrichments: Ref[F, Environment.Enrichments[F]],
    curDir: Path,
    duration: FiniteDuration,
    assets: List[Asset]
  ): Stream[F, Unit] =
    Stream.fixedDelay[F](duration).evalMap { _ =>
      val log = Logger[F].debug(show"Checking remote assets: ${assets.map(_._1).mkString(", ")}")
      val reinitialize: F[Unit] =
        for {
          // side-effecting get-set is inherently not thread-safe
          // we need to be sure the state.stop is set to true
          // before re-initializing enrichments
          _ <- Logger[F].info("Resuming enrich stream")
          old <- enrichments.get
          _ <- Logger[F].info(show"Reinitializing enrichments: ${old.configs.map(_.schemaKey.name).mkString(", ")}")
          fresh <- old.reinitialize(BlockerF.ofBlocker(blocker))
          _ <- enrichments.set(fresh)
          _ <- state.pauseEnrich.set(false)
        } yield ()

      val updated = downloadAndPause[F](blocker, state, curDir, assets)
      log *> updated.ifM(reinitialize, Logger[F].debug("No assets have been updated since last check"))
    }

  /**
   * Download list of assets, return false if none has been downloaded
   * It also can set `pauseEnrich` into `true` - a caller should make sure it's unpaused
   */
  def downloadAndPause[F[_]: ConcurrentEffect: ContextShift: Timer](
    blocker: Blocker,
    state: State[F],
    dir: Path,
    assets: List[Asset]
  ): F[Boolean] =
    assets
      .traverse {
        case (uri, path) =>
          update(blocker, state, dir, uri, Paths.get(path))
      }
      .map(_.contains(true))

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
    curDir: Path,
    uri: URI,
    path: Path
  ): F[Boolean] =
    tempFileResource[F](blocker, curDir).use { tmp =>
      // Set stop signal and replace old file with temporary
      def stopAndCopy(hash: Hash, delete: Boolean): F[Unit] =
        for {
          _ <- Logger[F].info(s"An asset at $uri has been updated since last check, pausing the enrich stream to reinitialize")
          _ <- state.pauseEnrich.set(true)
          _ <- if (delete) {
                 val deleted = Logger[F].info(s"Deleted outdated asset $path")
                 val notDeleted = Logger[F].warn(s"Couldn't delete $path, file didn't exist")
                 deleteIfExists(blocker, path).ifM(deleted, notDeleted)
               } else Sync[F].unit
          _ <- copy(blocker, tmp, path)
          _ <- state.files.update(_.updated(uri, hash))
          _ <- Logger[F].debug(s"Replaced $uri in Assets.State")
        } yield ()

      for {
        hash <- downloadAndHash(state.clients, blocker, uri, tmp)
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

  def downloadAndHash[F[_]: Concurrent: ContextShift: Timer](
    clients: Clients[F],
    blocker: Blocker,
    uri: URI,
    destination: Path
  ): F[Hash] = {
    val stream = clients.download(uri).observe(writeAll[F](destination, blocker))
    Logger[F].info(s"Downloading $uri") *> retryDownload(Hash.fromStream(stream))
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
