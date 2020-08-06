/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.beam

import java.io.File
import java.net.URI
import java.nio.file.NoSuchFileException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Paths}

import cats.syntax.either._

import org.joda.time.Duration

import org.slf4j.LoggerFactory

import org.apache.beam.sdk.extensions.gcp.options.GcsOptions
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.apache.beam.sdk.transforms.windowing.{AfterPane, Repeatedly}
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.util.RemoteFileUtil
import com.spotify.scio.values.{DistCache, SCollection, SideInput, WindowOptions}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.beam.utils.createSymLink

/**
 * Module responsible for assets (such as MaxMind and referer-parser DBs)
 * management: downloading, distributing per workers, updating when necessary
 */
object AssetsManagement {

  val SideInputName = "assets-refresh-tick"

  /** List of linked files or error messages */
  type DbList = List[Either[String, FileLink]]

  /**
   * A downloaded asset. `Path` not used because its not serializable
   * @param uri an original GCS URI
   * @param original real file path on a worker
   * @param link link path to `original`. Its used because enrichments
   *             refer to static/hardcoded filepath, whereas `original`
   *             is dynamic
   */
  case class FileLink(
    uri: String,
    original: String,
    link: String
  )

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Create a transformation sole purpose of which is to periodially refresh
   * worker assets. If no `refreshRate` given - it's an empty transformation
   * @param sc Scio context
   * @param refreshRate an interval with which assets should be updated
   * @param enrichmentConfs enrichment configurations to provide links
   * @tparam A type of data flowing through original stream
   * @return no-op or refresh transformation
   */
  def mkTransformation[A: Coder](
    sc: ScioContext,
    refreshRate: Option[Duration],
    enrichmentConfs: List[EnrichmentConf]
  ): SCollection[A] => SCollection[A] =
    refreshRate match {
      case Some(rate) =>
        val rfu = RemoteFileUtil.create(sc.optionsAs[GcsOptions])
        val refreshInput = getSideInput(sc, rate).asSingletonSideInput(false)
        val distCache = buildDistCache(sc, enrichmentConfs)
        withAssetsUpdate[A](distCache, refreshInput, rfu)
      case None =>
        val distCache = buildDistCache(sc, enrichmentConfs)
        // identity function analog for scio
        (collection: SCollection[A]) =>
          collection
            .withName("assets-refresh-noop")
            .map { a =>
              val _ = distCache()
              a
            }
    }

  /** `SCollection` ticking with `true` for a minute, after specified period of time */
  def getSideInput(sc: ScioContext, period: Duration): SCollection[Boolean] =
    sc
      .customInput(
        SideInputName,
        GenerateSequence
          .from(0L)
          .withRate(1L, Duration.standardMinutes(1))
      )
      .map(x => x % period.getStandardMinutes == 0)
      .withName("assets-refresh-window")
      .withGlobalWindow(
        options = WindowOptions(
          trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
          accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
          closingBehavior = ClosingBehavior.FIRE_IF_NON_EMPTY,
          allowedLateness = Duration.standardSeconds(0)
        )
      )
      .withName("assets-refresh-noupdate")

  /** Transformation that only updates DBs and returns data as is */
  def withAssetsUpdate[A: Coder](
    cachedFiles: DistCache[DbList],
    refreshInput: SideInput[Boolean],
    rfu: RemoteFileUtil
  )(
    raw: SCollection[A]
  ): SCollection[A] =
    raw
      .withGlobalWindow()
      .withSideInputs(refreshInput)
      .withName("assets-refresh") // aggregate somehow, if there's true in acc - return false
      .map { (raw, side) =>
        val update = side(refreshInput)
        if (update) {
          val existing = cachedFiles() // Get already downloaded
          // Traverse all existing files and find out if at least one had to be deleted
          val atLeastOne = existing.foldLeft(false)((acc, cur) => shouldUpdate(cur) || acc)
          if (atLeastOne) {
            logger.info(s"Deleting all assets: ${}")
            existing.foreach(deleteFile(rfu))
            logger.info("Re-downloading expired assets")
            val _ = cachedFiles()
          }
        } else {
          val _ = cachedFiles() // In case side-input's first value wasn't true somehow, should be no-op
        }

        raw
      }
      .toSCollection

  /**
   * Builds a Scio's [[DistCache]] which downloads the needed files and create the necessary
   * symlinks.
   * @param sc Scio context
   * @param enrichmentConfs list of enrichment configurations
   * @return a properly build [[DistCache]]
   */
  def buildDistCache(sc: ScioContext, enrichmentConfs: List[EnrichmentConf]): DistCache[DbList] = {
    val filesToCache: List[(URI, String)] = enrichmentConfs
      .flatMap(_.filesToCache)
    val filesToDownload = filesToCache.map(_._1.toString)
    val filesDestinations = filesToCache.map(_._2)

    sc.distCache(filesToDownload)(linkFiles(filesToDownload, filesDestinations))
  }

  /**
   * Check if link is older than 2 minutes and try to delete it and its original file
   * Checking that it's old allows to not re-trigger downloading
   * multiple times because refreshing side input will be true for one minute
   * @param fileLink data structure containing all information about a file
   *                 that potentially has to be deleted and re-downloaded
   * @return true if thread managed to delete a link and RFU
   *         false if there was any error or it wasn't necessary to delete it
   */
  private def shouldUpdate(fileLink: Either[String, FileLink]): Boolean =
    fileLink match {
      case Right(FileLink(_, originalPath, _)) =>
        val path = Paths.get(originalPath)
        try {
          val lastModified = Files
            .readAttributes(path, classOf[BasicFileAttributes])
            .lastModifiedTime()
            .toMillis
          val now = System.currentTimeMillis()
          val should = now - lastModified > 120000L
          if (should) logger.info(s"File $originalPath is timestamped with $lastModified at $now")
          should
        } catch {
          case _: NoSuchFileException =>
            logger.warn(
              s"File $path does not exist for lastModifiedTime, possibly another thread deleted it. Blocking the worker to avoid futher racing"
            )
            Thread.sleep(2000)
            false
        }
      case Left(_) =>
        // Threads ran into race condition and cannot overwrite each other's link - not a failure
        false
    }

  private def deleteFile(rfu: RemoteFileUtil)(fileLink: Either[String, FileLink]): Unit =
    fileLink match {
      case Right(FileLink(originalUri, originalPath, linkPath)) =>
        val deleted = deleteFilePhysically(originalPath) || deleteFilePhysically(linkPath)
        if (deleted) {
          rfu.delete(URI.create(originalUri))
          logger.info(s"Deleted $fileLink")
        }
      case Left(_) =>
        ()
    }

  private def deleteFilePhysically(path: String): Boolean =
    Either.catchOnly[NoSuchFileException](Files.delete(Paths.get(path))) match {
      case Left(_) =>
        logger.warn(s"Tried to delete nonexisting $path")
        false
      case Right(_) =>
        true
    }

  /**
   * Link every `downloaded` file to a destination from `links`
   * Both `downloaded` and `links` must have same amount of elements
   */
  private def linkFiles(uris: List[String], links: List[String])(downloaded: Seq[File]): DbList = {
    val mapped = downloaded.toList
      .zip(links)
      .map { case (file, symLink) => createSymLink(file, symLink) }

    uris.zip(mapped).zip(downloaded).map {
      case ((uri, Right(p)), file) =>
        logger.info(s"File $file cached at $p")
        FileLink(uri, file.toString, p.toString).asRight
      case ((uri, Left(e)), file) =>
        logger.warn(s"File $file (downloaded from $uri) could not be cached: $e")
        e.asLeft
    }
  }
}
