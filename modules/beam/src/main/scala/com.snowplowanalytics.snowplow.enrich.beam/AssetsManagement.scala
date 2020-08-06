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
import java.lang.{Long => JLong}

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

  /** Value to tick with when assets should not get updated */
  val DefaultValue: JLong = 0L

  /** List of linked files or error messages */
  type DbList = List[Either[String, FileLink]]

  /**
   * A downloaded asset. `Path` not used because its not serializable
   * @param original real file path on a worker
   * @param link link path to `original`. Its used because enrichments
   *             refer to static/hardcoded filepath, whereas `original`
   *             is dynamic
   */
  case class FileLink(original: String, link: String)

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Create a transformation sole purpose of which is to periodially refresh
   * worker assets. If no `refreshRate` given - it's an empty transformation
   * @param sc Scio context
   * @param refreshRate an interval with which assets should be updated
   *                    (in minutes)
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
        val refreshInput = getSideInput(sc, rate)
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

  /** Get a side-input producing `true` only after specified period */
  def getSideInput(sc: ScioContext, period: Duration): SideInput[JLong] =
    sc
      .customInput(
        SideInputName,
        GenerateSequence
          .from(1L)
          .withRate(1L, period)
      )
      .withName("assets-refresh-window")
      .withFixedWindows(
        duration = period,
        offset = Duration.ZERO,
        options = WindowOptions(
          trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
          accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
          closingBehavior = ClosingBehavior.FIRE_IF_NON_EMPTY,
          allowedLateness = Duration.standardSeconds(0)
        )
      )
      .withName("assets-refresh-false")
      .asSingletonSideInput(DefaultValue)

  /** Transformation that only updates DBs and returns data as is */
  def withAssetsUpdate[A: Coder](
    cachedFiles: DistCache[DbList],
    refreshInput: SideInput[JLong],
    rfu: RemoteFileUtil
  )(
    raw: SCollection[A]
  ): SCollection[A] =
    raw
      .withFixedWindows(Duration.standardSeconds(10))
      .withSideInputs(refreshInput)
      .withName("assets-refresh")
      .map { (raw, side) =>
        val update = side(refreshInput)
        if (update != DefaultValue) {
          logger.info(s"Updating cached assets on $update tick")
          val existing = cachedFiles() // Get already downloaded (or download for first invocation
          existing.foreach { // Delete all already downloaded
            case Right(FileLink(original, link)) =>
              val originalUri = URI.create(original)
              rfu.delete(originalUri)
              val linkUri = URI.create(link)
              rfu.delete(linkUri)
              logger.info(s"$originalUri and $linkUri deleted")
            case Left(error) =>
              logger.warn(s"Error during asset update: $error")
          }
          val _ = cachedFiles() // Re-download (update)
        } else {
          val _ = cachedFiles()
        }

        raw
      }
      .toSCollection
      .withGlobalWindow()

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

    sc.distCache(filesToDownload)(linkFiles(filesDestinations))
  }

  /**
   * Link every `downloaded` file to a destination from `links`
   * Both `downloaded` and `links` must have same amount of elements
   */
  private def linkFiles(links: List[String])(downloaded: Seq[File]): DbList = {
    val mapped = downloaded.toList
      .zip(links)
      .map { case (file, symLink) => createSymLink(file, symLink) }

    mapped.zip(downloaded).map {
      case (Right(p), file) =>
        logger.info(s"File $file cached at $p")
        FileLink(file.toString, p.toString).asRight
      case (Left(e), file) =>
        logger.warn(s"File $file could not be cached: $e")
        e.asLeft
    }
  }
}
