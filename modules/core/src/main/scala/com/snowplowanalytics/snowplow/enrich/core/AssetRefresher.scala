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
package com.snowplowanalytics.snowplow.enrich.core

import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.{FileSystems, Path => NioPath}

import scala.concurrent.duration._
import scala.util.control.NonFatal

import cats.{Applicative, Eq, Foldable}
import cats.implicits._

import cats.effect.{Async, Resource, Sync}
import cats.effect.std.AtomicCell
import cats.effect.implicits._

import retry.{RetryDetails, RetryPolicies, RetryPolicy, retryingOnSomeErrors}

import fs2.io.file.{Files, Path}
import fs2.hashing.{HashAlgorithm, Hashing}
import fs2.{Chunk, Stream}

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.runtime.processing.Coldswap

import com.snowplowanalytics.snowplow.enrich.cloudutils.core._

import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf

class AssetRefresher[F[_]: Async](
  atomicCell: AtomicCell[F, List[AssetRefresher.Asset[F]]],
  coldswap: Coldswap[F, EnrichmentRegistry[F]]
) {
  import AssetRefresher._

  /**
   * Checks for updated enrichment assets and reloads the enrichment registry if any changed.
   *
   * Downloads all assets in parallel using conditional requests (If-None-Match with etags or MD5 comparison).
   * Assets that haven't changed return 304 Not Modified without downloading content.
   * If any asset has changed, closes and reopens the enrichment registry with the new assets.
   * If no assets changed, the registry continues running with existing assets.
   */
  def refreshAndOpenRegistry: F[Unit] =
    atomicCell.evalUpdate { assets =>
      if (assets.nonEmpty)
        assets
          .parTraverse(downloadAsset(_))
          .flatMap {
            case newStatuses =>
              val hadUpdate = assets.zip(newStatuses).exists {
                case (asset, newStatus) => asset.status =!= newStatus
              }
              if (hadUpdate) {
                val newAssets = assets.zip(newStatuses).map {
                  case (asset, newStatus) => asset.copy(status = newStatus)
                }
                for {
                  _ <- coldswap.closed.use_
                  _ <- Logger[F].info("Opening Enrichment registry for new downloaded assets")
                  _ <- coldswap.opened.use_
                } yield newAssets
              } else
                assets.pure[F]
          }
      else coldswap.opened.use_.as(assets)
    }
}

object AssetRefresher {

  /**
   * Tracks the download status of an enrichment asset.
   * Used to determine if the asset needs to be re-downloaded.
   */
  private sealed trait AssetStatus

  /**
   * Asset has not been downloaded yet.
   * Initial state before first download attempt.
   */
  private case object NotDownloaded extends AssetStatus

  /**
   * Asset has been successfully downloaded.
   * Contains a value (etag or MD5) for detecting future changes.
   */
  private sealed trait AssetSuccessfulStatus extends AssetStatus

  /**
   * Asset downloaded with an etag from the blob storage service.
   * The etag enables efficient conditional requests (If-None-Match) on subsequent downloads.
   * Most blob storage backends (S3, GCS, Azure) provide etags.
   */
  private case class Etag(value: String) extends AssetSuccessfulStatus

  /**
   * Asset downloaded without an etag.
   * MD5 hash computed locally to detect changes on subsequent downloads.
   * Used when the blob storage backend does not provide etags (e.g., plain HTTP servers).
   * Less efficient than etag-based detection since the entire file must be downloaded to compare.
   */
  private case class Md5(value: String) extends AssetSuccessfulStatus

  private implicit def assetStatusEq: Eq[AssetStatus] = Eq.fromUniversalEquals

  private case class Asset[F[_]](
    client: BlobClient[F],
    uri: URI,
    localPath: NioPath,
    status: AssetStatus
  )

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def fromEnrichmentConfs[F[_]: Async](
    enrichmentConfs: List[EnrichmentConf],
    blobClients: List[BlobClientFactory[F]],
    coldswap: Coldswap[F, EnrichmentRegistry[F]]
  ): Resource[F, AssetRefresher[F]] = {
    val filesToDownload = enrichmentConfs.flatMap(_.filesToCache)
    val groupedByClientF = Foldable[List].foldM(filesToDownload, Map.empty[BlobClientFactory[F], List[(URI, NioPath)]]) {
      case (acc, (uri, localPath)) =>
        blobClients.find(_.canDownload(uri)) match {
          case Some(client) =>
            Async[F].pure(Map(client -> List(uri -> FileSystems.getDefault.getPath(localPath))) |+| acc)
          case None =>
            Async[F].raiseError[Map[BlobClientFactory[F], List[(URI, NioPath)]]] {
              new IllegalStateException(s"No blob client available to download $uri")
            }
        }
    }

    for {
      groupedByClient <- Resource.eval(groupedByClientF)
      assets <- groupedByClient.toList
                  .traverse {
                    case (client, toDownload) =>
                      client.mk.map { client =>
                        toDownload.map {
                          case (uri, localPath) => Asset(client, uri, localPath, NotDownloaded)
                        }
                      }
                  }
                  .map(_.flatten)
      atomicCell <- Resource.eval(AtomicCell[F].of(assets))
    } yield new AssetRefresher(atomicCell, coldswap)
  }

  def updateStream[F[_]: Async](
    assetRefresher: AssetRefresher[F],
    refreshPeriod: FiniteDuration
  ): Stream[F, Nothing] =
    Stream
      .fixedDelay[F](refreshPeriod)
      .evalTap(_ => assetRefresher.refreshAndOpenRegistry)
      .drain

  private def downloadAsset[F[_]: Async](asset: Asset[F]): F[AssetSuccessfulStatus] = {
    val io = asset match {
      case Asset(client, uri, localPath, NotDownloaded | Md5(_)) =>
        Logger[F].debug(s"Fetching asset from $uri") >>
          client.get(uri).flatMap(processDownloadedContent(uri, localPath, _))
      case Asset(client, uri, localPath, Etag(oldEtag)) =>
        Logger[F].debug(s"Fetching asset from $uri if etag does not match $oldEtag") >>
          client.getIfNeeded(uri, oldEtag).flatMap[AssetSuccessfulStatus] {
            case BlobClient.EtagMatched =>
              Logger[F]
                .debug(s"Asset from $uri was not fetched because etag matched $oldEtag")
                .as(Etag(oldEtag))
            case result: BlobClient.GetResult =>
              processDownloadedContent(uri, localPath, result)
          }
    }
    retryingOnSomeErrors(retryPolicy[F], worthRetrying[F], onError[F])(io)
  }

  private def getMd5[F[_]: Async](content: ByteBuffer): F[String] =
    Stream
      .chunk(Chunk.byteBuffer(content))
      .through(Hashing.forSync[F].hash(HashAlgorithm.MD5))
      .compile
      .lastOrError
      .map(_.toString)

  private def writeToPath[F[_]: Async](path: NioPath, content: ByteBuffer): F[Unit] =
    Stream
      .chunk(Chunk.byteBuffer(content))
      .through(Files.forAsync[F].writeAll(Path.fromNioPath(path)))
      .compile
      .drain

  private def processDownloadedContent[F[_]: Async](
    uri: URI,
    localPath: NioPath,
    result: BlobClient.GetResult
  ): F[AssetSuccessfulStatus] =
    result match {
      case BlobClient.ContentWithEtag(content, etag) =>
        for {
          _ <- writeToPath(localPath, content)
          _ <- Logger[F].info(s"Downloaded enrichment asset $uri with etag $etag to local path $localPath")
        } yield Etag(etag)
      case BlobClient.ContentNoEtag(content) =>
        for {
          md5 <- getMd5[F](content)
          _ <- writeToPath(localPath, content)
          _ <- Logger[F].info(s"Downloaded enrichment asset $uri with md5 $md5 to local path $localPath")
        } yield Md5(md5)
    }

  private def retryPolicy[F[_]: Applicative]: RetryPolicy[F] =
    RetryPolicies.fullJitter[F](1500.milliseconds).join(RetryPolicies.limitRetries[F](5))

  private def worthRetrying[F[_]: Applicative](e: Throwable): F[Boolean] =
    e match {
      case _: RetryableFailure => Applicative[F].pure(true)
      case _: IllegalArgumentException => Applicative[F].pure(false)
      case NonFatal(_) => Applicative[F].pure(false)
    }

  private def onError[F[_]: Sync](error: Throwable, retryDetails: RetryDetails): F[Unit] =
    if (retryDetails.givingUp)
      Logger[F].error(show"Failed to download an asset after ${retryDetails.retriesSoFar} retries: ${error.getMessage}. Aborting")
    else if (retryDetails.retriesSoFar === 0)
      Logger[F].warn(show"Failed to download an asset: ${error.getMessage}. Retrying")
    else
      Logger[F].warn(
        show"Failed to download an asset after ${retryDetails.retriesSoFar} retries: ${error.getMessage}. " +
          show"Retrying in ${retryDetails.upcomingDelay}"
      )
}
