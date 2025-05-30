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

import java.net.URI

import scala.concurrent.duration._
import scala.util.control.NonFatal

import cats.{Applicative, Foldable}
import cats.implicits._

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.implicits._

import retry.{RetryDetails, RetryPolicies, RetryPolicy, Sleep, retryingOnSomeErrors}

import fs2.io.file.{CopyFlag, CopyFlags, Files, Path}
import fs2.hashing.{HashAlgorithm, Hashing}
import fs2.Stream

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.runtime.processing.Coldswap

import com.snowplowanalytics.snowplow.enrich.cloudutils.core._

import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf

/** Code in charge of downloading the assets used by enrichments (e.g. MaxMind/IAB DBs) */
object Assets {

  case class Asset(uri: URI, localPath: String)

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def fromEnrichmentConfs(enrichmentConfs: List[EnrichmentConf]): List[Asset] =
    enrichmentConfs.flatMap { conf =>
      conf.filesToCache.map(tuple => Asset(tuple._1, tuple._2))
    }

  def downloadAssets[F[_]: Async](assets: List[Asset], blobClients: List[BlobClient[F]]): F[Unit] = {
    val groupedByClientF = Foldable[List].foldM(assets, Map.empty[BlobClient[F], List[Asset]]) {
      case (acc, asset) =>
        acc.find(_._1.canDownload(asset.uri)) match {
          case Some((client, otherAssets)) =>
            Async[F].pure(acc + (client -> (asset :: otherAssets)))
          case None =>
            blobClients.find(_.canDownload(asset.uri)) match {
              case Some(client) =>
                Async[F].pure(acc + (client -> List(asset)))
              case None =>
                Async[F].raiseError[Map[BlobClient[F], List[Asset]]](
                  new IllegalStateException(s"No blob client available to download ${asset.uri}")
                )
            }
        }
    }

    groupedByClientF.flatMap { groupedByClient =>
      groupedByClient.toList.parTraverse_ {
        case (client, assets) =>
          client.mk.use { impl =>
            assets.parTraverse { asset =>
              for {
                _ <- Logger[F].info(s"Downloading asset from ${asset.uri} to ${asset.localPath}")
                _ <- retry(impl.download(asset.uri).through(Files.forAsync[F].writeAll(Path(asset.localPath))).compile.drain)
              } yield ()
            }
          }
      }
    }
  }

  def updateStream[F[_]: Async](
    assets: List[Asset],
    refreshPeriod: FiniteDuration,
    enrichmentRegistry: Coldswap[F, EnrichmentRegistry[F]],
    blobClients: List[BlobClient[F]]
  ): Stream[F, Nothing] =
    Stream
      .fixedDelay[F](refreshPeriod)
      .evalMap { _ =>
        val resources =
          for {
            withTmpPath <- assets.traverse(asset => Files.forAsync[F].tempFile(None, "", "", None).map(tmpPath => (asset, tmpPath)))
            _ <- Resource.eval(
                   downloadAssets(
                     withTmpPath.map { case (asset, tmpPath) => Asset(asset.uri, tmpPath.toString) },
                     blobClients
                   )
                 )
            md5sum = Hashing.forSync[F].hash(HashAlgorithm.MD5)
            currentHashes <- Resource.eval(
                               withTmpPath.traverse {
                                 case (asset, _) => Files.forAsync[F].readAll(Path(asset.localPath)).through(md5sum).compile.lastOrError
                               }
                             )
            newHashes <-
              Resource.eval(
                withTmpPath.traverse { case (_, tmpPath) => Files.forAsync[F].readAll(tmpPath).through(md5sum).compile.lastOrError }
              )
            updated = withTmpPath.zip(currentHashes.zip(newHashes)).collect {
                        case ((asset, tmpPath), (currentHash, newHash)) if newHash != currentHash => (asset, tmpPath)
                      }
            _ <- Resource.eval {
                   if (updated.isEmpty) Sync[F].unit
                   else
                     enrichmentRegistry.closed.surround {
                       updated.traverse {
                         case (asset, tmpPath) =>
                           Files.forAsync[F].copy(tmpPath, Path(asset.localPath), CopyFlags(CopyFlag.ReplaceExisting))
                       }
                     }
                 }
          } yield ()

        resources.use_
      }
      .drain

  private def retry[F[_]: Sleep: Sync, A](download: F[A]): F[A] =
    retryingOnSomeErrors[A](retryPolicy[F], worthRetrying[F], onError[F])(download)

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
    else if (retryDetails.retriesSoFar == 0)
      Logger[F].warn(show"Failed to download an asset: ${error.getMessage}. Retrying")
    else
      Logger[F].warn(
        show"Failed to download an asset after ${retryDetails.retriesSoFar} retries: ${error.getMessage}. " +
          show"Retrying in ${retryDetails.upcomingDelay}"
      )
}
