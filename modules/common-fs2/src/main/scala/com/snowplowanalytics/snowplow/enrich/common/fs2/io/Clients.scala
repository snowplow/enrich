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
package com.snowplowanalytics.snowplow.enrich.common.fs2.io

import java.net.URI

import cats.syntax.option._
import cats.syntax.functor._
import cats.syntax.flatMap._

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Sync}

import fs2.{RaiseThrowable, Stream}

import blobstore.Path
import blobstore.s3.S3Store
import blobstore.gcs.GcsStore

import com.google.cloud.storage.StorageOptions

import org.http4s.{Request, Uri}
import org.http4s.client.{Client => HttpClient}
import org.http4s.client.blaze.BlazeClientBuilder

import software.amazon.awssdk.services.s3.S3AsyncClient

import scala.concurrent.ExecutionContext

case class Clients[F[_]](
  s3Store: Option[S3Store[F]],
  gcsStore: Option[GcsStore[F]],
  http: HttpClient[F]
) {

  /** Download an `uri` as a stream of bytes, using the appropriate client */
  def download(uri: URI)(implicit RT: RaiseThrowable[F]): Stream[F, Byte] =
    Clients.Client.getByUri(uri) match {
      case Some(Clients.Client.S3) =>
        for {
          s3 <- s3Store match {
                  case Some(c) => Stream.emit(c)
                  case None => Stream.raiseError(new IllegalStateException(s"S3 client is not initialized to download $uri"))
                }
          data <- s3.get(Path(uri.toString), 16 * 1024)
        } yield data
      case Some(Clients.Client.GCS) =>
        for {
          gcs <- gcsStore match {
                   case Some(c) => Stream.emit(c)
                   case None => Stream.raiseError(new IllegalStateException(s"GCS client is not initialized to download $uri"))
                 }
          data <- gcs.get(Path(uri.toString), 16 * 1024)
        } yield data
      case Some(Clients.Client.HTTP) =>
        val request = Request[F](uri = Uri.unsafeFromString(uri.toString))
        for {
          response <- http.stream(request)
          body <- if (response.status.isSuccess) response.body
                  else Stream.raiseError[F](Clients.DownloadingFailure(uri))
        } yield body
      case None =>
        Stream.raiseError(new IllegalStateException(s"No client  initialized to download $uri"))
    }
}

object Clients {

  sealed trait Client
  object Client {
    case object S3 extends Client
    case object GCS extends Client
    case object HTTP extends Client

    def getByUri(uri: URI): Option[Client] =
      uri.getScheme match {
        case "http" | "https" =>
          Some(HTTP)
        case "gs" =>
          Some(GCS)
        case "s3" =>
          Some(S3)
        case _ =>
          None
      }

    def required(uris: List[URI]): Set[Client] =
      uris.foldLeft(Set.empty[Client]) { (acc, uri) =>
        getByUri(uri) match {
          case Some(client) => acc + client
          case None => acc // This should short-circuit on initialisation
        }
      }
  }

  def mkS3[F[_]: ConcurrentEffect]: F[S3Store[F]] =
    Sync[F].delay(S3AsyncClient.builder().build()).flatMap(client => S3Store[F](client))

  def mkGCS[F[_]: ConcurrentEffect: ContextShift](blocker: Blocker): F[GcsStore[F]] =
    Sync[F].delay(StorageOptions.getDefaultInstance.getService).map { storage =>
      GcsStore(storage, blocker, List.empty)
    }

  def mkHTTP[F[_]: ConcurrentEffect](ec: ExecutionContext): Resource[F, HttpClient[F]] =
    BlazeClientBuilder[F](ec).resource

  /** Initialise all necessary clients capable of fetching provides `uris` */
  def make[F[_]: ConcurrentEffect: ContextShift](
    blocker: Blocker,
    uris: List[URI],
    http: HttpClient[F]
  ): Resource[F, Clients[F]] = {
    val toInit = Client.required(uris)
    for {
      s3 <- if (toInit.contains(Client.S3)) Resource.eval(mkS3[F]).map(_.some) else Resource.pure[F, Option[S3Store[F]]](none)
      gcs <- if (toInit.contains(Client.GCS)) Resource.eval(mkGCS[F](blocker).map(_.some)) else Resource.pure[F, Option[GcsStore[F]]](none)
    } yield Clients(s3, gcs, http)
  }

  case class DownloadingFailure(uri: URI) extends Throwable {
    override def getMessage: String = s"Cannot download $uri"
  }
}
