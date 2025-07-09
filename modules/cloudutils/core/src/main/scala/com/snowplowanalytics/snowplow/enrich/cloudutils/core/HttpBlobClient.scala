/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.enrich.cloudutils.core

import java.net.URI

import cats.effect.kernel.{Resource, Sync}

import org.http4s.client.{Client => Http4sClient}
import org.http4s.{Request, Uri}

import fs2.Stream

object HttpBlobClient {

  private def canDownload(uri: URI): Boolean =
    // Since Azure Blob Storage urls' scheme are https as well and we want to fetch them with
    // their own client, we added second condition to not pick up those urls
    (uri.getScheme == "http" || uri.getScheme == "https") && !uri.toString.contains("core.windows.net")

  def wrapHttp4sClient[F[_]: Sync](httpClient: Http4sClient[F]) =
    new BlobClient[F] {
      override def canDownload(uri: URI): Boolean = HttpBlobClient.canDownload(uri: URI)

      override def mk: Resource[F, BlobClientImpl[F]] =
        Resource.pure {
          new BlobClientImpl[F] {
            override def canDownload(uri: URI): Boolean = HttpBlobClient.canDownload(uri)

            override def download(uri: URI): Stream[F, Byte] = {
              val request = Request[F](uri = Uri.unsafeFromString(uri.toString))
              for {
                response <- httpClient.stream(request)
                body <- if (response.status.isSuccess) response.body
                        else Stream.raiseError[F](HttpDownloadFailure(uri))
              } yield body
            }
          }
        }
    }

  case class HttpDownloadFailure(uri: URI) extends RetryableFailure {
    override def getMessage: String = s"Cannot download $uri"
  }
}
