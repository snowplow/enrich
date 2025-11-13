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

import cats.implicits._
import cats.effect.kernel.{Async, Resource}

import org.http4s.client.{Client => Http4sClient}
import org.http4s.{Request, Response, Status, Uri}
import org.http4s.headers.{ETag, `If-None-Match`}

import scodec.bits.ByteVector

object HttpBlobClient {

  def wrapHttp4sClient[F[_]: Async](httpClient: Http4sClient[F]): BlobClientFactory[F] =
    new BlobClientFactory[F] {

      // Since Azure Blob Storage urls' scheme are https as well and we want to fetch them with
      // their own client, we added second condition to not pick up those urls
      override def canDownload(uri: URI): Boolean =
        (uri.getScheme === "http" || uri.getScheme === "https") && !uri.toString.contains("core.windows.net")

      override def mk: Resource[F, BlobClient[F]] =
        Resource.pure {
          new BlobClient[F] {

            private def handleResponseWithBody(
              response: Response[F]
            ): F[BlobClient.GetResult] = {
              val responseEtag = response.headers.get[ETag].map(_.tag.tag)
              response.body.compile
                .to(ByteVector)
                .map { byteVector =>
                  val content = byteVector.toByteBuffer
                  responseEtag match {
                    case Some(etag) => BlobClient.ContentWithEtag(content, etag)
                    case None => BlobClient.ContentNoEtag(content)
                  }
                }
            }

            override def get(uri: URI): F[BlobClient.GetResult] = {
              val request = Request[F](uri = Uri.unsafeFromString(uri.toString))
              httpClient
                .run(request)
                .use[Either[Throwable, BlobClient.GetResult]] { response =>
                  if (response.status.isSuccess)
                    handleResponseWithBody(response).map(Right(_))
                  else
                    Async[F].pure(Left(HttpErrorResponse(uri, response.status.code)))
                }
                .handleErrorWith { e =>
                  Async[F].raiseError(HttpException(uri, e))
                }
                .rethrow
            }

            override def getIfNeeded(
              uri: URI,
              etag: String
            ): F[BlobClient.GetIfNeededResult] = {
              val etagHeader = `If-None-Match`(ETag.EntityTag(etag))
              val request = Request[F](uri = Uri.unsafeFromString(uri.toString))
                .withHeaders(etagHeader)

              httpClient
                .run(request)
                .use[Either[Throwable, BlobClient.GetIfNeededResult]] { response =>
                  if (response.status === Status.NotModified)
                    // 304 Not Modified - etag matched, no need to download
                    Async[F].pure(Right(BlobClient.EtagMatched))
                  else if (response.status.isSuccess)
                    // Download needed - etag didn't match or server doesn't support conditional requests
                    handleResponseWithBody(response).map(Right(_))
                  else
                    Async[F].pure(Left(HttpErrorResponse(uri, response.status.code)))
                }
                .handleErrorWith { e =>
                  Async[F].raiseError(HttpException(uri, e))
                }
                .rethrow
            }
          }
        }
    }

  case class HttpErrorResponse(uri: URI, statusCode: Int) extends RetryableFailure {
    override def getMessage: String = s"Cannot GET $uri (status code: $statusCode)"
  }

  case class HttpException(uri: URI, cause: Throwable) extends RetryableFailure {
    override def getMessage: String = s"Exception during GET of $uri: ${cause.getMessage}"
    override def getCause: Throwable = cause
  }
}
