/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.io

import java.net.URI

import scala.concurrent.duration._

import org.typelevel.ci.CIString

import cats.effect.kernel.{Async, Resource}

import fs2.Stream

import org.http4s.{Headers, Request, Uri}
import org.http4s.client.defaults
import org.http4s.client.{Client => Http4sClient}
import org.http4s.client.middleware.Retry
import org.http4s.ember.client.EmberClientBuilder

import Clients._

case class Clients[F[_]: Async](clients: List[Client[F]]) {

  /** Download a URI as a stream of bytes, using the appropriate client */
  def download(uri: URI): Stream[F, Byte] =
    clients.find(_.canDownload(uri)) match {
      case Some(client) =>
        client.download(uri)
      case None =>
        Stream.raiseError[F](new IllegalStateException(s"No client initialized to download $uri"))
    }
}

object Clients {
  def init[F[_]: Async](httpClient: Http4sClient[F], others: List[Client[F]]): Clients[F] =
    Clients(wrapHttpClient(httpClient) :: others)

  def wrapHttpClient[F[_]: Async](client: Http4sClient[F]): Client[F] =
    new Client[F] {
      def canDownload(uri: URI): Boolean =
        // Since Azure Blob Storage urls' scheme are https as well and we want to fetch them with
        // their own client, we added second condition to not pick up those urls
        (uri.getScheme == "http" || uri.getScheme == "https") && !uri.toString.contains("core.windows.net")

      def download(uri: URI): Stream[F, Byte] = {
        val request = Request[F](uri = Uri.unsafeFromString(uri.toString))
        for {
          response <- client.stream(request)
          body <- if (response.status.isSuccess) response.body
                  else Stream.raiseError[F](HttpDownloadFailure(uri))
        } yield body
      }
    }

  def mkHttp[F[_]: Async](
    connectionTimeout: FiniteDuration = defaults.ConnectTimeout,
    readTimeout: FiniteDuration = defaults.RequestTimeout,
    maxConnections: Int = 100 // default of Ember client
  ): Resource[F, Http4sClient[F]] = {
    val builder = EmberClientBuilder
      .default[F]
      .withTimeout(readTimeout)
      .withIdleConnectionTime(connectionTimeout)
      .withMaxTotal(maxConnections)
    val retryPolicy = builder.retryPolicy
    builder.build.map(Retry[F](retryPolicy, redactHeadersWhen))
  }

  private def redactHeadersWhen(header: CIString) =
    (Headers.SensitiveHeaders + CIString("apikey")).contains(header)

  trait RetryableFailure extends Throwable

  case class HttpDownloadFailure(uri: URI) extends RetryableFailure {
    override def getMessage: String = s"Cannot download $uri"
  }

  trait Client[F[_]] {
    def canDownload(uri: URI): Boolean
    def download(uri: URI): Stream[F, Byte]
  }
}
