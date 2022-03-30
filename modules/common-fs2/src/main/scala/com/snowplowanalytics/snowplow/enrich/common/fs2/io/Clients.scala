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

import cats.effect.{ConcurrentEffect, Resource}

import fs2.Stream

import org.http4s.{Request, Uri}
import org.http4s.client.defaults
import org.http4s.client.{Client => Http4sClient}
import org.http4s.client.blaze.BlazeClientBuilder

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

import Clients._

case class Clients[F[_]: ConcurrentEffect](clients: List[Client[F]]) {

  /** Download a URI as a stream of bytes, using the appropriate client */
  def download(uri: URI): Stream[F, Byte] =
    clients.find(_.prefixes.contains(uri.getScheme())) match {
      case Some(client) =>
        client.download(uri)
      case None =>
        Stream.raiseError[F](new IllegalStateException(s"No client initialized to download $uri"))
    }
}

object Clients {
  def init[F[_]: ConcurrentEffect](httpClient: Http4sClient[F], others: List[Client[F]]): Clients[F] =
    Clients(wrapHttpClient(httpClient) :: others)

  def wrapHttpClient[F[_]: ConcurrentEffect](client: Http4sClient[F]): Client[F] =
    new Client[F] {
      val prefixes = List("http", "https")

      def download(uri: URI): Stream[F, Byte] = {
        val request = Request[F](uri = Uri.unsafeFromString(uri.toString))
        for {
          response <- client.stream(request)
          body <- if (response.status.isSuccess) response.body
                  else Stream.raiseError[F](HttpDownloadFailure(uri))
        } yield body
      }
    }

  def mkHttp[F[_]: ConcurrentEffect](
    connectionTimeout: FiniteDuration = defaults.ConnectTimeout,
    readTimeout: FiniteDuration = defaults.RequestTimeout,
    maxConnections: Int = 10, // http4s uses 10 by default
    ec: ExecutionContext
  ): Resource[F, Http4sClient[F]] =
    BlazeClientBuilder[F](ec)
      .withConnectTimeout(connectionTimeout)
      .withRequestTimeout(readTimeout)
      .withMaxTotalConnections(maxConnections)
      .resource

  trait RetryableFailure extends Throwable

  case class HttpDownloadFailure(uri: URI) extends RetryableFailure {
    override def getMessage: String = s"Cannot download $uri"
  }

  trait Client[F[_]] {
    val prefixes: List[String]
    def download(uri: URI): Stream[F, Byte]
  }
}
