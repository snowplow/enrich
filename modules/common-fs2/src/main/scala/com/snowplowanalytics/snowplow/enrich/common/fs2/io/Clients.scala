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

import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Resource, Timer}

import fs2.Stream

import org.http4s.{Request, Uri}
import org.http4s.client.{Client => HttpClient}
import org.http4s.ember.client.EmberClientBuilder

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
  def init[F[_]: ConcurrentEffect](httpClient: HttpClient[F], others: List[Client[F]]): Clients[F] =
    Clients(wrapHttpClient(httpClient) :: others)

  def wrapHttpClient[F[_]: ConcurrentEffect](client: HttpClient[F]): Client[F] =
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

  def mkHttp[F[_]: Concurrent: Timer: ContextShift]: Resource[F, HttpClient[F]] =
    EmberClientBuilder.default[F].build

  trait RetryableFailure extends Throwable

  case class HttpDownloadFailure(uri: URI) extends RetryableFailure {
    override def getMessage: String = s"Cannot download $uri"
  }

  trait Client[F[_]] {
    val prefixes: List[String]
    def download(uri: URI): Stream[F, Byte]
  }
}
