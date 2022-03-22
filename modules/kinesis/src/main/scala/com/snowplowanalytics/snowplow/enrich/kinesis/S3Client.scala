/*
 * Copyright (c) 2021-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.kinesis

import java.net.URI

import cats.effect.{ConcurrentEffect, Resource}

import fs2.Stream

import blobstore.Path
import blobstore.s3.S3Store

import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.core.exception.SdkException

import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Clients.{Client, RetryableFailure}

object S3Client {

  def mk[F[_]: ConcurrentEffect]: Resource[F, Client[F]] =
    for {
      s3Client <- Resource.fromAutoCloseable(ConcurrentEffect[F].delay(S3AsyncClient.builder().build()))
      store <- Resource.eval(S3Store[F](s3Client))
    } yield new Client[F] {
      val prefixes = List("s3")

      def download(uri: URI): Stream[F, Byte] =
        store
          .get(Path(uri.toString), 16 * 1024)
          .handleErrorWith { e =>
            val e2 = e match {
              case sdke: SdkException if sdke.retryable =>
                new RetryableFailure {
                  override def getMessage: String = sdke.getMessage
                  override def getCause: Throwable = sdke
                }
              case e => e
            }
            Stream.raiseError(e2)
          }
    }
}
