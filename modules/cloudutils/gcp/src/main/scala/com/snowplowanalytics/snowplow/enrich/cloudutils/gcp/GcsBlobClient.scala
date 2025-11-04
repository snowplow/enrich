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
package com.snowplowanalytics.snowplow.enrich.cloudutils.gcp

import java.net.URI

import cats.implicits._
import cats.effect.kernel.{Async, Resource, Sync}

import fs2.Stream

import blobstore.gcs.GcsStore
import blobstore.url.Url

import com.google.cloud.storage.StorageOptions
import com.google.cloud.BaseServiceException

import com.snowplowanalytics.snowplow.enrich.cloudutils.core._

object GcsBlobClient {

  def client[F[_]: Async]: BlobClient[F] =
    new BlobClient[F] {

      override def canDownload(uri: URI): Boolean =
        uri.getScheme == "gs"

      override def mk: Resource[F, BlobClientImpl[F]] =
        for {
          service <- Resource.fromAutoCloseable(Sync[F].delay(StorageOptions.getDefaultInstance.getService))
          store <- Resource.eval(GcsStore.builder[F](service).build.toEither.leftMap(_.head).pure[F].rethrow)
        } yield new BlobClientImpl[F] {

          override def download(uri: URI): Stream[F, Byte] =
            Stream.eval(Url.parseF[F](uri.toString)).flatMap { url =>
              store
                .get(url, 16 * 1024)
                .handleErrorWith { e =>
                  val e2 = e match {
                    case bse: BaseServiceException if bse.isRetryable =>
                      new RetryableFailure {
                        override def getMessage: String = bse.getMessage
                        override def getCause: Throwable = bse
                      }
                    case e => e
                  }
                  Stream.raiseError[F](e2)
                }
            }
        }
    }
}
