/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.gcp

import java.net.URI

import cats.implicits._
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Timer}

import fs2.Stream

import blobstore.gcs.GcsStore
import blobstore.url.Url

import com.google.cloud.storage.StorageOptions
import com.google.cloud.BaseServiceException

import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Clients.{Client, RetryableFailure}

object GcsClient {

  def mk[F[_]: ConcurrentEffect: ContextShift: Timer](blocker: Blocker): F[Client[F]] =
    ConcurrentEffect[F].delay(StorageOptions.getDefaultInstance.getService).map { service =>
      new Client[F] {
        val store = GcsStore.builder(service, blocker).unsafe

        def canDownload(uri: URI): Boolean = uri.getScheme == "gs"

        def download(uri: URI): Stream[F, Byte] =
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
