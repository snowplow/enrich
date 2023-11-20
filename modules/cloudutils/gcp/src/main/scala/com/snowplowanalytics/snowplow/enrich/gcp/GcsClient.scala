/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
