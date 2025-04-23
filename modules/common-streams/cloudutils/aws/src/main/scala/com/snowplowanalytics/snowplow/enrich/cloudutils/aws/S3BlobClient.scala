/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.cloudutils.aws

import java.net.URI

import cats.implicits._
import cats.effect.kernel.{Async, Resource, Sync}

import fs2.Stream

import blobstore.url.Url
import blobstore.s3.S3Store

import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode

import com.snowplowanalytics.snowplow.enrich.cloudutils.core._

object S3BlobClient {

  private def canDownload(uri: URI): Boolean =
    uri.getScheme == "s3"

  def client[F[_]: Async]: BlobClient[F] = new BlobClient[F] {
    override def canDownload(uri: URI) = S3BlobClient.canDownload(uri)

    override def mk: Resource[F, BlobClientImpl[F]] =
    for {
      s3Client <- Resource.fromAutoCloseable(Sync[F].delay(S3AsyncClient.builder().defaultsMode(DefaultsMode.AUTO).build()))
      store <- Resource.eval(S3Store.builder[F](s3Client).build.toEither.leftMap(_.head).pure[F].rethrow)
    } yield new BlobClientImpl[F] {

      override def canDownload(uri: URI) = S3BlobClient.canDownload(uri)

      override def download(uri: URI): Stream[F, Byte] =
        Stream.eval(Url.parseF[F](uri.toString)).flatMap { url =>
          store.get(url, 16 * 1024)
        }
    }
  }
}
