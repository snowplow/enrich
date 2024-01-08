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
package com.snowplowanalytics.snowplow.enrich.aws

import java.net.URI

import cats.implicits._
import cats.effect.{ConcurrentEffect, Resource, Timer}

import fs2.Stream

import blobstore.url.Url
import blobstore.s3.S3Store

import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.core.exception.SdkException
import software.amazon.awssdk.regions.providers.AwsRegionProviderChain
import software.amazon.awssdk.regions.Region

import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Clients.{Client, RetryableFailure}

object S3Client {

  def mk[F[_]: ConcurrentEffect: Timer]: Resource[F, Client[F]] =
    for {
      s3Client <- Resource.fromAutoCloseable(ConcurrentEffect[F].delay(S3AsyncClient.builder().region(getRegion).build()))
      store <- Resource.eval(S3Store.builder[F](s3Client).build.toEither.leftMap(_.head).pure[F].rethrow)
    } yield new Client[F] {
      def canDownload(uri: URI): Boolean =
        uri.getScheme == "s3"

      def download(uri: URI): Stream[F, Byte] =
        Stream.eval(Url.parseF[F](uri.toString)).flatMap { url =>
          store
            .get(url, 16 * 1024)
            .handleErrorWith { e =>
              val e2 = e match {
                case sdke: SdkException if sdke.retryable =>
                  new RetryableFailure {
                    override def getMessage: String = sdke.getMessage
                    override def getCause: Throwable = sdke
                  }
                case e => e
              }
              Stream.raiseError[F](e2)
            }
        }
    }

  private def getRegion(): Region =
    Either.catchNonFatal(new AwsRegionProviderChain().getRegion) match {
      case Right(region) =>
        region
      case _ =>
        Region.EU_CENTRAL_1
    }
}

