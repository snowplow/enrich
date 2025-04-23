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

import cats.effect.kernel.Resource

import fs2.Stream

trait BlobClient[F[_]] {
  def canDownload(uri: URI): Boolean
  def mk: Resource[F, BlobClientImpl[F]]
}

trait BlobClientImpl[F[_]] {
  def canDownload(uri: URI): Boolean
  def download(uri: URI): Stream[F, Byte]
}

trait RetryableFailure extends Throwable
