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
package com.snowplowanalytics.snowplow.enrich.streams.common

import scala.concurrent.duration.FiniteDuration

import org.typelevel.ci.CIString

import cats.effect.kernel.{Async, Resource}

import fs2.io.net.Network

import org.http4s.Headers
import org.http4s.client.{Client => Http4sClient}
import org.http4s.client.middleware.Retry
import org.http4s.client.defaults
import org.http4s.ember.client.EmberClientBuilder

object HttpClient {
  def resource[F[_]: Async](
    timeout: FiniteDuration = defaults.RequestTimeout
  ): Resource[F, Http4sClient[F]] = {
    implicit val n = Network.forAsync[F]
    val builder = EmberClientBuilder
      .default[F]
      .withTimeout(timeout)
    val retryPolicy = builder.retryPolicy
    builder.build.map(Retry[F](retryPolicy, redactHeadersWhen))
  }

  private def redactHeadersWhen(header: CIString) =
    (Headers.SensitiveHeaders + CIString("apikey")).contains(header)
}
