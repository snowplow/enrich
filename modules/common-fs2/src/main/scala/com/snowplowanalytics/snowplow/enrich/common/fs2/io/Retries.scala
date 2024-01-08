/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.io

import retry.{RetryPolicies, RetryPolicy}

import cats.Applicative

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.BackoffPolicy

object Retries {

  def fullJitter[F[_]: Applicative](config: BackoffPolicy): RetryPolicy[F] =
    capBackoffAndRetries(config, RetryPolicies.fullJitter[F](config.minBackoff))

  def fibonacci[F[_]: Applicative](config: BackoffPolicy): RetryPolicy[F] =
    capBackoffAndRetries(config, RetryPolicies.fibonacciBackoff[F](config.minBackoff))

  private def capBackoffAndRetries[F[_]: Applicative](config: BackoffPolicy, policy: RetryPolicy[F]): RetryPolicy[F] = {
    val capped = RetryPolicies.capDelay[F](config.maxBackoff, policy)
    config.maxRetries.fold(capped)(max => capped.join(RetryPolicies.limitRetries(max)))
  }

}
