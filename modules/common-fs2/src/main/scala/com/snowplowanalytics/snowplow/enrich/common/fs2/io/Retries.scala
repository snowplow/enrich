/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
