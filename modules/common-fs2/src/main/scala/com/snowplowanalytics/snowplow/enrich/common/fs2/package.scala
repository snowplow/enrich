/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common

import cats.data.{EitherT, Validated, ValidatedNel}

import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

package object fs2 {

  type Parsed[F[_], A] = EitherT[F, String, A]

  type ValidationResult[A] = ValidatedNel[String, A]

  type ByteSink[F[_]] = List[Array[Byte]] => F[Unit]
  type AttributedByteSink[F[_]] = List[AttributedData[Array[Byte]]] => F[Unit]

  /** Enrichment result, containing list of (valid and invalid) results as well as the collector timestamp */
  type Result = (List[Validated[BadRow, EnrichedEvent]], Option[Long])

  /** Function to transform an origin raw payload into good and/or bad rows */
  type Enrich[F[_]] = Array[Byte] => F[Result]
}
