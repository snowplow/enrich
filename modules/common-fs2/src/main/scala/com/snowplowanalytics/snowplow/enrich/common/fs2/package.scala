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
package com.snowplowanalytics.snowplow.enrich.common

import cats.data.{EitherT, Ior, ValidatedNel}

import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

package object fs2 {

  type Parsed[F[_], A] = EitherT[F, String, A]

  type ValidationResult[A] = ValidatedNel[String, A]

  type ByteSink[F[_]] = List[Array[Byte]] => F[Unit]
  type AttributedByteSink[F[_]] = List[AttributedData[Array[Byte]]] => F[Unit]

  type Enriched = Ior[BadRow, EnrichedEvent]
  type Result = (List[Enriched], Option[Long])

  /** Function to transform an origin raw payload into good and/or bad rows */
  type Enrich[F[_]] = Array[Byte] => F[Result]
}
