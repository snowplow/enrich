/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.config

import java.net.URI

import cats.syntax.either._

import _root_.io.circe.{Decoder, Encoder}
import _root_.io.circe.generic.extras.semiauto._

case class Sentry(dsn: URI)

object Sentry {

  implicit val javaNetUriDecoder: Decoder[URI] =
    Decoder[String].emap { str =>
      Either.catchOnly[IllegalArgumentException](URI.create(str)).leftMap(_.getMessage)
    }

  implicit val javaNetUriEncoder: Encoder[URI] =
    Encoder[String].contramap(_.toString)

  implicit val sentryDecoder: Decoder[Sentry] =
    deriveConfiguredDecoder[Sentry]
  implicit val sentryEncoder: Encoder[Sentry] =
    deriveConfiguredEncoder[Sentry]
}
