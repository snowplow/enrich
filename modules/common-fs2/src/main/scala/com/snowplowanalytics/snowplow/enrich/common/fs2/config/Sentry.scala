/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
