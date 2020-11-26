/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.fs2

import java.net.URI
import java.nio.file.Path

import scala.concurrent.duration.FiniteDuration

import _root_.cats.syntax.either._

import _root_.io.circe.{Encoder, Decoder}
import _root_.io.circe.generic.extras.Configuration

package object config {

  type EncodedOrPath = Either[Base64Json, Path]
  type EncodedHoconOrPath = Either[Base64Hocon, Path]

  private[config] implicit def customCodecConfig: Configuration =
    Configuration.default.withDiscriminator("type")

  // Missing in circe-config
  implicit val finiteDurationEncoder: Encoder[FiniteDuration] =
    implicitly[Encoder[String]].contramap(_.toString)

  implicit val javaNetUriDecoder: Decoder[URI] =
    Decoder[String].emap { str =>
      Either.catchOnly[IllegalArgumentException](URI.create(str)).leftMap(_.getMessage)
    }

  implicit val javaNetUriEncoder: Encoder[URI] =
    Encoder[String].contramap(_.toString)

}
