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
package com.snowplowanalytics.snowplow.enrich.fs2.config

import java.nio.file.{InvalidPathException, Path, Paths}

import cats.syntax.either._

import _root_.io.circe.{Decoder, Encoder}
import _root_.io.circe.generic.extras.semiauto._

object io {

  implicit val javaPathDecoder: Decoder[Path] =
    Decoder[String].emap { s =>
      Either.catchOnly[InvalidPathException](Paths.get(s)).leftMap(_.getMessage)
    }
  implicit val javaPathEncoder: Encoder[Path] =
    Encoder[String].contramap(_.toString)

  sealed trait Authentication extends Product with Serializable

  object Authentication {
    final case class Gcp(projectId: String) extends Authentication

    implicit val authenticationDecoder: Decoder[Authentication] =
      deriveConfiguredDecoder[Authentication]
    implicit val authenticationEncoder: Encoder[Authentication] =
      deriveConfiguredEncoder[Authentication]
  }

  /** Source of raw collector data (only PubSub supported atm) */
  sealed trait Input

  object Input {

    case class PubSub(subscriptionId: String) extends Input
    case class FileSystem(dir: Path) extends Input

    implicit val inputDecoder: Decoder[Input] =
      deriveConfiguredDecoder[Input]
    implicit val inputEncoder: Encoder[Input] =
      deriveConfiguredEncoder[Input]
  }

  sealed trait Output

  object Output {
    case class PubSub(topic: String) extends Output
    case class FileSystem(dir: Path) extends Output

    implicit val outputDecoder: Decoder[Output] =
      deriveConfiguredDecoder[Output]
    implicit val outputEncoder: Encoder[Output] =
      deriveConfiguredEncoder[Output]
  }
}
