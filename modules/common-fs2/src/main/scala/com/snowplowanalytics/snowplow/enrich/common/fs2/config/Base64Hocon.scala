/*
 * Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.fs2.config

import java.util.Base64

import cats.data.ValidatedNel
import cats.syntax.either._

import com.typesafe.config.{ConfigException, ConfigFactory}

import _root_.io.circe.Json

import pureconfig.syntax._
import pureconfig.module.circe._

import com.monovore.decline.Argument

final case class Base64Hocon(value: Json) extends AnyVal

object Base64Hocon {

  private val base64 = Base64.getDecoder

  implicit val base64Hocon: Argument[Base64Hocon] =
    new Argument[Base64Hocon] {
      def read(string: String): ValidatedNel[String, Base64Hocon] = {
        val result = for {
          bytes <- Either.catchOnly[IllegalArgumentException](base64.decode(string)).leftMap(_.getMessage)
          hocon <- parseHocon(new String(bytes))
        } yield hocon
        result.toValidatedNel
      }

      def defaultMetavar: String = "base64"
    }

  def parseHocon(str: String): Either[String, Base64Hocon] =
    for {
      configValue <- Either.catchOnly[ConfigException](ConfigFactory.parseString(str)).leftMap(_.toString).map(_.toConfig)
      json <- configValue.to[Json].leftMap(_.toString)
    } yield Base64Hocon(json)
}
