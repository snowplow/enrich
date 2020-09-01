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

import java.util.Base64

import cats.data.ValidatedNel
import cats.syntax.show._
import cats.syntax.either._

import _root_.io.circe.Json
import _root_.io.circe.parser.parse

import com.monovore.decline.Argument

final case class Base64Json(value: Json) extends AnyVal

object Base64Json {

  private val base64 = Base64.getDecoder

  implicit val base64Json: Argument[Base64Json] =
    new Argument[Base64Json] {

      def read(string: String): ValidatedNel[String, Base64Json] = {
        val result = for {
          bytes <- Either.catchOnly[IllegalArgumentException](base64.decode(string)).leftMap(_.getMessage)
          str = new String(bytes)
          json <- parse(str).leftMap(_.show)
        } yield Base64Json(json)
        result.toValidatedNel
      }

      def defaultMetavar: String = "base64"
    }
}
