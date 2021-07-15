/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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

import java.nio.file.Path

import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._

import com.monovore.decline.{Argument, Command, Opts}

final case class CliConfig(
  config: EncodedHoconOrPath,
  resolver: EncodedOrPath,
  enrichments: EncodedOrPath
)

object CliConfig {

  implicit val encodedOrPathArgument: Argument[EncodedOrPath] =
    new Argument[EncodedOrPath] {
      def read(string: String): ValidatedNel[String, EncodedOrPath] = {
        val encoded = Argument[Base64Json].read(string).map(_.asLeft)
        val path = Argument[Path].read(string).map(_.asRight)
        val error = show"Value $string cannot be parsed as Base64 JSON neither as FS path"
        encoded.orElse(path).leftMap(_ => NonEmptyList.one(error))
      }

      def defaultMetavar: String = "input"
    }

  implicit val encodedHoconOrPathArgument: Argument[EncodedHoconOrPath] =
    new Argument[EncodedHoconOrPath] {
      def read(string: String): ValidatedNel[String, EncodedHoconOrPath] = {
        val encoded = Argument[Base64Hocon].read(string).map(_.asLeft)
        val path = Argument[Path].read(string).map(_.asRight)
        val error = show"Value $string cannot be parsed as Base64 JSON neither as FS path"
        encoded.orElse(path).leftMap(_ => NonEmptyList.one(error))
      }

      def defaultMetavar: String = "input"
    }

  val configFile: Opts[EncodedHoconOrPath] =
    Opts.option[EncodedHoconOrPath]("config", "Base64-encoded HOCON string with enrichment configurations", "c", "base64")

  val enrichments: Opts[EncodedOrPath] =
    Opts.option[EncodedOrPath]("enrichments", "Base64-encoded JSON string with enrichment configurations", "e", "base64")

  val igluConfig: Opts[EncodedOrPath] =
    Opts.option[EncodedOrPath]("iglu-config", "Iglu resolver configuration JSON", "r", "base64")

  val enrichedJobConfig: Opts[CliConfig] =
    (configFile, igluConfig, enrichments).mapN(CliConfig.apply)

  def command(appName: String, appVersion: String, appDescription: String): Command[CliConfig] =
    Command(show"$appName", show"$appName $appVersion\n$appDescription")(enrichedJobConfig)
}
