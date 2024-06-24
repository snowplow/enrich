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

import java.nio.file.Path

import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._

import com.monovore.decline.{Argument, Command, Opts}

final case class CliConfig(
  config: EncodedHoconOrPath,
  resolver: EncodedHoconOrPath,
  enrichments: EncodedHoconOrPath
)

object CliConfig {

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

  val enrichments: Opts[EncodedHoconOrPath] =
    Opts.option[EncodedHoconOrPath]("enrichments", "Base64-encoded JSON string with enrichment configurations", "e", "base64")

  val igluConfig: Opts[EncodedHoconOrPath] =
    Opts.option[EncodedHoconOrPath]("iglu-config", "Iglu resolver configuration HOCON", "r", "base64")

  val enrichedJobConfig: Opts[CliConfig] =
    (configFile, igluConfig, enrichments).mapN(CliConfig.apply)

  def command(
    appName: String,
    appVersion: String,
    appDescription: String
  ): Command[CliConfig] =
    Command(show"$appName", show"$appName $appVersion\n$appDescription")(enrichedJobConfig)
}
