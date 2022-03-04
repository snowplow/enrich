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

import scala.concurrent.duration.FiniteDuration

import cats.data.EitherT
import cats.implicits._

import cats.effect.{Blocker, ContextShift, Sync}

import _root_.io.circe.{Decoder, Encoder, Json}
import _root_.io.circe.config.syntax._
import _root_.io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}

import pureconfig.ConfigSource
import pureconfig.module.catseffect.syntax._
import pureconfig.module.circe._

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Concurrency, Input, Monitoring, Output, Outputs, Telemetry}

/**
 * Parsed HOCON configuration file
 *
 * @param input input (PubSub, Kinesis etc)
 * @param output wraps good bad and pii outputs (PubSub, Kinesis, FS etc)
 * @param assetsUpdatePeriod time after which assets should be updated, in minutes
 * @param monitoring configuration for sentry and metrics
 * @param telemetry configuration for telemetry
 */
final case class ConfigFile(
  input: Input,
  output: Outputs,
  concurrency: Concurrency,
  assetsUpdatePeriod: Option[FiniteDuration],
  monitoring: Option[Monitoring],
  telemetry: Telemetry
)

object ConfigFile {

  // Missing in circe-config
  implicit val finiteDurationEncoder: Encoder[FiniteDuration] =
    implicitly[Encoder[String]].contramap(_.toString)

  implicit val configFileDecoder: Decoder[ConfigFile] =
    deriveConfiguredDecoder[ConfigFile].emap {
      case ConfigFile(_, _, _, Some(aup), _, _) if aup._1 <= 0L =>
        "assetsUpdatePeriod in config file cannot be less than 0".asLeft // TODO: use newtype
      // Remove pii output if streamName and region empty
      case c @ ConfigFile(_, Outputs(good, Some(Output.Kinesis(s, r, _, _, _, _, _, _, _, _, _, _, _)), bad), _, _, _)
        if(s.isEmpty && r.isEmpty) => c.copy(output = Outputs(good, None, bad)).asRight
      case other => other.asRight
    }
  implicit val configFileEncoder: Encoder[ConfigFile] =
    deriveConfiguredEncoder[ConfigFile]

  def parse[F[_]: Sync: ContextShift](in: EncodedHoconOrPath): EitherT[F, String, ConfigFile] =
    in match {
      case Right(path) =>
        val result = Blocker[F].use { blocker =>
          ConfigSource
            .file(path)
            .withFallback(ConfigSource.default)
            .loadF[F, Json](blocker)
            .map(_.as[ConfigFile].leftMap(f => show"Couldn't parse the config $f"))
        }
        result.attemptT.leftMap(_.getMessage).subflatMap(identity)
      case Left(encoded) =>
        EitherT.fromEither[F](encoded.value.as[ConfigFile].leftMap(failure => show"Couldn't parse a base64-encoded config file:\n$failure"))
    }
}
