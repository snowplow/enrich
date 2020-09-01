package com.snowplowanalytics.snowplow.enrich.fs2.config

import scala.concurrent.duration.FiniteDuration

import java.net.URI

import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.functor._
import cats.syntax.show._
import cats.effect.{Blocker, ContextShift, Sync}

import _root_.io.circe.{Decoder, Encoder, Json}
import _root_.io.circe.config.syntax._
import _root_.io.circe.generic.extras.semiauto.{deriveConfiguredDecoder, deriveConfiguredEncoder}

import com.snowplowanalytics.snowplow.enrich.fs2.config.io.{Authentication, Input, Output}

import pureconfig.ConfigSource
import pureconfig.module.catseffect.syntax._
import pureconfig.module.circe._

/**
 * Parsed HOCON configuration file
 *
 * @param auth
 * @param input
 * @param good
 * @param bad
 * @param assetsUpdatePeriod time after which assets should be updated, in minutes
 */
final case class ConfigFile(
  auth: Authentication,
  input: Input,
  good: Output,
  bad: Output,
  assetsUpdatePeriod: Option[FiniteDuration],
  sentryDsn: Option[URI]
)

object ConfigFile {

  // Missing in circe-config
  implicit val finiteDurationEncoder: Encoder[FiniteDuration] =
    implicitly[Encoder[String]].contramap(_.toString)

  implicit val javaNetUriDecoder: Decoder[URI] =
    Decoder[String].emap { str =>
      Either.catchOnly[IllegalArgumentException](URI.create(str)).leftMap(_.getMessage)
    }

  implicit val javaNetUriEncoder: Encoder[URI] =
    Encoder[String].contramap(_.toString)

  implicit val configFileDecoder: Decoder[ConfigFile] =
    deriveConfiguredDecoder[ConfigFile]
  implicit val configFileEncoder: Encoder[ConfigFile] =
    deriveConfiguredEncoder[ConfigFile]

  def parse[F[_]: Sync: ContextShift](in: EncodedHoconOrPath): EitherT[F, String, ConfigFile] =
    in match {
      case Right(path) =>
        val result = Blocker[F].use { blocker =>
          ConfigSource
            .default(ConfigSource.file(path))
            .loadF[F, Json](blocker)
            .map(_.as[ConfigFile].leftMap(f => show"Couldn't parse the config $f"))
        }
        EitherT(result)
      case Left(encoded) =>
        EitherT.fromEither[F](encoded.value.as[ConfigFile].leftMap(failure => show"Couldn't parse a base64-encoded config file:\n$failure"))
    }
}
