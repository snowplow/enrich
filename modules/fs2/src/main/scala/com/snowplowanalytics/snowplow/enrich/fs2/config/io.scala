package com.snowplowanalytics.snowplow.enrich.fs2.config

import _root_.io.circe.{Decoder, Encoder}
import _root_.io.circe.generic.extras.semiauto._

object io {

  sealed trait Authentication extends Product with Serializable

  object Authentication {
    final case class Gcp(projectId: String) extends Authentication
    final case class Aws() extends Authentication

    implicit val authenticationDecoder: Decoder[Authentication] =
      deriveConfiguredDecoder[Authentication]
    implicit val authenticationEncoder: Encoder[Authentication] =
      deriveConfiguredEncoder[Authentication]
  }

  /** Source of raw collector data (only PubSub supported atm) */
  sealed trait Input

  object Input {

    case class PubSub(subscriptionId: String) extends Input

    implicit val inputDecoder: Decoder[Input] =
      deriveConfiguredDecoder[Input]
    implicit val inputEncoder: Encoder[Input] =
      deriveConfiguredEncoder[Input]
  }

  sealed trait Output

  object Output {
    case class PubSub(topic: String) extends Output

    implicit val outputDecoder: Decoder[Output] =
      deriveConfiguredDecoder[Output]
    implicit val outputEncoder: Encoder[Output] =
      deriveConfiguredEncoder[Output]
  }

}
