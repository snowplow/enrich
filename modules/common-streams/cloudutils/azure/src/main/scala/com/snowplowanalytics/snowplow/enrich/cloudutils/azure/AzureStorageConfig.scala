/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.cloudutils.azure

import io.circe.{Decoder, DecodingFailure}
import io.circe.generic.semiauto._

case class AzureStorageConfig(accounts: List[AzureStorageConfig.Account])

object AzureStorageConfig {

  final case class Account(name: String, auth: Option[Account.Auth])

  object Account {

    sealed trait Auth

    object Auth {

      final case object DefaultCredentialsChain extends Auth

      final case class SasToken(value: String) extends Auth

      implicit val sasTokenDecoder: Decoder[SasToken] = deriveDecoder[SasToken]

      implicit val accountAuthDecoder: Decoder[Auth] =
        Decoder.instance { cursor =>
          val typeCur = cursor.downField("type")
          typeCur.as[String].map(_.toLowerCase) match {
            case Right("default") =>
              Right(DefaultCredentialsChain)
            case Right("sas") =>
              cursor.as[SasToken]
            case Right(other) =>
              Left(
                DecodingFailure(s"Storage account authentication type '$other' is not supported yet. Supported types: 'default', 'sas'",
                  typeCur.history
                )
              )
            case Left(other) =>
              Left(other)
          }
        }
    }

    implicit val storageAccountDecoder: Decoder[Account] = deriveDecoder[Account]
  }

  implicit val azureStorageDecoder: Decoder[AzureStorageConfig] = deriveDecoder[AzureStorageConfig]
}
