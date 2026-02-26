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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import java.net.URI

import scala.io.Source

import cats.data.{EitherT, NonEmptyList, ValidatedNel}
import cats.implicits._
import cats.effect.{Resource, Sync}

import io.circe._
import io.circe.generic.semiauto._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingData}

import com.snowplowanalytics.snowplow.enrich.common.utils.CirceUtils

object AsnLookupsEnrichment extends ParseableEnrichment {
  override val supportedSchema = SchemaCriterion(
    "com.snowplowanalytics.snowplow.enrichments",
    "asn_lookups",
    "jsonschema",
    1,
    0
  )

  /**
   * Creates an AsnLookupsConf from a Json.
   * @param c The asn_lookups enrichment JSON
   * @param schemaKey schema of the config file provided by `EnrichmentRegistry`, must be supported by this enrichment
   * @param localMode Whether to use the local ASN file, enabled for tests
   * @return an AsnLookups configuration
   */
  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean
  ): ValidatedNel[String, EnrichmentConf.AsnLookupsConf] =
    isParseable(c, schemaKey)
      .leftMap(e => NonEmptyList.one(e))
      .flatMap { _ =>
        (
          getBotAsnsFileFromConfig(c).sequence,
          getBotAsnsFromConfig(c),
          getBypassPlatformsFromConfig(c)
        ).mapN { (botAsnsFile, botAsns, bypassPlatforms) =>
          EnrichmentConf.AsnLookupsConf(
            schemaKey,
            file(botAsnsFile, localMode),
            botAsns,
            bypassPlatforms
          )
        }.toEither
      }
      .toValidated

  private def getBotAsnsFileFromConfig(config: Json): Option[ValidatedNel[String, (URI, String)]] =
    if (config.hcursor.downField("parameters").downField("botAsnsFile").focus.isDefined) {
      val uri = CirceUtils.extract[String](config, "parameters", "botAsnsFile", "uri")
      val db = CirceUtils.extract[String](config, "parameters", "botAsnsFile", "database")

      (for {
        uriAndDb <- (uri.toValidatedNel, db.toValidatedNel).mapN((_, _)).toEither
        uri <- getDatabaseUri(uriAndDb._1, uriAndDb._2).leftMap(NonEmptyList.one)
      } yield (uri, uriAndDb._2)).toValidated.some
    } else None

  private def getBotAsnsFromConfig(config: Json): ValidatedNel[String, Set[Long]] =
    CirceUtils
      .extract[Option[List[BotAsn]]](config, "parameters", "botAsns")
      .map(_.getOrElse(Nil).map(_.asn).toSet)
      .toValidatedNel

  final case class BotAsn(asn: Long, name: Option[String]) // name is used only for parsing, it gets discarded
  object BotAsn {
    implicit val botAsnDecoder: Decoder[BotAsn] = deriveDecoder[BotAsn]
  }

  private def getBypassPlatformsFromConfig(config: Json): ValidatedNel[String, Set[String]] =
    CirceUtils
      .extract[Option[List[String]]](config, "parameters", "bypassPlatforms")
      .map(_.getOrElse(Nil).toSet)
      .toValidatedNel

  private def file(db: Option[(URI, String)], localMode: Boolean): Option[(URI, String)] =
    db.map {
      case (uri, database) =>
        if (localMode)
          (uri, Option(getClass.getResource(database)).getOrElse(getClass.getResource("/" + database)).toURI.getPath)
        else
          (uri, s"./asn_$database")
    }

  def create[F[_]: Sync](
    botAsnsFile: Option[String],
    botAsns: Set[Long],
    bypassPlatforms: Set[String]
  ): EitherT[F, String, AsnLookupsEnrichment] =
    botAsnsFile match {
      case Some(path) =>
        parseCsvFile[F](path).map { csvAsns =>
          AsnLookupsEnrichment(csvAsns ++ botAsns, bypassPlatforms)
        }
      case None =>
        EitherT.rightT(AsnLookupsEnrichment(botAsns, bypassPlatforms))
    }

  private def parseCsvFile[F[_]: Sync](path: String): EitherT[F, String, Set[Long]] =
    EitherT {
      Resource
        .fromAutoCloseable(Sync[F].blocking(Source.fromFile(path)))
        .use { file =>
          Sync[F].blocking {
            file.getLines.drop(1).map(_.split(",").head.trim.toLong).toSet
          }
        }
        .attempt
    }
      .leftMap(e => s"Failed to parse CSV file with ASNs. Error: ${e.getMessage}")
}

final case class AsnLookupsEnrichment(
  botAsns: Set[Long],
  bypassPlatforms: Set[String]
) {

  /**
   * Enriches ASN context with likelyBot field (if event not bypassed)
   * @param asnContext The ASN context to enrich
   * @param platform The event platform, used to potentially bypass the enrichment
   * @return Some(enriched context) with likelyBot set to true or false, or None if the platform is in bypassPlatforms
   */
  def lookupAsn(asnContext: SelfDescribingData[Json], platform: Option[String]): Option[SelfDescribingData[Json]] =
    if (platform.exists(bypassPlatforms.contains))
      None
    else
      asnContext.data.hcursor.get[Long]("number").toOption.map { asn =>
        val organization = asnContext.data.hcursor.get[String]("organization").toOption
        IpLookupsEnrichment.makeAsnContext(asn, organization, likelyBot = Some(likelyBot(asn)))
      }

  private def likelyBot(asn: Long): Boolean =
    botAsns.contains(asn)
}
