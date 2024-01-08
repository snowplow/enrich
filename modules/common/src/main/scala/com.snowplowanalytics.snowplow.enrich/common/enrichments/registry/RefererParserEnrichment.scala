/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import java.net.URI

import cats.data.{EitherT, NonEmptyList, ValidatedNel}
import cats.implicits._

import cats.effect.Sync

import io.circe.Json

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}

import com.snowplowanalytics.refererparser._

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.RefererParserConf
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils => CU, CirceUtils}

/** Companion object. Lets us create a RefererParserEnrichment from a Json */
object RefererParserEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "referer_parser", "jsonschema", 2, 0)

  private val localFile = "./referer-parser.json"

  /**
   * Creates a RefererParserConf from a Json.
   * @param c The referer_parser enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return a referer parser enrichment configuration
   */
  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean
  ): ValidatedNel[String, RefererParserConf] =
    (for {
      _ <- isParseable(c, schemaKey).leftMap(NonEmptyList.one)
      // better-monadic-for
      conf <- (
                  CirceUtils.extract[String](c, "parameters", "uri").toValidatedNel,
                  CirceUtils.extract[String](c, "parameters", "database").toValidatedNel,
                  CirceUtils.extract[List[String]](c, "parameters", "internalDomains").toValidatedNel
              ).mapN { (uri, db, domains) =>
                (uri, db, domains)
              }.toEither
      source <- getDatabaseUri(conf._1, conf._2).leftMap(NonEmptyList.one)
    } yield RefererParserConf(schemaKey, file(source, conf._2, localFile, localMode), conf._3)).toValidated

  private def file(
    uri: URI,
    db: String,
    localFile: String,
    localMode: Boolean
  ): (URI, String) =
    if (localMode)
      (uri, Option(getClass.getResource(db)).getOrElse(getClass.getResource("/" + db)).toURI.getPath)
    else
      (uri, localFile)

  def create[F[_]: Sync](filePath: String, internalDomains: List[String]): EitherT[F, String, RefererParserEnrichment] =
    EitherT(CreateParser[F].create(filePath))
      .leftMap(_.getMessage)
      .map(p => RefererParserEnrichment(p, internalDomains))
}

/**
 * Config for a referer_parser enrichment
 * @param parser Referer parser
 * @param domains List of internal domains
 */
final case class RefererParserEnrichment(parser: Parser, domains: List[String]) extends Enrichment {

  /**
   * Extract details about the referer (sic). Uses the referer-parser library.
   * @param uri The referer URI to extract referer details from
   * @param pageHost The host of the current page (used to determine if this is an internal referer)
   * @return a Tuple3 containing referer medium, source and term, all Strings
   */
  def extractRefererDetails(uri: URI, pageHost: String): Option[Referer] =
    parser.parse(uri, Option(pageHost), domains).map {
      case SearchReferer(m, s, t) =>
        val fixedTerm = t.flatMap(CU.fixTabsNewlines)
        SearchReferer(m, s, fixedTerm)
      case o => o
    }
}
