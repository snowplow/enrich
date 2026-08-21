/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd.
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

import cats.data.{EitherT, NonEmptyList, ValidatedNel}
import cats.implicits._

import cats.effect.Sync

import io.circe.Json

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer}

import com.snowplowanalytics.refererparser._

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.RefererParserConf
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils => CU, CirceUtils}

/** Companion object. Lets us create a RefererParserEnrichment from a Json */
object RefererParserEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "referer_parser", "jsonschema", 2, 0)

  /** Schema for the entity emitted when a `utm_source` query parameter is classified as a referer */
  val utmReferrerSchema: SchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow", "utm_referrer", "jsonschema", SchemaVer.Full(1, 0, 0))

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
      maybeReferers = c.hcursor.downField("parameters").downField("referrers").focus
      referers <- maybeReferers.fold(ParsedReferers.empty.asRight[NonEmptyList[String]]) { json =>
                    ParseReferers
                      .loadJson(json)
                      .leftMap(e => NonEmptyList.one(s"Can't decode referers: ${e.getMessage}"))
                  }
    } yield RefererParserConf(schemaKey, file(source, conf._2, localFile, localMode), conf._3, referers)).toValidated

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

  def create[F[_]: Sync](
    filePath: String,
    internalDomains: List[String],
    referers: ParsedReferers
  ): EitherT[F, String, RefererParserEnrichment] =
    EitherT(CreateParser[F].create(filePath, referers))
      .leftMap(_.getMessage)
      .map(p => RefererParserEnrichment(p, internalDomains))
}

/**
 * Config for a referer_parser enrichment
 * @param parser Referer parser
 * @param domains List of internal domains
 */
final case class RefererParserEnrichment(parser: Parser, domains: List[String]) {

  /**
   * Extract details about the referer (sic). Uses the referer-parser library.
   * @param uri The referer URI to extract referer details from
   * @param pageHost The host of the current page (used to determine if this is an internal referer)
   * @return a Tuple3 containing referer medium, source and term, all Strings
   */
  def extractRefererDetails(uri: URI, pageHost: String): Option[Referer] =
    parser.parse(uri, Option(pageHost), domains).map {
      case ExternalReferer(m, s, t) =>
        val fixedTerm = t.flatMap(CU.fixTabsNewlines)
        ExternalReferer(m, s, fixedTerm)
      case other => other
    }

  /**
   * Classify an injected `utm_source` query parameter value against the referer database's
   * `utm_sources` entries. This is independent of the Referer header parsing done by
   * [[extractRefererDetails]] - some sites inject `utm_source` instead of (or in addition to)
   * setting a Referer header.
   * @param utmSource The value of the `utm_source` query parameter
   * @return the matched referer details, or None if the value is not recognised
   */
  def extractUtmReferrer(utmSource: String): Option[ExternalReferer] =
    parser.parseUtmSource(utmSource)
}
