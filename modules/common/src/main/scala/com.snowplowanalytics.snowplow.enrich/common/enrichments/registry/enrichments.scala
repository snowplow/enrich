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

import cats.data.ValidatedNel
import cats.syntax.either._

import io.circe._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}

import com.snowplowanalytics.snowplow.enrich.common.utils.ConversionUtils

/** Trait inherited by every enrichment config case class */
trait Enrichment

/** Trait to hold helpers relating to enrichment config */
trait ParseableEnrichment {

  /** The schemas supported by this enrichment */
  def supportedSchema: SchemaCriterion

  /**
   * Tentatively parses an enrichment configuration and sends back the files that need to be cached
   * prior to the EnrichmentRegistry construction.
   * @param config Json configuration for the enrichment (not self-describing)
   * @param schemaKey Version of the schema we want to run
   * @param localMode whether to have an enrichment conf which will produce an enrichment running locally,
   * used for testing
   * @return the configuration for this enrichment as well as the list of files it needs cached
   */
  def parse(
    config: Json,
    schemaKey: SchemaKey,
    localMode: Boolean
  ): ValidatedNel[String, EnrichmentConf]

  /**
   * Tests whether a JSON is parseable by a specific EnrichmentConfig constructor
   * @param config The JSON
   * @param schemaKey The schemaKey which needs to be checked
   * @return The JSON or an error message, boxed
   */
  def isParseable(config: Json, schemaKey: SchemaKey): Either[String, Json] =
    if (supportedSchema.matches(schemaKey))
      config.asRight
    else
      (s"Schema key ${schemaKey.toSchemaUri} is not supported. A '${supportedSchema.name}' " +
        s"enrichment must have schema ${supportedSchema.asString}.").asLeft

  /**
   * Convert the path to a file from a String to a URI.
   * @param uri URI to a database file
   * @param database Name of the database
   * @return an Either-boxed URI
   */
  protected def getDatabaseUri(uri: String, database: String): Either[String, URI] =
    ConversionUtils
      .stringToUri(uri + (if (uri.endsWith("/")) "" else "/") + database)
      .flatMap {
        case Some(u) => u.asRight
        case None => "URI to IAB file must be provided".asLeft
      }
}
