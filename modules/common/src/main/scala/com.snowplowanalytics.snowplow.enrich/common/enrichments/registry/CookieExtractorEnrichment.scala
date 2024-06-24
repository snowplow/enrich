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

import cats.data.ValidatedNel
import cats.syntax.either._

import io.circe._
import io.circe.syntax._

import org.apache.http.message.BasicHeaderValueParser

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.CookieExtractorConf
import com.snowplowanalytics.snowplow.enrich.common.utils.CirceUtils

object CookieExtractorEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "cookie_extractor_config", "jsonschema", 1, 0)
  val outputSchema = SchemaKey("org.ietf", "http_cookie", "jsonschema", SchemaVer.Full(1, 0, 0))

  /**
   * Creates a CookieExtractorConf instance from a Json.
   * @param config The cookie_extractor enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return a CookieExtractor configuration
   */
  override def parse(
    config: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, CookieExtractorConf] =
    (for {
      _ <- isParseable(config, schemaKey)
      cookieNames <- CirceUtils.extract[List[String]](config, "parameters", "cookies").toEither
    } yield CookieExtractorConf(schemaKey, cookieNames)).toValidatedNel
}

/**
 * Enrichment extracting certain cookies from headers.
 * @param cookieNames Names of the cookies to be extracted
 */
final case class CookieExtractorEnrichment(cookieNames: List[String]) extends Enrichment {

  def extract(headers: List[String]): List[SelfDescribingData[Json]] = {
    // rfc6265 - sections 4.2.1 and 4.2.2
    val cookies = headers.flatMap { header =>
      header.split(":", 2) match {
        case Array(cookieStr, value) if cookieStr.toLowerCase == "cookie" =>
          val nameValuePairs =
            BasicHeaderValueParser.parseParameters(value, BasicHeaderValueParser.INSTANCE)

          val filtered = nameValuePairs.filter { nvp =>
            cookieNames.contains(nvp.getName)
          }

          Some(filtered)
        case _ => None
      }
    }.flatten

    cookies.map { cookie =>
      SelfDescribingData(
        CookieExtractorEnrichment.outputSchema,
        Json.obj("name" := stringToJson(cookie.getName), "value" := stringToJson(cookie.getValue))
      )
    }
  }

  private def stringToJson(str: String): Json =
    Option(str).map(Json.fromString).getOrElse(Json.Null)
}
