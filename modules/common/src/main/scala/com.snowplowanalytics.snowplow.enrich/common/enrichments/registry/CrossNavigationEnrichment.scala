/**
 * Copyright (c) 2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import java.time.format.DateTimeFormatter

import cats.data.ValidatedNel
import cats.syntax.either._
import cats.syntax.option._

import io.circe.Json
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.badrows.FailureDetails
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{EventEnrichments => EE}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.CrossNavigationConf
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils => CU}

/**
 * Companion object to create an instance of CrossNavigationEnrichment
 * from the configuration.
 */
object CrossNavigationEnrichment extends ParseableEnrichment {
  val supportedSchema = SchemaCriterion(
    "com.snowplowanalytics.snowplow.enrichments",
    "cross_navigation_config",
    "jsonschema",
    1,
    0
  )

  val outputSchema = SchemaKey(
    "com.snowplowanalytics.snowplow",
    "cross_navigation",
    "jsonschema",
    SchemaVer.Full(1, 0, 0)
  )

  val CrossNavProps = List(
    "domain_user_id",
    "timestamp",
    "session_id",
    "user_id",
    "source_id",
    "source_platform",
    "reason"
  )

  /**
   * Creates a CrossNavigationConf instance from a Json.
   * @param config The cross_navigation_config enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return a CrossNavigation configuration
   */
  override def parse(
    config: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, CrossNavigationConf] =
    (for {
      _ <- isParseable(config, schemaKey)
    } yield CrossNavigationConf(schemaKey)).toValidatedNel

  /**
   * Wrapper around CU.decodeBase64Url.
   * If passed an empty string returns Right(None).
   * @param str The string to decode
   * @return either the decoded string or enrichment failure
   */
  private def decodeWithFailure(str: String): Either[FailureDetails.EnrichmentFailure, Option[String]] =
    CU.decodeBase64Url(str) match {
      case Right(r) => Option(r).filter(_.trim.nonEmpty).asRight
      case Left(msg) =>
        FailureDetails
          .EnrichmentFailure(
            None,
            FailureDetails.EnrichmentFailureMessage.Simple(msg)
          )
          .asLeft
    }

  /**
   * Wrapper around EE.extractTimestamp
   * If passed an empty string returns Right(None).
   * @param str The string to extract the timestamp from
   * @return either the extracted timestamp or enrichment failure
   */
  private def extractTstamp(str: String): Either[FailureDetails.EnrichmentFailure, Option[String]] =
    str match {
      case "" => None.asRight
      case s => EE.extractTimestamp("sp_dtm", s).map(_.some)
    }

  /**
   * Converts a timestamp to an ISO-8601 format
   *
   * @param tstamp The timestamp expected as output of EE.extractTimestamp
   * @return ISO-8601 timestamp
   */
  private def reformatTstamp(tstamp: Option[String]): Option[String] = {
    val pFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    val formatter = DateTimeFormatter.ISO_DATE_TIME

    tstamp match {
      case Some(t) =>
        Some(formatter.format(pFormatter.parse(t)).replaceAll(" ", "T") + "Z")
      case None => None
    }
  }

  /**
   * Finalizes the cross navigation map by reformatting its timestamp key
   *
   * @param inputMap A Map of cross navigation properties
   * @return The finalized Map
   */
  private def finalizeCrossNavigationMap(inputMap: Map[String, Option[String]]): Map[String, Option[String]] =
    inputMap
      .map({
        case ("timestamp", t) => ("timestamp" -> reformatTstamp(t))
        case kvPair => kvPair
      })

  /**
   * Parses the QueryString into a Map
   * @param sp QueryString
   * @return either a map of query string parameters or enrichment failure
   */
  def makeCrossDomainMap(sp: String): Either[FailureDetails.EnrichmentFailure, Map[String, Option[String]]] =
    sp.split("\\.", -1)
      .padTo(
        CrossNavProps.size,
        ""
      )
      .toList match {
      case List(
            duidElt,
            tstampElt,
            sessionIdElt,
            userIdElt,
            sourceIdElt,
            sourcePlatformElt,
            reasonElt
          ) =>
        for {
          domainUserId <- CU.makeTsvSafe(duidElt).some.asRight
          timestamp <- extractTstamp(tstampElt)
          sessionId <- Option(sessionIdElt).filter(_.trim.nonEmpty).asRight
          userId <- decodeWithFailure(userIdElt)
          sourceId <- decodeWithFailure(sourceIdElt)
          sourcePlatform <- Option(sourcePlatformElt).filter(_.trim.nonEmpty).asRight
          reason <- decodeWithFailure(reasonElt)
        } yield (CrossNavProps zip List(
          domainUserId,
          timestamp,
          sessionId,
          userId,
          sourceId,
          sourcePlatform,
          reason
        )).filterNot(t => t._1 != "timestamp" && t._2 == None).toMap
      case _ => Map.empty[String, Option[String]].asRight
    }
}

/**
 * Enrichment adding cross navigation context
 */
final case class CrossNavigationEnrichment(schemaKey: SchemaKey) extends Enrichment {
  private val enrichmentInfo =
    FailureDetails.EnrichmentInformation(schemaKey, "cross-navigation").some

  /**
   * Gets the cross navigation parameters as self-describing JSON.
   * @param cnMap The map of cross navigation data
   * @return the cross navigation context wrapped in a List
   */
  def getCrossNavigationContext(cnMap: Map[String, Option[String]]): List[SelfDescribingData[Json]] =
    cnMap match {
      case m: Map[String, Option[String]] if m.isEmpty => Nil
      case _ =>
        List(
          SelfDescribingData(
            CrossNavigationEnrichment.outputSchema,
            CrossNavigationEnrichment.finalizeCrossNavigationMap(cnMap).asJson
          )
        )
    }

  /**
   * Given an EnrichmentFailure, returns one with the cross-navigation
   *  enrichment information added.
   * @param failure The input enrichment failure
   * @return the EnrichmentFailure with cross-navigation enrichment information
   */
  def addEnrichmentInfo(failure: FailureDetails.EnrichmentFailure): FailureDetails.EnrichmentFailure =
    failure match {
      case FailureDetails.EnrichmentFailure(_, msg) => FailureDetails.EnrichmentFailure(enrichmentInfo, msg)
      case _ => failure
    }

}
