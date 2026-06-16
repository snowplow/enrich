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

import cats.data.{EitherT, NonEmptyList, Validated, ValidatedNel}
import cats.implicits._
import cats.effect.{Resource, Sync}

import io.circe._
import io.circe.generic.semiauto._
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.AgentClassificationConf
import com.snowplowanalytics.snowplow.enrich.common.utils.CirceUtils

object AgentClassificationEnrichment extends ParseableEnrichment {

  override val supportedSchema: SchemaCriterion =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "agent_classification_enrichment_config",
      "jsonschema",
      1,
      0
    )

  val outputSchema: SchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow", "agent_classification", "jsonschema", SchemaVer.Full(1, 0, 0))

  /**
   * A fully-resolved classification record, as found in the classification file or produced by
   * merging in the user overrides. `operator` is required to emit the entity.
   */
  final case class AgentClassification(operator: String, purpose: Option[String])
  object AgentClassification {
    implicit val decoder: Decoder[AgentClassification] = deriveDecoder
  }

  /**
   * A user override. Either `operator` or `purpose` must be present; the supplied fields override
   * the corresponding values of the file-based record for the same agent.
   */
  final case class AgentClassificationOverride(operator: Option[String], purpose: Option[String])
  object AgentClassificationOverride {
    implicit val decoder: Decoder[AgentClassificationOverride] = deriveDecoder
  }

  /**
   * Creates an AgentClassificationConf from a Json.
   * @param config The agent_classification enrichment JSON
   * @param schemaKey provided by `EnrichmentRegistry`, must be supported by this enrichment
   * @param localMode whether to resolve the classification file locally, enabled for tests
   */
  override def parse(
    config: Json,
    schemaKey: SchemaKey,
    localMode: Boolean
  ): ValidatedNel[String, AgentClassificationConf] =
    isParseable(config, schemaKey)
      .leftMap(NonEmptyList.one)
      .flatMap { _ =>
        (
          getClassificationFile(config).sequence,
          getOverrides(config)
        ).mapN { (classificationFile, overrides) =>
          AgentClassificationConf(schemaKey, file(classificationFile, localMode), overrides)
        }.toEither
      }
      .toValidated

  private def getClassificationFile(config: Json): Option[ValidatedNel[String, (URI, String)]] =
    if (config.hcursor.downField("parameters").downField("classificationFile").focus.isDefined) {
      val uri = CirceUtils.extract[String](config, "parameters", "classificationFile", "uri")
      val db = CirceUtils.extract[String](config, "parameters", "classificationFile", "database")
      (for {
        uriAndDb <- (uri.toValidatedNel, db.toValidatedNel).mapN((_, _)).toEither
        uri <- getDatabaseUri(uriAndDb._1, uriAndDb._2).leftMap(NonEmptyList.one)
      } yield (uri, uriAndDb._2)).toValidated.some
    } else None

  private def getOverrides(config: Json): ValidatedNel[String, Map[String, AgentClassificationOverride]] =
    CirceUtils
      .extract[Option[Map[String, AgentClassificationOverride]]](config, "parameters", "classification")
      .toValidatedNel
      .andThen { (opt: Option[Map[String, AgentClassificationOverride]]) =>
        val overrides = opt.getOrElse(Map.empty[String, AgentClassificationOverride])
        val errors = overrides.toList.collect {
          case (agent, o) if o.operator.isEmpty && o.purpose.isEmpty =>
            s"Override for agent '$agent' must specify at least 'operator' or 'purpose'"
        }
        NonEmptyList.fromList(errors) match {
          case None => Validated.Valid(overrides): ValidatedNel[String, Map[String, AgentClassificationOverride]]
          case Some(nel) => Validated.Invalid(nel)
        }
      }

  private def file(db: Option[(URI, String)], localMode: Boolean): Option[(URI, String)] =
    db.map {
      case (uri, database) =>
        if (localMode)
          (uri, Option(getClass.getResource(database)).getOrElse(getClass.getResource("/" + database)).toURI.getPath)
        else
          (uri, s"./agent_classification_$database")
    }

  def create[F[_]: Sync](
    classificationFile: Option[String],
    overrides: Map[String, AgentClassificationOverride]
  ): EitherT[F, String, AgentClassificationEnrichment] =
    (classificationFile match {
      case Some(path) => parseClassificationFile[F](path)
      case None => EitherT.rightT[F, String](Map.empty[String, AgentClassification])
    }).map(fromFile => AgentClassificationEnrichment(merge(fromFile, overrides)))

  private def parseClassificationFile[F[_]: Sync](path: String): EitherT[F, String, Map[String, AgentClassification]] =
    EitherT {
      Resource
        .fromAutoCloseable(Sync[F].blocking(Source.fromFile(path)))
        .use(source => Sync[F].blocking(source.mkString))
        .attempt
    }.leftMap(e => s"Failed to read agent classification file: ${e.getMessage}")
      .subflatMap { content =>
        parser
          .decode[Map[String, AgentClassification]](content)
          .leftMap(e => s"Failed to parse agent classification file: ${e.getMessage}")
      }

  /**
   * Merge the file-based classification with the user overrides. For each agent present in the
   * overrides, the supplied `operator`/`purpose` take precedence over the file value. Agents that
   * end up without an `operator` are dropped, since the entity cannot be emitted without one.
   */
  private[registry] def merge(
    fromFile: Map[String, AgentClassification],
    overrides: Map[String, AgentClassificationOverride]
  ): Map[String, AgentClassification] =
    overrides.foldLeft(fromFile) {
      case (acc, (agent, o)) =>
        val base = acc.get(agent)
        val operator = o.operator.orElse(base.map(_.operator))
        val purpose = o.purpose.orElse(base.flatMap(_.purpose))
        operator match {
          case Some(op) => acc + (agent -> AgentClassification(op, purpose))
          case None => acc
        }
    }
}

final case class AgentClassificationEnrichment(
  classifications: Map[String, AgentClassificationEnrichment.AgentClassification]
) {
  import AgentClassificationEnrichment._

  /**
   * Looks at the `agentName` parsed by YAUAA and, if it matches a known agent, builds an
   * `agent_classification` entity describing the operator and (optional) purpose.
   */
  def getAgentClassificationContext(yauaaContext: SelfDescribingData[Json]): List[SelfDescribingData[Json]] =
    if (classifications.isEmpty) Nil
    else
      (for {
        agentName <- yauaaContext.data.hcursor.get[String]("agentName").toOption
        classification <- classifications.get(agentName)
      } yield {
        // `purpose` is omitted (not set to null) when unknown, consistent with asn_lookups
        val fields = ("operator" := classification.operator) :: classification.purpose.map("purpose" := _).toList
        SelfDescribingData(outputSchema, Json.obj(fields: _*))
      }).toList
}
