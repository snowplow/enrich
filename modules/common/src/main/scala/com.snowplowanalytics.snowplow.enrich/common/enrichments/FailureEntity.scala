/*
 * Copyright (c) 2024-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import java.time.Instant

import cats.syntax.option._

import io.circe.{Encoder, Json}
import io.circe.generic.semiauto._
import io.circe.syntax._

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.client.validator.ValidatorError

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits.schemaKeyCirceJsonEncoder

/**
 * Represents a failure encountered during enrichment of the event.
 * Failure entities will be attached to incomplete events as derived contexts.
 */
case class FailureEntity(
  failureType: String,
  errors: List[Json],
  schema: Option[SchemaKey],
  data: Option[String],
  timestamp: Instant,
  componentName: String,
  componentVersion: String
)

object FailureEntity {

  val failureEntitySchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "failure", "jsonschema", SchemaVer.Full(1, 0, 0))

  implicit val failureEntityEncoder: Encoder[FailureEntity] = deriveEncoder[FailureEntity]

  case class BadRowWithFailureEntities(
    badRow: BadRow,
    failureEntities: List[FailureEntity]
  )

  /**
   * Wrapper for schema violation failure that stores extra information about the failure.
   * These extra information will be used while creating the failure entities that will be
   * attached to incomplete events.
   */
  case class SchemaViolationWithExtraContext(
    schemaViolation: FailureDetails.SchemaViolation,
    source: String,
    data: Option[String]
  )

  def toSDJ(failure: FailureEntity): SelfDescribingData[Json] =
    SelfDescribingData(
      failureEntitySchemaKey,
      failure.asJson
    )

  def fromEnrichmentFailure(
    ef: FailureDetails.EnrichmentFailure,
    timestamp: Instant,
    processor: Processor
  ): Option[FailureEntity] = {
    val failureType = s"EnrichmentError: ${ef.enrichment.map(_.identifier).getOrElse("")}"
    ef.message match {
      case m: FailureDetails.EnrichmentFailureMessage.InputData =>
        FailureEntity(
          failureType = failureType,
          errors = List(
            Json.obj(
              "message" := s"${m.field} - ${m.expectation}",
              "source" := m.field
            )
          ),
          schema = ef.enrichment.map(_.schemaKey),
          data = s"""{"${m.field}" : "${m.value.getOrElse("")}"}""".some,
          timestamp = timestamp,
          componentName = processor.artifact,
          componentVersion = processor.version
        ).some
      case m: FailureDetails.EnrichmentFailureMessage.Simple =>
        FailureEntity(
          failureType = failureType,
          errors = List(
            Json.obj(
              "message" := m.error
            )
          ),
          schema = ef.enrichment.map(_.schemaKey),
          data = None,
          timestamp = timestamp,
          componentName = processor.artifact,
          componentVersion = processor.version
        ).some
      case _: FailureDetails.EnrichmentFailureMessage.IgluError =>
        // EnrichmentFailureMessage.IgluError isn't used anywhere in the project
        // therefore we don't expect it in here
        None
    }
  }

  def fromSchemaViolation(
    v: SchemaViolationWithExtraContext,
    timestamp: Instant,
    processor: Processor
  ): FailureEntity =
    v.schemaViolation match {
      case f: FailureDetails.SchemaViolation.NotJson =>
        FailureEntity(
          failureType = "NotJSON",
          errors = List(Json.obj("message" := f.error.asJson, "source" := v.source.asJson)),
          schema = None,
          data = v.data,
          timestamp = timestamp,
          componentName = processor.artifact,
          componentVersion = processor.version
        )
      case f: FailureDetails.SchemaViolation.NotIglu =>
        val message = f.error.message("").split(":").headOption
        FailureEntity(
          failureType = "NotIglu",
          errors = List(Json.obj("message" := message.asJson, "source" := v.source.asJson)),
          schema = None,
          data = v.data,
          timestamp = timestamp,
          componentName = processor.artifact,
          componentVersion = processor.version
        )
      case f: FailureDetails.SchemaViolation.CriterionMismatch =>
        val message = s"Unexpected schema: ${f.schemaKey.toSchemaUri} does not match the criterion"
        FailureEntity(
          failureType = "CriterionMismatch",
          errors = List(
            Json.obj(
              "message" := message,
              "source" := v.source,
              "criterion" := f.schemaCriterion.asString
            )
          ),
          schema = f.schemaKey.some,
          data = v.data,
          timestamp = timestamp,
          componentName = processor.artifact,
          componentVersion = processor.version
        )
      case FailureDetails.SchemaViolation.IgluError(schemaKey, ClientError.ResolutionError(lh)) =>
        val message = s"Resolution error: schema ${schemaKey.toSchemaUri} not found"
        val lookupHistory = lh.toList
          .map {
            case (repo, lookups) =>
              lookups.asJson.deepMerge(Json.obj("repository" := repo.asJson))
          }
        FailureEntity(
          failureType = "ResolutionError",
          errors = List(
            Json.obj(
              "message" := message,
              "source" := v.source,
              "lookupHistory" := lookupHistory
            )
          ),
          schema = schemaKey.some,
          data = v.data,
          timestamp = timestamp,
          componentName = processor.artifact,
          componentVersion = processor.version
        )
      case FailureDetails.SchemaViolation.IgluError(schemaKey, ClientError.ValidationError(ValidatorError.InvalidData(e), _)) =>
        val errors = e.toList.map { r =>
          Json.obj(
            "message" := r.message,
            "source" := v.source,
            "path" := r.path,
            "keyword" := r.keyword,
            "targets" := r.targets
          )
        }
        FailureEntity(
          failureType = "ValidationError",
          errors = errors,
          schema = schemaKey.some,
          data = v.data,
          timestamp = timestamp,
          componentName = processor.artifact,
          componentVersion = processor.version
        )
      case FailureDetails.SchemaViolation.IgluError(schemaKey, ClientError.ValidationError(ValidatorError.InvalidSchema(e), _)) =>
        val errors = e.toList.map { r =>
          Json.obj(
            "message" := s"Invalid schema: $schemaKey - ${r.message}",
            "source" := v.source,
            "path" := r.path
          )
        }
        FailureEntity(
          failureType = "ValidationError",
          errors = errors,
          schema = schemaKey.some,
          data = v.data,
          timestamp = timestamp,
          componentName = processor.artifact,
          componentVersion = processor.version
        )
    }
}
