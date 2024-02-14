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
sealed trait Failure {
  def toSDJ(timestamp: Instant, processor: Processor): SelfDescribingData[Json]
}

object Failure {

  val failureSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "failure", "jsonschema", SchemaVer.Full(1, 0, 0))

  case class SchemaViolation(
    schemaViolation: FailureDetails.SchemaViolation,
    source: String,
    data: Json
  ) extends Failure {
    def toSDJ(timestamp: Instant, processor: Processor): SelfDescribingData[Json] = {
      val feJson = fromSchemaViolation(this, timestamp, processor)
      SelfDescribingData(failureSchemaKey, feJson.asJson)
    }

  }

  case class EnrichmentFailure(
    enrichmentFailure: FailureDetails.EnrichmentFailure
  ) extends Failure {
    def toSDJ(timestamp: Instant, processor: Processor): SelfDescribingData[Json] = {
      val feJson = fromEnrichmentFailure(this, timestamp, processor)
      SelfDescribingData(failureSchemaKey, feJson.asJson)
    }
  }

  case class FailureContext(
    failureType: String,
    errors: List[Json],
    schema: Option[SchemaKey],
    data: Option[Json],
    timestamp: Instant,
    componentName: String,
    componentVersion: String
  )

  object FailureContext {
    implicit val failureContextEncoder: Encoder[FailureContext] = deriveEncoder[FailureContext]
  }

  def fromEnrichmentFailure(
    ef: EnrichmentFailure,
    timestamp: Instant,
    processor: Processor
  ): FailureContext = {
    val failureType = s"EnrichmentError: ${ef.enrichmentFailure.enrichment.map(_.identifier).getOrElse("")}"
    val schemaKey = ef.enrichmentFailure.enrichment.map(_.schemaKey)
    val (errors, data) = ef.enrichmentFailure.message match {
      case FailureDetails.EnrichmentFailureMessage.InputData(field, value, expectation) =>
        (
          List(
            Json.obj(
              "message" := s"$field - $expectation",
              "source" := field
            )
          ),
          Json.obj(field := value).some
        )
      case FailureDetails.EnrichmentFailureMessage.Simple(error) =>
        (
          List(
            Json.obj(
              "message" := error
            )
          ),
          None
        )
      case FailureDetails.EnrichmentFailureMessage.IgluError(_, error) =>
        // EnrichmentFailureMessage.IgluError isn't used anywhere in the project.
        // We are return this value for completeness.
        (
          List(
            Json.obj(
              "message" := error
            )
          ),
          None
        )
    }
    FailureContext(
      failureType = failureType,
      errors = errors,
      schema = schemaKey,
      data = data,
      timestamp = timestamp,
      componentName = processor.artifact,
      componentVersion = processor.version
    )
  }

  def fromSchemaViolation(
    v: SchemaViolation,
    timestamp: Instant,
    processor: Processor
  ): FailureContext = {
    val (failureType, errors, schema, data) = v.schemaViolation match {
      case FailureDetails.SchemaViolation.NotJson(_, _, err) =>
        val error = Json.obj("message" := err, "source" := v.source)
        ("NotJSON", List(error), None, Json.obj(v.source := v.data).some)
      case FailureDetails.SchemaViolation.NotIglu(_, err) =>
        val message = err.message("").split(":").headOption
        val error = Json.obj("message" := message, "source" := v.source)
        ("NotIglu", List(error), None, v.data.some)
      case FailureDetails.SchemaViolation.CriterionMismatch(schemaKey, schemaCriterion) =>
        val message = s"Unexpected schema: ${schemaKey.toSchemaUri} does not match the criterion"
        val error = Json.obj(
          "message" := message,
          "source" := v.source,
          "criterion" := schemaCriterion.asString
        )
        ("CriterionMismatch", List(error), schemaKey.some, v.data.some)
      case FailureDetails.SchemaViolation.IgluError(schemaKey, ClientError.ResolutionError(lh)) =>
        val message = s"Resolution error: schema ${schemaKey.toSchemaUri} not found"
        val lookupHistory = lh.toList
          .map {
            case (repo, lookups) =>
              lookups.asJson.deepMerge(Json.obj("repository" := repo.asJson))
          }
        val error = Json.obj(
          "message" := message,
          "source" := v.source,
          "lookupHistory" := lookupHistory
        )
        ("ResolutionError", List(error), schemaKey.some, v.data.some)
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
        ("ValidationError", errors, schemaKey.some, v.data.some)
      case FailureDetails.SchemaViolation.IgluError(schemaKey, ClientError.ValidationError(ValidatorError.InvalidSchema(e), _)) =>
        val errors = e.toList.map { r =>
          Json.obj(
            "message" := s"Invalid schema: ${schemaKey.toSchemaUri} - ${r.message}",
            "source" := v.source,
            "path" := r.path
          )
        }
        ("ValidationError", errors, schemaKey.some, v.data.some)
    }
    FailureContext(
      failureType = failureType,
      errors = errors,
      schema = schema,
      data = data,
      timestamp = timestamp,
      componentName = processor.artifact,
      componentVersion = processor.version
    )
  }
}
