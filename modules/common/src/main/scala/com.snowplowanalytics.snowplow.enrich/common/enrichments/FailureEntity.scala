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

import io.circe.{Encoder, Json}
import io.circe.generic.semiauto._
import io.circe.syntax._

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

/**
 * Represents a failure encountered during enrichment of the event.
 * Failure entities will be attached to incomplete events as derived contexts.
 */
case class FailureEntity(
  failureType: String,
  errors: List[Json],
  schema: Option[String],
  data: Option[String],
  timestamp: Instant,
  componentName: String,
  componentVersion: String
)

object FailureEntity {

  val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "failure", "jsonschema", SchemaVer.Full(1, 0, 0))

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
      schemaKey,
      failure.asJson
    )

  implicit val instantEncoder: Encoder[Instant] = Encoder.encodeString.contramap[Instant](_.toString)
  implicit val encoder: Encoder[FailureEntity] = deriveEncoder[FailureEntity]

  def fromSchemaViolation(
    v: SchemaViolationWithExtraContext,
    timestamp: Instant,
    processor: Processor
  ): FailureEntity =
    v.schemaViolation match {
      case f: FailureDetails.SchemaViolation.NotJson =>
        FailureEntity(
          failureType = "NotJson",
          errors = List(Json.obj("message" -> f.error.asJson, "source" -> v.source.asJson)),
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
          errors = List(Json.obj("message" -> message.asJson, "source" -> v.source.asJson)),
          schema = None,
          data = v.data,
          timestamp = timestamp,
          componentName = processor.artifact,
          componentVersion = processor.version
        )
      // TODO: Implement remaining cases
      case _ => throw new Exception("")
    }
}
