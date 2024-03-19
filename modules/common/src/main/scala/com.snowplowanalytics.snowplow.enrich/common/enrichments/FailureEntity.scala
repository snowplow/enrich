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

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

case class FailureEntity(
  failureType: String,
  errors: List[Json], // message and source
  schema: Option[String],
  data: Option[Json],
  timestamp: Instant,
  componentName: String,
  componentVersion: String
)

object FailureEntity {
  val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "failure", "jsonschema", SchemaVer.Full(1, 0, 0))

  implicit val instantEncoder: Encoder[Instant] = Encoder.encodeString.contramap[Instant](_.toString)
  implicit val encoder: Encoder[FailureEntity] = deriveEncoder[FailureEntity]

  def toSDJ(failure: FailureEntity): SelfDescribingData[Json] =
    SelfDescribingData(
      schemaKey,
      failure.asJson
    )

  def addDerivedContexts(enriched: EnrichedEvent, failures: List[FailureEntity]) =
    enriched.derived_contexts = MiscEnrichments.formatContexts(failures.map(toSDJ)).get
}
