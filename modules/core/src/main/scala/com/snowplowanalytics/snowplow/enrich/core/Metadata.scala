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
package com.snowplowanalytics.snowplow.enrich.core

import cats.Semigroup
import cats.implicits._
import io.circe.Json

import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

object Metadata {

  private val eventSpecSchemaVendor = "com.snowplowanalytics.snowplow"
  private val eventSpecSchemaName = "event_specification"

  /** The unique combinations that get reported to the metadata collector */
  type Aggregates = Map[MetadataEvent, EntitiesAndCount]

  case class EntitiesAndCount(entities: Set[SchemaKey], count: Int)

  /**
   * A metadata domain representation of an enriched event
   *
   * @param schema - schema key of given event
   * @param source - `app_id` for given event
   * @param tracker - `v_tracker` for given event
   * @param platform - The platform the app runs on for given event (`platform` field)
   * @param scenarioId - Identifier for the tracking scenario the event is being tracked for
   */
  case class MetadataEvent(
    schema: Option[SchemaKey],
    source: Option[String],
    tracker: Option[String],
    platform: Option[String],
    scenarioId: Option[String]
  )

  /**
   * For a batch of successfully enriched events, extract unique combinations of metadata
   */
  def extractsForBatch(batch: List[EnrichedEvent]): Aggregates =
    batch.map { event =>
      val entities = schemaKeysFromEntities(event.contexts) ++ schemaKeysFromEntities(event.derived_contexts)
      val key = extractMetadataEvent(event)
      Map(key -> EntitiesAndCount(entities, 1))
    }.combineAll

  implicit def entitiesAndCountSemigroup: Semigroup[EntitiesAndCount] =
    new Semigroup[EntitiesAndCount] {
      override def combine(x: EntitiesAndCount, y: EntitiesAndCount): EntitiesAndCount =
        EntitiesAndCount(
          x.entities |+| y.entities,
          x.count + y.count
        )
    }

  private def extractMetadataEvent(event: EnrichedEvent): MetadataEvent =
    MetadataEvent(
      schema = event.unstruct_event.map(_.schema),
      source = Option(event.app_id),
      tracker = Option(event.v_tracker),
      platform = Option(event.platform),
      scenarioId = extractScenarioId(event)
    )

  private def schemaKeysFromEntities(sdjs: List[SelfDescribingData[Json]]): Set[SchemaKey] =
    sdjs.map(_.schema).toSet

  private def extractScenarioId(event: EnrichedEvent): Option[String] =
    (event.contexts.view ++ event.derived_contexts.view).collectFirst {
      case sdj if sdj.schema.vendor === eventSpecSchemaVendor && sdj.schema.name === eventSpecSchemaName =>
        sdj.data.hcursor.downField("id").as[String] match {
          case Right(scenarioId) =>
            Some(scenarioId)
          case _ =>
            None
        }
    }.flatten

}
