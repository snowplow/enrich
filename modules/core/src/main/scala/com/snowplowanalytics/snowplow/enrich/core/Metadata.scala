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
  private val eventSpecValidationFailureName = "event_specification_validation"

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
    scenarioId: Option[String],
    scenarioVersion: Option[Int],
    eventSpecInferred: Boolean,
    eventSpecValid: Boolean
  )

  /**
   * For a batch of successfully enriched events, extract unique combinations of metadata
   */
  def extractsForBatch(batch: List[EnrichedEvent]): Aggregates =
    batch.map(extractMetadataEvent).combineAll

  implicit def entitiesAndCountSemigroup: Semigroup[EntitiesAndCount] =
    new Semigroup[EntitiesAndCount] {
      override def combine(x: EntitiesAndCount, y: EntitiesAndCount): EntitiesAndCount =
        EntitiesAndCount(
          x.entities |+| y.entities,
          x.count + y.count
        )
    }

  private def extractMetadataEvent(event: EnrichedEvent): Aggregates = {
    val entities = schemaKeysFromEntities(event.contexts) ++ schemaKeysFromEntities(event.derived_contexts)
    val schema = event.unstruct_event.map(_.schema)
    val source = Option(event.app_id)
    val tracker = Option(event.v_tracker)
    val platform = Option(event.platform)

    val statedEventSpec = extractStatedScenarioId(event)
    val entitiesAndCount = EntitiesAndCount(entities, 1)

    lazy val inferredEventSpecs = extractInferredScenarioIds(event)
    lazy val plainRow =
      MetadataEvent(
        schema = schema,
        source = source,
        tracker = tracker,
        platform = platform,
        scenarioId = None,
        scenarioVersion = None,
        eventSpecInferred = false,
        eventSpecValid = true
      ) -> entitiesAndCount

    statedEventSpec match {
      case Some((id, version)) =>
        val eventSpecValid = extractValidityStatus(event)
        Map(
          MetadataEvent(
            schema = schema,
            source = source,
            tracker = tracker,
            platform = platform,
            scenarioId = Some(id),
            scenarioVersion = version,
            eventSpecInferred = false,
            eventSpecValid = eventSpecValid
          ) -> entitiesAndCount
        )
      case None if inferredEventSpecs.nonEmpty =>
        // plainRow is needed to calculate the total number of events incoming, without duplication of the inference
        // consumers can calculate total by filtering data with eventSpecInferred = false
        (plainRow :: inferredEventSpecs.map {
          case (id, version) =>
            MetadataEvent(schema, source, tracker, platform, Some(id), version, eventSpecInferred = true, eventSpecValid = true) ->
              entitiesAndCount
        }).toMap
      case _ => Map(plainRow)
    }
  }

  private def schemaKeysFromEntities(sdjs: List[SelfDescribingData[Json]]): Set[SchemaKey] =
    sdjs.map(_.schema).toSet

  private def extractStatedScenarioId(event: EnrichedEvent): Option[(String, Option[Int])] =
    event.contexts.collectFirst {
      case sdj if sdj.schema.vendor === eventSpecSchemaVendor && sdj.schema.name === eventSpecSchemaName =>
        parseScenarioIdentifiers(sdj)
    }.flatten

  private def extractInferredScenarioIds(event: EnrichedEvent): List[(String, Option[Int])] =
    event.derived_contexts.collect {
      case sdj if sdj.schema.vendor === eventSpecSchemaVendor && sdj.schema.name === eventSpecSchemaName =>
        parseScenarioIdentifiers(sdj)
    }.flatten

  private def extractValidityStatus(event: EnrichedEvent): Boolean =
    // currently we are only sending the context when the event was not valid
    // if we change it in the future, this function should be changed to return Option[Boolean]
    // that should be parsed from the body of the context, instead of relying on it's existence
    !event.derived_contexts.exists(sdj => sdj.schema.vendor === eventSpecSchemaVendor && sdj.schema.name === eventSpecValidationFailureName)

  private def parseScenarioIdentifiers(sdj: SelfDescribingData[Json]): Option[(String, Option[Int])] =
    (for {
      id <- sdj.data.hcursor.downField("id").as[String]
      version <- sdj.data.hcursor.downField("version").as[Option[Int]]
    } yield (id, version)).toOption

}
