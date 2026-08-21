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

import cats.data.{EitherT, Ior, NonEmptyList}
import cats.syntax.either._
import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.badrows.FailureDetails
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EventSpecEnrichment.{Entity, EventSpec}
import org.specs2.Specification
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import io.circe.Json
import io.circe.literal.JsonStringContext

class EventSpecEnrichmentSpec extends Specification with CatsEffect {

  val esId1 = "75604331-bff1-41bf-9adb-9b7ac10f4de8"
  val esName1 = "testEs1"
  val esId2 = "c6fcb515-24ec-4bdb-8911-50b37001fe58"
  val esName2 = "testEs2"
  val esId3 = "0e543161-19ad-4b93-a5ac-4eb202c44121"
  val esName3 = "testEs3"

  val maxJsonDepth = 10

  val eventSpecSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "event_specification", "jsonschema", SchemaVer.Full(1, 0, 4))
  val enrichmentConfigSchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow.enrichments", "event_spec_enrichment_config", "jsonschema", SchemaVer.Full(1, 0, 0))
  val pagePingSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "page_ping", "jsonschema", SchemaVer.Full(1, 0, 0))
  val pageViewSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "page_view", "jsonschema", SchemaVer.Full(1, 0, 0))

  def mkEventSpecSDJ(
    id: String,
    name: String,
    version: Option[Int] = None
  ) =
    SelfDescribingData(
      eventSpecSchemaKey,
      version.fold(
        json"""
        {
          "id" : $id,
          "name" : $name
        }
      """
      )(v => json"""
        {
          "id" : $id,
          "name" : $name,
          "version" : $v
        }
      """)
    )

  /** Extracts the contexts of a good-stream result, failing the test on Ior.Left/Ior.Both */
  def goodStreamContexts(
    result: Ior[NonEmptyList[FailureDetails.EnrichmentFailure], List[SelfDescribingData[Json]]]
  ): List[SelfDescribingData[Json]] =
    result match {
      case Ior.Right(contexts) => contexts
      case other => sys.error(s"Expected Ior.Right but got $other")
    }

  /** Helper to build an event spec */
  def mkSpec(
    id: String,
    name: String,
    schemaKey: SchemaKey,
    version: Option[Int] = None,
    constraint: Option[io.circe.Json] = None,
    entities: List[Entity] = List.empty,
    sendInvalidToBad: Option[Boolean] = None
  ): EventSpec =
    EventSpec(id, name, version, schemaKey, constraint, entities, sendInvalidToBad)

  /** Helper to build an entity JSON object */
  def mkEntity(
    schemaKey: SchemaKey,
    minCardinality: Option[Int] = None,
    maxCardinality: Option[Int] = None,
    constraint: Option[io.circe.Json] = None
  ): Entity =
    Entity(schemaKey, minCardinality, maxCardinality, constraint)

  val customEventKey = SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0))
  val customEntityKey = SchemaKey("com.snowplowanalytics.snowplow", "custom_entity", "jsonschema", SchemaVer.Full(1, 0, 0))
  val anotherEntityKey = SchemaKey("com.snowplowanalytics.snowplow", "another_entity", "jsonschema", SchemaVer.Full(1, 0, 0))

  val testConstraint = json"""{
    "type": "object",
    "schema": "http://json-schema.org/draft-04/schema#",
    "properties": {
        "page_url": {
            "enum": ["test.com"],
            "type": "string",
            "description": ""
        }
    },
    "additionalProperties": true
  }"""

  val wrongConstraint = json"""{
    "type": "object",
    "schema": "http://json-schema.org/draft-04/schema#",
    "properties": {
        "page_url": {
            "enum": ["wrong.com"],
            "type": "string",
            "description": ""
        }
    },
    "additionalProperties": true
  }"""

  val someValueConstraint = json"""{
    "type": "object",
    "schema": "http://json-schema.org/draft-04/schema#",
    "properties": {
        "some_field": {
            "enum": ["some_value"],
            "type": "string",
            "description": ""
        }
    },
    "additionalProperties": true
  }"""

  val wrongValueConstraint = json"""{
    "type": "object",
    "schema": "http://json-schema.org/draft-04/schema#",
    "properties": {
        "some_field": {
            "enum": ["wrong_value"],
            "type": "string",
            "description": ""
        }
    },
    "additionalProperties": true
  }"""

  val notMatchingEntityConstraint = json"""{
    "type": "object",
    "schema": "http://json-schema.org/draft-04/schema#",
    "properties": {
        "another_entity_field": {
            "enum": ["something_not_matching"],
            "type": "string",
            "description": ""
        }
    },
    "additionalProperties": true
  }"""

  def is =
    s2"""
  EventSpecEnrichment#inferEventSpec should return nothing on empty config+event $e1
  EventSpecEnrichment#inferEventSpec should return nothing on empty event $e2
  EventSpecEnrichment#inferEventSpec should return matched page_ping without rules $e3
  EventSpecEnrichment#inferEventSpec should return matched page_ping with rules $e4
  EventSpecEnrichment#inferEventSpec should return matched page_view without rules $e5
  EventSpecEnrichment#inferEventSpec should return matched unstruct_event without rules $e6
  EventSpecEnrichment#inferEventSpec should return matched unstruct_event with rules $e7
  EventSpecEnrichment#inferEventSpec should return empty based on entity set $e8
  EventSpecEnrichment#inferEventSpec should return matched based on entity set $e9
  EventSpecEnrichment#inferEventSpec should return matched multiple based on entity set $e10
  EventSpecEnrichment#inferEventSpec should return matched based on entity rules $e11
  EventSpecEnrichment#inferEventSpec should return skip inference if event spec context is already present $e12
  EventSpecEnrichment#inferEventSpec should return validate rules for optional context if it's present $e13
  EventSpecEnrichment#inferEventSpec should return pass for an optional entity that is not present $e14
  EventSpecEnrichment should load event specs from file $e15
  EventSpecEnrichment should use latest version for inference when multiple versions exist $e16
  EventSpecEnrichment should place versionless specs in inference tier $e17
  EventSpecEnrichment should handle mixed versioned and versionless specs for same schema $e18
  EventSpecEnrichment should find spec in validation cache by id and version $e19
  EventSpecEnrichment should promote spec from archive to validation cache on lookup $e20
  EventSpecEnrichment should return None for non-existent spec version $e21
  EventSpecEnrichment#inferEventSpec should match versioned specs with event constraints (<=3 versions) $e22
  EventSpecEnrichment#inferEventSpec should match versioned specs with event constraints (>3 versions) $e23
  EventSpecEnrichment#inferEventSpec should match versioned specs with entity constraints (<=3 versions) $e24
  EventSpecEnrichment#inferEventSpec should match versioned specs with entity constraints (>3 versions) $e25
  EventSpecEnrichment#processEvent should route to inference when no event_specification context $e34
  EventSpecEnrichment#processEvent should route to validation when event_specification context has version $e35
  EventSpecEnrichment#processEvent should skip when event_specification context has no version $e36
  EventSpecEnrichment#processEvent should return failure context when event constraint fails $e41
  EventSpecEnrichment#validateEventSpec should produce structured error for cardinality min $e44
  EventSpecEnrichment#validateEventSpec should produce structured error for cardinality max $e45
  EventSpecEnrichment#validateEventSpec should produce structured error for entity constraint $e46
  EventSpecEnrichment#validateEventSpec should aggregate event constraint and cardinality errors $e47
  EventSpecEnrichment#processEvent should return empty list for valid event $e48
  EventSpecEnrichment#processEvent should attach failure context for cardinality failure $e49
  EventSpecEnrichment#validateEventSpec should return Valid for spec in cache with no constraints $e26
  EventSpecEnrichment#validateEventSpec should return Valid for spec in cache passing constraint $e27
  EventSpecEnrichment#validateEventSpec should return Invalid when event constraint fails (cache) $e28
  EventSpecEnrichment#validateEventSpec should return Invalid when required entity missing (cache) $e29
  EventSpecEnrichment#validateEventSpec should return Invalid when entity constraint fails (cache) $e30
  EventSpecEnrichment#validateEventSpec should return SpecNotFound for non-existent version $e31
  EventSpecEnrichment#validateEventSpec should return Valid for spec promoted from archive $e32
  EventSpecEnrichment#validateEventSpec should return Invalid when event constraint fails (archive) $e33
  EventSpecEnrichment#validateEventSpec should emit failures and keep the context when sendInvalidToBad is set $e50
  EventSpecEnrichment#validateEventSpec should not emit failures for a valid event when sendInvalidToBad is set $e51
  EventSpecEnrichment#validateEventSpec should keep invalid events in the good stream when sendInvalidToBad is false $e52
  EventSpecEnrichment payload decoder should read sendInvalidToBad and default to None when absent $e53
  EventSpecEnrichment#validateEventSpec should route to bad when the flagged spec version comes from the archive $e54
  EventSpecEnrichment#validateEventSpec should emit one failure per validation error when sendInvalidToBad is set $e55
  """

  def e1 = {
    val event = new EnrichedEvent {
      app_id = "some-app-id"
    }
    EventSpecEnrichment.createFromSpecs[IO](List.empty, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(event, maxJsonDepth) must beEmpty
      }
    }
  }

  def e2 = {
    val specs = List(mkSpec(esId1, esName1, pagePingSchemaKey))
    val event = new EnrichedEvent {
      app_id = "some-app-id"
    }
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(event, maxJsonDepth) must beEmpty
      }
    }
  }

  def e3 = {
    val specs = List(mkSpec(esId1, esName1, pagePingSchemaKey))
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1))
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e4 = {
    val specs = List(
      mkSpec(esId1, esName1, pagePingSchemaKey),
      mkSpec(esId2, esName2, pagePingSchemaKey, constraint = Some(testConstraint)),
      mkSpec(esId3, esName3, pagePingSchemaKey, constraint = Some(wrongConstraint))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      page_url = "test.com"
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1), mkEventSpecSDJ(esId2, esName2))
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e5 = {
    val specs = List(mkSpec(esId1, esName1, pageViewSchemaKey))
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_view"
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1))
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e6 = {
    val specs = List(mkSpec(esId1, esName1, customEventKey))
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(
          customEventKey,
          json"""{"some_field": "some_value", "other_field": 123}"""
        )
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1))
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e7 = {
    val specs = List(
      mkSpec(esId1, esName1, customEventKey),
      mkSpec(esId2, esName2, customEventKey, constraint = Some(someValueConstraint)),
      mkSpec(esId3, esName3, customEventKey, constraint = Some(wrongValueConstraint))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(
          customEventKey,
          json"""{"some_field": "some_value", "other_field": 123}"""
        )
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1), mkEventSpecSDJ(esId2, esName2))
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e8 = {
    val requiredEntity1 = mkEntity(customEntityKey, minCardinality = Some(1))
    val requiredEntity2 = mkEntity(anotherEntityKey, minCardinality = Some(1))
    val specs = List(
      mkSpec(esId1, esName1, customEventKey, entities = List(requiredEntity1, requiredEntity2))
    )
    val eventWithSingleEntity = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(customEventKey, json"""{"some_field": "some_value", "other_field": 123}""")
      )
      contexts = List(
        SelfDescribingData(customEntityKey, json"""{"entity_field": "entity_value"}""")
      )
    }
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(eventWithSingleEntity, maxJsonDepth) must beEmpty
      }
    }
  }

  def e9 = {
    val requiredEntity1 = mkEntity(customEntityKey, minCardinality = Some(1))
    val requiredEntity2 = mkEntity(anotherEntityKey, minCardinality = Some(1))
    val specs = List(
      mkSpec(esId1, esName1, customEventKey, entities = List(requiredEntity1, requiredEntity2)),
      mkSpec(esId2, esName2, customEventKey, entities = List(requiredEntity1))
    )
    val eventWithSingleEntity = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(customEventKey, json"""{"some_field": "some_value", "other_field": 123}""")
      )
      contexts = List(
        SelfDescribingData(customEntityKey, json"""{"entity_field": "entity_value"}""")
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId2, esName2))
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(eventWithSingleEntity, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e10 = {
    val requiredEntity1 = mkEntity(customEntityKey, minCardinality = Some(1))
    val requiredEntity2 = mkEntity(anotherEntityKey, minCardinality = Some(1))
    val specs = List(
      mkSpec(esId1, esName1, customEventKey, entities = List(requiredEntity1, requiredEntity2)),
      mkSpec(esId2, esName2, customEventKey, entities = List(requiredEntity1))
    )
    val eventWithBothEntities = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(customEventKey, json"""{"some_field": "some_value", "other_field": 123}""")
      )
      contexts = List(
        SelfDescribingData(customEntityKey, json"""{"entity_field": "entity_value"}"""),
        SelfDescribingData(anotherEntityKey, json"""{"another_entity_field": "another_entity_value"}""")
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1), mkEventSpecSDJ(esId2, esName2))
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(eventWithBothEntities, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e11 = {
    val requiredEntity1 = mkEntity(customEntityKey, minCardinality = Some(1))
    val requiredEntity2 = mkEntity(anotherEntityKey, minCardinality = Some(1), constraint = Some(notMatchingEntityConstraint))
    val specs = List(
      mkSpec(esId1, esName1, customEventKey, entities = List(requiredEntity1, requiredEntity2)),
      mkSpec(esId2, esName2, customEventKey, entities = List(requiredEntity1))
    )
    val eventWithBothEntities = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(customEventKey, json"""{"some_field": "some_value", "other_field": 123}""")
      )
      contexts = List(
        SelfDescribingData(customEntityKey, json"""{"entity_field": "entity_value"}"""),
        SelfDescribingData(anotherEntityKey, json"""{"another_entity_field": "another_entity_value"}""")
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId2, esName2))
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(eventWithBothEntities, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e12 = {
    val specs = List(mkSpec(esId1, esName1, customEventKey))
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(customEventKey, json"""{"some_field": "some_value", "other_field": 123}""")
      )
      contexts = List(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "event_specification", "jsonschema", SchemaVer.Full(1, 0, 1)),
          json"""{"dummy": "dummy"}"""
        )
      )
    }
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(event, maxJsonDepth) must beEmpty
      }
    }
  }

  def e13 = {
    val requiredEntity1 = mkEntity(customEntityKey, minCardinality = Some(1))
    val optionalEntity2 = mkEntity(anotherEntityKey, constraint = Some(notMatchingEntityConstraint))
    val specs = List(
      mkSpec(esId1, esName1, customEventKey, entities = List(requiredEntity1, optionalEntity2)),
      mkSpec(esId2, esName2, customEventKey, entities = List(requiredEntity1))
    )
    val eventWithBothEntities = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(customEventKey, json"""{"some_field": "some_value", "other_field": 123}""")
      )
      contexts = List(
        SelfDescribingData(customEntityKey, json"""{"entity_field": "entity_value"}"""),
        SelfDescribingData(anotherEntityKey, json"""{"another_entity_field": "another_entity_value"}""")
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId2, esName2))
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(eventWithBothEntities, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e14 = {
    val requiredEntity1 = mkEntity(customEntityKey, minCardinality = Some(1))
    val optionalEntity2 = mkEntity(anotherEntityKey, constraint = Some(notMatchingEntityConstraint))
    val specs = List(
      mkSpec(esId1, esName1, customEventKey, entities = List(requiredEntity1, optionalEntity2)),
      mkSpec(esId2, esName2, customEventKey, entities = List(requiredEntity1))
    )
    val eventWithSingleEntity = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(customEventKey, json"""{"some_field": "some_value", "other_field": 123}""")
      )
      contexts = List(
        SelfDescribingData(customEntityKey, json"""{"entity_field": "entity_value"}""")
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1), mkEventSpecSDJ(esId2, esName2))
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(eventWithSingleEntity, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e15 = {
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
    }
    val expectedSpecs = List(mkEventSpecSDJ("test-spec-from-file", "Test Event Spec From File", Some(0)))

    (for {
      conf <- EitherT.fromEither[IO](
                EventSpecEnrichment
                  .parse(
                    json"""{
                      "name": "event_spec_enrichment_config",
                      "vendor": "com.snowplowanalytics.snowplow.enrichments",
                      "enabled": true,
                      "parameters": {
                        "uri": "http://snowplow.com",
                        "database": "event-specs-test.json"
                      }
                    }""",
                    SchemaKey(
                      "com.snowplowanalytics.snowplow.enrichments",
                      "event_spec_enrichment_config",
                      "jsonschema",
                      SchemaVer.Full(1, 0, 0)
                    ),
                    localMode = true
                  )
                  .toEither
                  .leftMap(_.head)
              )
      enrichment <- conf.enrichment[IO]
    } yield enrichment.inferEventSpec(event, maxJsonDepth)).value.map(_ must beRight(expectedSpecs))
  }

  def e16 = {
    val specs = (0 to 4).toList.map { v =>
      mkSpec(esId1, s"$esName1-v$v", pagePingSchemaKey, version = Some(v))
    }
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, s"$esName1-v4", Some(4)))

    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e17 = {
    val specs = List(
      mkSpec(esId1, esName1, pagePingSchemaKey),
      mkSpec(esId2, esName2, pageViewSchemaKey, version = Some(0))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1))

    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e18 = {
    val specs = List(
      mkSpec(esId1, esName1, pagePingSchemaKey),
      mkSpec(esId2, esName2, pagePingSchemaKey, version = Some(0))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1), mkEventSpecSDJ(esId2, esName2, Some(0)))

    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e19 = {
    val specs = (0 to 2).toList.map { v =>
      mkSpec(esId1, s"$esName1-v$v", pagePingSchemaKey, version = Some(v))
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      found <- enrichment.lookupInCache(esId1, 1)
    } yield found must beRight.like {
      case compiled => compiled.name must beEqualTo(s"$esName1-v1")
    }
  }

  def e20 = {
    // 5 versions: 0,1,2,3,4. Cache has 2,3,4. Archive has 0,1.
    val specs = (0 to 4).toList.map { v =>
      mkSpec(esId1, s"$esName1-v$v", pagePingSchemaKey, version = Some(v))
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      foundV0 <- enrichment.lookupInCache(esId1, 0)
      foundV0Again <- enrichment.lookupInCache(esId1, 0)
    } yield (foundV0 must beRight.like {
      case compiled => compiled.name must beEqualTo(s"$esName1-v0")
    }) and
      (foundV0Again must beRight)
  }

  def e21 = {
    val specs = List(
      mkSpec(esId1, esName1, pagePingSchemaKey, version = Some(0))
    )

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      found <- enrichment.lookupInCache(esId1, 99)
    } yield found must beLeft.like {
      case err => err.message must contain("version=99")
    }
  }

  def e22 = {
    val specs = List(
      mkSpec(esId1, esName1, pagePingSchemaKey, version = Some(0)),
      mkSpec(esId2, esName2, pagePingSchemaKey, version = Some(1), constraint = Some(testConstraint)),
      mkSpec(esId3, esName3, pagePingSchemaKey, version = Some(2), constraint = Some(wrongConstraint))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      page_url = "test.com"
    }
    val expectedSpecs = List(
      mkEventSpecSDJ(esId1, esName1, Some(0)),
      mkEventSpecSDJ(esId2, esName2, Some(1))
    )
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e23 = {
    val specsId1 = (0 to 4).toList.map { v =>
      mkSpec(esId1, s"$esName1-v$v", pagePingSchemaKey, version = Some(v))
    }
    val specs = specsId1 ++ List(
      mkSpec(esId2, esName2, pagePingSchemaKey, version = Some(0), constraint = Some(testConstraint)),
      mkSpec(esId3, esName3, pagePingSchemaKey, version = Some(0), constraint = Some(wrongConstraint))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      page_url = "test.com"
    }
    val expectedSpecs = List(
      mkEventSpecSDJ(esId1, s"$esName1-v4", Some(4)),
      mkEventSpecSDJ(esId2, esName2, Some(0))
    )
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e24 = {
    val requiredEntity1 = mkEntity(customEntityKey, minCardinality = Some(1))
    val requiredEntity2 = mkEntity(anotherEntityKey, minCardinality = Some(1), constraint = Some(notMatchingEntityConstraint))
    val specs = List(
      mkSpec(esId1, esName1, customEventKey, version = Some(0), entities = List(requiredEntity1, requiredEntity2)),
      mkSpec(esId2, esName2, customEventKey, version = Some(0), entities = List(requiredEntity1))
    )
    val eventWithBothEntities = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(customEventKey, json"""{"some_field": "some_value", "other_field": 123}""")
      )
      contexts = List(
        SelfDescribingData(customEntityKey, json"""{"entity_field": "entity_value"}"""),
        SelfDescribingData(anotherEntityKey, json"""{"another_entity_field": "another_entity_value"}""")
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId2, esName2, Some(0)))
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(eventWithBothEntities, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e25 = {
    val requiredEntity1 = mkEntity(customEntityKey, minCardinality = Some(1))
    val requiredEntity2 = mkEntity(anotherEntityKey, minCardinality = Some(1), constraint = Some(notMatchingEntityConstraint))
    val specsId1 = (0 to 4).toList.map { v =>
      mkSpec(esId1, s"$esName1-v$v", customEventKey, version = Some(v), entities = List(requiredEntity1, requiredEntity2))
    }
    val specs = specsId1 ++ List(
      mkSpec(esId2, esName2, customEventKey, version = Some(0), entities = List(requiredEntity1))
    )
    val eventWithBothEntities = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(customEventKey, json"""{"some_field": "some_value", "other_field": 123}""")
      )
      contexts = List(
        SelfDescribingData(customEntityKey, json"""{"entity_field": "entity_value"}"""),
        SelfDescribingData(anotherEntityKey, json"""{"another_entity_field": "another_entity_value"}""")
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId2, esName2, Some(0)))
    EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(eventWithBothEntities, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

  def e26 = {
    val specs = (0 to 2).toList.map { v =>
      mkSpec(esId1, s"$esName1-v$v", pagePingSchemaKey, version = Some(v))
    }
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      validation <- enrichment.validateEventSpec(event, esId1, 1, maxJsonDepth).map(goodStreamContexts)
    } yield validation must beEmpty
  }

  def e27 = {
    val specs = List(
      mkSpec(esId1, esName1, pagePingSchemaKey, version = Some(0), constraint = Some(testConstraint))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      page_url = "test.com"
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      validation <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth).map(goodStreamContexts)
    } yield validation must beEmpty
  }

  def e28 = {
    val specs = List(
      mkSpec(esId1, esName1, pagePingSchemaKey, version = Some(0), constraint = Some(testConstraint))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      page_url = "wrong.com"
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      validation <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth).map(goodStreamContexts)
    } yield {
      val errors = validation.head.data.hcursor.downField("errors").values.map(_.toList).getOrElse(List.empty)
      (validation must haveSize(1)) and (errors must not(beEmpty))
    }
  }

  def e29 = {
    val requiredEntity = mkEntity(customEntityKey, minCardinality = Some(1))
    val specs = List(
      mkSpec(esId1, esName1, pagePingSchemaKey, version = Some(0), entities = List(requiredEntity))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      validation <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth).map(goodStreamContexts)
    } yield {
      val errors = validation.head.data.hcursor.downField("errors").values.map(_.toList).getOrElse(List.empty)
      errors.flatMap(_.hcursor.get[String]("message").toOption) must contain(contain("expected at least 1"))
    }
  }

  def e30 = {
    val entityWithConstraint = mkEntity(customEntityKey, minCardinality = Some(1), constraint = Some(someValueConstraint))
    val specs = List(
      mkSpec(esId1, esName1, customEventKey, version = Some(0), entities = List(entityWithConstraint))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(customEventKey, json"""{"some_field": "some_value"}""")
      )
      contexts = List(
        SelfDescribingData(customEntityKey, json"""{"some_field": "wrong_value"}""")
      )
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      validation <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth).map(goodStreamContexts)
    } yield {
      val errors = validation.head.data.hcursor.downField("errors").values.map(_.toList).getOrElse(List.empty)
      (validation must haveSize(1)) and (errors must not(beEmpty))
    }
  }

  def e31 = {
    val specs = List(
      mkSpec(esId1, esName1, pagePingSchemaKey, version = Some(0))
    )

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      validation <- enrichment.validateEventSpec(event = new EnrichedEvent, "nonexistent", 99, maxJsonDepth).map(goodStreamContexts)
    } yield {
      val errors = validation.head.data.hcursor.downField("errors").values.map(_.toList).getOrElse(List.empty)
      val message = errors.head.hcursor.get[String]("message").getOrElse("")
      val errorKeys = errors.head.hcursor.keys.map(_.toList).getOrElse(List.empty)
      (validation must haveSize(1)) and (message must contain("not found")) and (errorKeys must beEqualTo(List("message")))
    }
  }

  def e32 = {
    val specs = (0 to 4).toList.map { v =>
      mkSpec(esId1, s"$esName1-v$v", pagePingSchemaKey, version = Some(v))
    }
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      validation <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth).map(goodStreamContexts)
    } yield validation must beEmpty
  }

  def e33 = {
    val specs = (0 to 4).toList.map { v =>
      mkSpec(esId1, s"$esName1-v$v", pagePingSchemaKey, version = Some(v), constraint = Some(testConstraint))
    }
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      page_url = "wrong.com"
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      validation <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth).map(goodStreamContexts)
    } yield {
      val errors = validation.head.data.hcursor.downField("errors").values.map(_.toList).getOrElse(List.empty)
      (validation must haveSize(1)) and (errors must not(beEmpty))
    }
  }

  def e34 = {
    val specs = List(mkSpec(esId1, esName1, pagePingSchemaKey))
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1))

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      actual <- enrichment.processEvent(event, maxJsonDepth)
    } yield actual must beLike { case Ior.Right(contexts) => contexts must beEqualTo(expectedSpecs) }
  }

  def e35 = {
    val specs = List(mkSpec(esId1, esName1, pagePingSchemaKey, version = Some(0)))
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      contexts = List(
        SelfDescribingData(
          eventSpecSchemaKey,
          json"""{"id": $esId1, "version": 0}"""
        )
      )
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      actual <- enrichment.processEvent(event, maxJsonDepth)
    } yield actual must beLike { case Ior.Right(contexts) => contexts must beEmpty }
  }

  def e36 = {
    val specs = List(mkSpec(esId1, esName1, pagePingSchemaKey))
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      contexts = List(
        SelfDescribingData(
          eventSpecSchemaKey,
          json"""{"id": $esId1}"""
        )
      )
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      actual <- enrichment.processEvent(event, maxJsonDepth)
    } yield actual must beLike { case Ior.Right(contexts) => contexts must beEmpty }
  }

  def e41 = {
    val specs = List(
      mkSpec(esId1, esName1, pagePingSchemaKey, version = Some(0), constraint = Some(testConstraint))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      page_url = "wrong.com"
      contexts = List(
        SelfDescribingData(
          eventSpecSchemaKey,
          json"""{"id": $esId1, "version": 0}"""
        )
      )
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      actual <- enrichment.processEvent(event, maxJsonDepth)
    } yield actual must beLike {
      case Ior.Right(contexts) =>
        val errors = contexts.head.data.hcursor.downField("errors").values.map(_.toList).getOrElse(List.empty)
        (contexts must haveSize(1)) and (errors must not(beEmpty))
    }
  }

  def e44 = {
    val specs = List(
      mkSpec(esId1, esName1, pagePingSchemaKey, version = Some(0), entities = List(mkEntity(customEntityKey, minCardinality = Some(1))))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      validation <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth).map(goodStreamContexts)
    } yield {
      val errors = validation.head.data.hcursor.downField("errors").values.map(_.toList).getOrElse(List.empty)
      val first = errors.head.hcursor
      (validation must haveSize(1)) and
        (errors must haveSize(1)) and
        (first.get[String]("message").getOrElse("") must contain("expected at least 1")) and
        (first.get[String]("schema").getOrElse("") must beEqualTo(customEntityKey.toSchemaUri)) and
        (first.keys.map(_.toList).getOrElse(List.empty) must containTheSameElementsAs(List("message", "schema")))
    }
  }

  def e45 = {
    val specs = List(
      mkSpec(esId1, esName1, pagePingSchemaKey, version = Some(0), entities = List(mkEntity(customEntityKey, maxCardinality = Some(1))))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      contexts = List(
        SelfDescribingData(customEntityKey, json"""{"some_field": "a"}"""),
        SelfDescribingData(customEntityKey, json"""{"some_field": "b"}""")
      )
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      validation <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth).map(goodStreamContexts)
    } yield {
      val errors = validation.head.data.hcursor.downField("errors").values.map(_.toList).getOrElse(List.empty)
      val first = errors.head.hcursor
      (validation must haveSize(1)) and
        (errors must haveSize(1)) and
        (first.get[String]("message").getOrElse("") must contain("expected at most 1, found 2")) and
        (first.get[String]("schema").getOrElse("") must beEqualTo(customEntityKey.toSchemaUri))
    }
  }

  def e46 = {
    val entityWithConstraint = mkEntity(customEntityKey, minCardinality = Some(1), constraint = Some(someValueConstraint))
    val specs = List(
      mkSpec(esId1, esName1, customEventKey, version = Some(0), entities = List(entityWithConstraint))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(SelfDescribingData(customEventKey, json"""{"some_field": "some_value"}"""))
      contexts = List(SelfDescribingData(customEntityKey, json"""{"some_field": "wrong_value"}"""))
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      validation <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth).map(goodStreamContexts)
    } yield {
      val errors = validation.head.data.hcursor.downField("errors").values.map(_.toList).getOrElse(List.empty)
      val first = errors.head.hcursor
      (validation must haveSize(1)) and
        (errors must haveSize(1)) and
        (first.get[String]("schema").getOrElse("") must beEqualTo(customEntityKey.toSchemaUri)) and
        (first.get[String]("path").toOption must beSome) and
        (first.keys.map(_.toList).getOrElse(List.empty) must containTheSameElementsAs(List("message", "schema", "path")))
    }
  }

  def e47 = {
    val specs = List(
      mkSpec(
        esId1,
        esName1,
        pagePingSchemaKey,
        version = Some(0),
        constraint = Some(wrongConstraint),
        entities = List(mkEntity(customEntityKey, minCardinality = Some(1)))
      )
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      page_url = "test.com"
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      validation <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth).map(goodStreamContexts)
    } yield {
      val errors = validation.head.data.hcursor.downField("errors").values.map(_.toList).getOrElse(List.empty)
      val schemas = errors.flatMap(_.hcursor.get[String]("schema").toOption)
      (validation must haveSize(1)) and
        (errors must haveSize(2)) and
        (schemas must containTheSameElementsAs(List(pagePingSchemaKey.toSchemaUri, customEntityKey.toSchemaUri)))
    }
  }

  def e48 = {
    val specs = List(mkSpec(esId1, esName1, pagePingSchemaKey, version = Some(0)))
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      contexts = List(
        SelfDescribingData(eventSpecSchemaKey, json"""{"id": $esId1, "version": 0}""")
      )
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      actual <- enrichment.processEvent(event, maxJsonDepth)
    } yield actual must beLike { case Ior.Right(contexts) => contexts must beEmpty }
  }

  def e49 = {
    val specs = List(
      mkSpec(esId1, esName1, pagePingSchemaKey, version = Some(0), entities = List(mkEntity(customEntityKey, minCardinality = Some(1))))
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      contexts = List(
        SelfDescribingData(eventSpecSchemaKey, json"""{"id": $esId1, "version": 0}""")
      )
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      actual <- enrichment.processEvent(event, maxJsonDepth)
    } yield actual must beLike {
      case Ior.Right(contexts) =>
        val isValid = contexts.head.data.hcursor.get[Boolean]("isValid").getOrElse(true)
        val errors = contexts.head.data.hcursor.downField("errors").values.map(_.toList).getOrElse(List.empty)
        val message = errors.head.hcursor.get[String]("message").getOrElse("")
        (contexts must haveSize(1)) and (isValid must beFalse) and (errors must haveSize(1)) and (message must contain(
          "expected at least 1"
        ))
    }
  }

  def e50 = {
    val specs = List(
      mkSpec(
        esId1,
        esName1,
        pagePingSchemaKey,
        version = Some(0),
        constraint = Some(testConstraint),
        sendInvalidToBad = Some(true)
      )
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      page_url = "wrong.com"
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      actual <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth)
    } yield actual must beLike {
      case Ior.Both(failures, contexts) =>
        val attributed = failures.toList.forall(_.enrichment.exists(_.schemaKey == enrichmentConfigSchemaKey))
        val errors = contexts.head.data.hcursor.downField("errors").values.map(_.toList).getOrElse(List.empty)
        (attributed must beTrue) and
          (contexts must haveSize(1)) and
          (errors must not(beEmpty))
    }
  }

  def e51 = {
    val specs = List(
      mkSpec(
        esId1,
        esName1,
        pagePingSchemaKey,
        version = Some(0),
        constraint = Some(testConstraint),
        sendInvalidToBad = Some(true)
      )
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      page_url = "test.com"
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      actual <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth)
    } yield actual must beLike {
      case Ior.Right(contexts) => contexts must beEmpty
    }
  }

  def e52 = {
    val specs = List(
      mkSpec(
        esId1,
        esName1,
        pagePingSchemaKey,
        version = Some(0),
        constraint = Some(testConstraint),
        sendInvalidToBad = Some(false)
      )
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      page_url = "wrong.com"
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      actual <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth)
    } yield actual must beLike {
      case Ior.Right(contexts) =>
        val errors = contexts.head.data.hcursor.downField("errors").values.map(_.toList).getOrElse(List.empty)
        (contexts must haveSize(1)) and (errors must not(beEmpty))
    }
  }

  def e53 = {
    val withFlag = json"""{
      "id": $esId1,
      "name": $esName1,
      "version": 0,
      "schemaKey": "iglu:com.snowplowanalytics.snowplow/page_ping/jsonschema/1-0-0",
      "entities": [],
      "sendInvalidToBad": true
    }"""
    val withoutFlag = json"""{
      "id": $esId1,
      "name": $esName1,
      "version": 0,
      "schemaKey": "iglu:com.snowplowanalytics.snowplow/page_ping/jsonschema/1-0-0",
      "entities": []
    }"""

    (withFlag.as[EventSpec].map(_.sendInvalidToBad) must beRight(Some(true): Option[Boolean])) and
      (withoutFlag.as[EventSpec].map(_.sendInvalidToBad) must beRight(None: Option[Boolean]))
  }

  def e54 = {
    val specs = (0 to 4).toList.map { v =>
      mkSpec(
        esId1,
        s"$esName1-v$v",
        pagePingSchemaKey,
        version = Some(v),
        constraint = Some(testConstraint),
        sendInvalidToBad = Some(true)
      )
    }
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      page_url = "wrong.com"
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      actual <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth)
    } yield actual must beLike {
      case Ior.Both(failures, contexts) =>
        (failures.toList must not(beEmpty)) and (contexts must haveSize(1))
    }
  }

  def e55 = {
    val requiredEntity = mkEntity(customEntityKey, minCardinality = Some(1))
    val specs = List(
      mkSpec(
        esId1,
        esName1,
        pagePingSchemaKey,
        version = Some(0),
        constraint = Some(testConstraint),
        entities = List(requiredEntity),
        sendInvalidToBad = Some(true)
      )
    )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      page_url = "wrong.com"
    }

    for {
      result <- EventSpecEnrichment.createFromSpecs[IO](specs, enrichmentConfigSchemaKey)
      enrichment = result.fold(e => sys.error(e), identity)
      actual <- enrichment.validateEventSpec(event, esId1, 0, maxJsonDepth)
    } yield actual must beLike {
      case Ior.Both(failures, contexts) =>
        val errors = contexts.head.data.hcursor.downField("errors").values.map(_.toList).getOrElse(List.empty)
        (failures.size must beEqualTo(errors.size)) and (errors must haveSize(2))
    }
  }

}
