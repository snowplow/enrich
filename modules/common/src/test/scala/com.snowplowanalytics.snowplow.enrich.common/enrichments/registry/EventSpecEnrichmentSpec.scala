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

import cats.data.EitherT
import cats.syntax.either._
import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EventSpecEnrichment.{Entity, EventSpec}
import org.specs2.Specification
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import io.circe.literal.JsonStringContext

class EventSpecEnrichmentSpec extends Specification with CatsEffect {

  val esId1 = "75604331-bff1-41bf-9adb-9b7ac10f4de8"
  val esName1 = "testEs1"
  val esId2 = "c6fcb515-24ec-4bdb-8911-50b37001fe58"
  val esName2 = "testEs2"
  val esId3 = "0e543161-19ad-4b93-a5ac-4eb202c44121"
  val esName3 = "testEs3"

  val maxJsonDepth = 10

  val eventSpecSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "event_specification", "jsonschema", SchemaVer.Full(1, 0, 3))
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

  /** Helper to build an event spec */
  def mkSpec(
    id: String,
    name: String,
    schemaKey: SchemaKey,
    version: Option[Int] = None,
    constraint: Option[io.circe.Json] = None,
    entities: List[Entity] = List.empty
  ): EventSpec =
    EventSpec(id, name, version, schemaKey, constraint, entities)

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
  """

  def e1 = {
    val event = new EnrichedEvent {
      app_id = "some-app-id"
    }
    EventSpecEnrichment.createFromSpecs[IO](List.empty).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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

    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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

    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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

    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
      result <- EventSpecEnrichment.createFromSpecs[IO](specs)
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
      result <- EventSpecEnrichment.createFromSpecs[IO](specs)
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
      result <- EventSpecEnrichment.createFromSpecs[IO](specs)
      enrichment = result.fold(e => sys.error(e), identity)
      found <- enrichment.lookupInCache(esId1, 99)
    } yield found must beLeft.like {
      case err: EventSpecEnrichment.LookupError.NotFound => err.version must beEqualTo(99)
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
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
    EventSpecEnrichment.createFromSpecs[IO](specs).map { result =>
      result must beRight.like {
        case enrichment =>
          enrichment.inferEventSpec(eventWithBothEntities, maxJsonDepth) must beEqualTo(expectedSpecs)
      }
    }
  }

}
