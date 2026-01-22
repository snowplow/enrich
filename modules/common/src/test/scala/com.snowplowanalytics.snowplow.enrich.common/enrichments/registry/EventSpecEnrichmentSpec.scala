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

  def mkEventSpecSDJ(id: String, name: String) =
    SelfDescribingData(eventSpecSchemaKey, json"""
        {
          "id" : $id,
          "name" : $name
        }
      """)

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
  """

  def e1 = {
    val eventSpecs = List.empty[EventSpecEnrichment.EventSpec]
    val event = new EnrichedEvent {
      app_id = "some-app-id"
    }
    EventSpecEnrichment.createFromSpecs(eventSpecs) must beRight.like {
      case enrichment: EventSpecEnrichment =>
        enrichment.inferEventSpec(event, maxJsonDepth) must beEmpty
    }
  }

  def e2 = {
    val eventSpecs = List(EventSpecEnrichment.EventSpec(esId1, esName1, pagePingSchemaKey, None, List.empty))
    val event = new EnrichedEvent {
      app_id = "some-app-id"
    }
    EventSpecEnrichment.createFromSpecs(eventSpecs) must beRight.like {
      case enrichment: EventSpecEnrichment =>
        enrichment.inferEventSpec(event, maxJsonDepth) must beEmpty
    }
  }

  def e3 = {
    val eventSpecs =
      List(
        EventSpecEnrichment.EventSpec(esId1, esName1, pagePingSchemaKey, None, List.empty)
      )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1))
    EventSpecEnrichment.createFromSpecs(eventSpecs) must beRight.like {
      case enrichment: EventSpecEnrichment =>
        enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
    }
  }

  def e4 = {
    val eventSpecs =
      List(
        EventSpecEnrichment.EventSpec(esId1, esName1, pagePingSchemaKey, None, List.empty),
        EventSpecEnrichment.EventSpec(
          esId2,
          esName2,
          pagePingSchemaKey,
          Some(json"""{
                    "type": "object",
                    "schema": "http://json-schema.org/draft-04/schema#",
                    "properties": {
                        "page_url": {
                            "enum": [
                                "test.com"
                            ],
                            "type": "string",
                            "description": ""
                        }
                    },
                    "additionalProperties": true
                }
        """),
          List.empty
        ),
        EventSpecEnrichment.EventSpec(
          esId3,
          esName3,
          pagePingSchemaKey,
          Some(json"""{
                    "type": "object",
                    "schema": "http://json-schema.org/draft-04/schema#",
                    "properties": {
                        "page_url": {
                            "enum": [
                                "wrong.com"
                            ],
                            "type": "string",
                            "description": ""
                        }
                    },
                    "additionalProperties": true
                }
        """),
          List.empty
        )
      )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
      page_url = "test.com"
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1), mkEventSpecSDJ(esId2, esName2))
    EventSpecEnrichment.createFromSpecs(eventSpecs) must beRight.like {
      case enrichment: EventSpecEnrichment =>
        enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
    }
  }

  def e5 = {
    val eventSpecs =
      List(
        EventSpecEnrichment.EventSpec(esId1, esName1, pageViewSchemaKey, None, List.empty)
      )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_view"
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1))
    EventSpecEnrichment.createFromSpecs(eventSpecs) must beRight.like {
      case enrichment: EventSpecEnrichment =>
        enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
    }
  }

  def e6 = {
    val eventSpecs =
      List(
        EventSpecEnrichment.EventSpec(esId1,
                                      esName1,
                                      SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
                                      None,
                                      List.empty
        )
      )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "some_field": "some_value",
            "other_field": 123
          }"""
        )
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1))
    EventSpecEnrichment.createFromSpecs(eventSpecs) must beRight.like {
      case enrichment: EventSpecEnrichment =>
        enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
    }
  }

  def e7 = {
    val eventSpecs =
      List(
        EventSpecEnrichment.EventSpec(esId1,
                                      esName1,
                                      SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
                                      None,
                                      List.empty
        ),
        EventSpecEnrichment.EventSpec(
          esId2,
          esName2,
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Some(
            json"""{
            "type": "object",
            "schema": "http://json-schema.org/draft-04/schema#",
            "properties": {
                "some_field": {
                    "enum": [
                        "some_value"
                    ],
                    "type": "string",
                    "description": ""
                }
            },
            "additionalProperties": true
        }
        """
          ),
          List.empty
        ),
        EventSpecEnrichment.EventSpec(
          esId3,
          esName3,
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          Some(
            json"""{
            "type": "object",
            "schema": "http://json-schema.org/draft-04/schema#",
            "properties": {
                "some_field": {
                    "enum": [
                        "wrong_value"
                    ],
                    "type": "string",
                    "description": ""
                }
            },
            "additionalProperties": true
        }
        """
          ),
          List.empty
        )
      )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "some_field": "some_value",
            "other_field": 123
          }"""
        )
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1), mkEventSpecSDJ(esId2, esName2))
    EventSpecEnrichment.createFromSpecs(eventSpecs) must beRight.like {
      case enrichment: EventSpecEnrichment =>
        enrichment.inferEventSpec(event, maxJsonDepth) must beEqualTo(expectedSpecs)
    }
  }

  def e8 = {
    val requiredEntity1 = EventSpecEnrichment.Entity(
      SchemaKey("com.snowplowanalytics.snowplow", "custom_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
      Some(1),
      None,
      None
    )
    val requiredEntity2 = EventSpecEnrichment.Entity(
      SchemaKey("com.snowplowanalytics.snowplow", "another_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
      Some(1),
      None,
      None
    )
    val eventSpecs =
      List(
        EventSpecEnrichment.EventSpec(
          esId1,
          esName1,
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          None,
          List(requiredEntity1, requiredEntity2)
        )
      )
    val eventWithSingleEntity = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "some_field": "some_value",
            "other_field": 123
          }"""
        )
      )
      contexts = List(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "entity_field": "entity_value"
          }"""
        )
      )
    }
    EventSpecEnrichment.createFromSpecs(eventSpecs) must beRight.like {
      case enrichment: EventSpecEnrichment =>
        enrichment.inferEventSpec(eventWithSingleEntity, maxJsonDepth) must beEmpty
    }
  }

  def e9 = {
    val requiredEntity1 = EventSpecEnrichment.Entity(
      SchemaKey("com.snowplowanalytics.snowplow", "custom_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
      Some(1),
      None,
      None
    )
    val requiredEntity2 = EventSpecEnrichment.Entity(
      SchemaKey("com.snowplowanalytics.snowplow", "another_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
      Some(1),
      None,
      None
    )
    val eventSpecs =
      List(
        EventSpecEnrichment.EventSpec(
          esId1,
          esName1,
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          None,
          List(requiredEntity1, requiredEntity2)
        ),
        EventSpecEnrichment.EventSpec(esId2,
                                      esName2,
                                      SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
                                      None,
                                      List(requiredEntity1)
        )
      )
    val eventWithSingleEntity = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "some_field": "some_value",
            "other_field": 123
          }"""
        )
      )
      contexts = List(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "entity_field": "entity_value"
          }"""
        )
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId2, esName2))
    EventSpecEnrichment.createFromSpecs(eventSpecs) must beRight.like {
      case enrichment: EventSpecEnrichment =>
        enrichment.inferEventSpec(eventWithSingleEntity, maxJsonDepth) must beEqualTo(expectedSpecs)
    }
  }

  def e10 = {
    val requiredEntity1 = EventSpecEnrichment.Entity(
      SchemaKey("com.snowplowanalytics.snowplow", "custom_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
      Some(1),
      None,
      None
    )
    val requiredEntity2 = EventSpecEnrichment.Entity(
      SchemaKey("com.snowplowanalytics.snowplow", "another_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
      Some(1),
      None,
      None
    )
    val eventSpecs =
      List(
        EventSpecEnrichment.EventSpec(
          esId1,
          esName1,
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          None,
          List(requiredEntity1, requiredEntity2)
        ),
        EventSpecEnrichment.EventSpec(esId2,
                                      esName2,
                                      SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
                                      None,
                                      List(requiredEntity1)
        )
      )
    val eventWithSingleEntity = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "some_field": "some_value",
            "other_field": 123
          }"""
        )
      )
      contexts = List(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "entity_field": "entity_value"
          }"""
        ),
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "another_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "another_entity_field": "another_entity_value"
          }"""
        )
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1), mkEventSpecSDJ(esId2, esName2))
    EventSpecEnrichment.createFromSpecs(eventSpecs) must beRight.like {
      case enrichment: EventSpecEnrichment =>
        enrichment.inferEventSpec(eventWithSingleEntity, maxJsonDepth) must beEqualTo(expectedSpecs)
    }
  }

  def e11 = {
    val requiredEntity1 = EventSpecEnrichment.Entity(
      SchemaKey("com.snowplowanalytics.snowplow", "custom_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
      Some(1),
      None,
      None
    )
    val requiredEntity2 = EventSpecEnrichment.Entity(
      SchemaKey("com.snowplowanalytics.snowplow", "another_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
      Some(1),
      None,
      Some(
        json"""{
                    "type": "object",
                    "schema": "http://json-schema.org/draft-04/schema#",
                    "properties": {
                        "another_entity_field": {
                            "enum": [
                                "something_not_matching"
                            ],
                            "type": "string",
                            "description": ""
                        }
                    },
                    "additionalProperties": true
                }
        """
      )
    )
    val eventSpecs =
      List(
        EventSpecEnrichment.EventSpec(
          esId1,
          esName1,
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          None,
          List(requiredEntity1, requiredEntity2)
        ),
        EventSpecEnrichment.EventSpec(esId2,
                                      esName2,
                                      SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
                                      None,
                                      List(requiredEntity1)
        )
      )
    val eventWithSingleEntity = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "some_field": "some_value",
            "other_field": 123
          }"""
        )
      )
      contexts = List(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "entity_field": "entity_value"
          }"""
        ),
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "another_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "another_entity_field": "another_entity_value"
          }"""
        )
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId2, esName2))
    EventSpecEnrichment.createFromSpecs(eventSpecs) must beRight.like {
      case enrichment: EventSpecEnrichment =>
        enrichment.inferEventSpec(eventWithSingleEntity, maxJsonDepth) must beEqualTo(expectedSpecs)
    }
  }

  def e12 = {
    val eventSpecs =
      List(
        EventSpecEnrichment.EventSpec(esId1,
                                      esName1,
                                      SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
                                      None,
                                      List.empty
        )
      )
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "some_field": "some_value",
            "other_field": 123
          }"""
        )
      )
      contexts = List(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "event_specification", "jsonschema", SchemaVer.Full(1, 0, 1)),
          json"""{
            "dummy": "dummy"
          }"""
        )
      )
    }
    EventSpecEnrichment.createFromSpecs(eventSpecs) must beRight.like {
      case enrichment: EventSpecEnrichment =>
        enrichment.inferEventSpec(event, maxJsonDepth) must beEmpty
    }
  }

  def e13 = {
    val requiredEntity1 = EventSpecEnrichment.Entity(
      SchemaKey("com.snowplowanalytics.snowplow", "custom_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
      Some(1),
      None,
      None
    )
    val requiredEntity2 = EventSpecEnrichment.Entity(
      SchemaKey("com.snowplowanalytics.snowplow", "another_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
      None,
      None,
      Some(
        json"""{
                    "type": "object",
                    "schema": "http://json-schema.org/draft-04/schema#",
                    "properties": {
                        "another_entity_field": {
                            "enum": [
                                "something_not_matching"
                            ],
                            "type": "string",
                            "description": ""
                        }
                    },
                    "additionalProperties": true
                }
        """
      )
    )
    val eventSpecs =
      List(
        EventSpecEnrichment.EventSpec(
          esId1,
          esName1,
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          None,
          List(requiredEntity1, requiredEntity2)
        ),
        EventSpecEnrichment.EventSpec(esId2,
                                      esName2,
                                      SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
                                      None,
                                      List(requiredEntity1)
        )
      )
    val eventWithSingleEntity = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "some_field": "some_value",
            "other_field": 123
          }"""
        )
      )
      contexts = List(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "entity_field": "entity_value"
          }"""
        ),
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "another_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "another_entity_field": "another_entity_value"
          }"""
        )
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId2, esName2))
    EventSpecEnrichment.createFromSpecs(eventSpecs) must beRight.like {
      case enrichment: EventSpecEnrichment =>
        enrichment.inferEventSpec(eventWithSingleEntity, maxJsonDepth) must beEqualTo(expectedSpecs)
    }
  }

  def e14 = {
    val requiredEntity1 = EventSpecEnrichment.Entity(
      SchemaKey("com.snowplowanalytics.snowplow", "custom_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
      Some(1),
      None,
      None
    )
    val requiredEntity2 = EventSpecEnrichment.Entity(
      SchemaKey("com.snowplowanalytics.snowplow", "another_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
      None,
      None,
      Some(
        json"""{
                    "type": "object",
                    "schema": "http://json-schema.org/draft-04/schema#",
                    "properties": {
                        "another_entity_field": {
                            "enum": [
                                "something_not_matching"
                            ],
                            "type": "string",
                            "description": ""
                        }
                    },
                    "additionalProperties": true
                }
        """
      )
    )
    val eventSpecs =
      List(
        EventSpecEnrichment.EventSpec(
          esId1,
          esName1,
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          None,
          List(requiredEntity1, requiredEntity2)
        ),
        EventSpecEnrichment.EventSpec(esId2,
                                      esName2,
                                      SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
                                      None,
                                      List(requiredEntity1)
        )
      )
    val eventWithSingleEntity = new EnrichedEvent {
      app_id = "some-app-id"
      unstruct_event = Some(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "some_field": "some_value",
            "other_field": 123
          }"""
        )
      )
      contexts = List(
        SelfDescribingData(
          SchemaKey("com.snowplowanalytics.snowplow", "custom_entity", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{
            "entity_field": "entity_value"
          }"""
        )
      )
    }
    val expectedSpecs = List(mkEventSpecSDJ(esId1, esName1), mkEventSpecSDJ(esId2, esName2))
    EventSpecEnrichment.createFromSpecs(eventSpecs) must beRight.like {
      case enrichment: EventSpecEnrichment =>
        enrichment.inferEventSpec(eventWithSingleEntity, maxJsonDepth) must beEqualTo(expectedSpecs)
    }
  }

  def e15 = {
    val event = new EnrichedEvent {
      app_id = "some-app-id"
      event_name = "page_ping"
    }
    val expectedSpecs = List(mkEventSpecSDJ("test-spec-from-file", "Test Event Spec From File"))

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

}
