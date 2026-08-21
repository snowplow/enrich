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

import org.specs2.Specification
import io.circe.literal._

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}

object MetadataSpec extends Specification {

  def is = s2"""
  Metadata.extractsForBatch with a single event should:
    Extract empty metadata for an empty event $e1
    Extract all relevant fields when set on a single event $e2
    Extract extract entities from contexts field and derived_contexts field $e3
    Extract scenarioId from contexts if present $e4a
    Extract scenarioId from derived_contexts if present and send a plain event count $e4b
    Extract scenarioVersion if present $e4c
    Extract validation failures if present $e4d
    Marks event as invalid, regardless of the isValid property in the context $e4e
    Emit a row per inferred spec plus a single plain row when multiple specs are inferred $e4f
    Not emit a plain row when a stated spec is present $e4g
    Extract null as scenario id if scenario context is present with no id $e5
  Metadata.extractsForBatch with a batch of many events should:
    Merge the plain row of an inferred-spec event with a spec-less event into one bucket $e4h
    Increment the `count` when multiple events yield the same `MetadataEvent` $e6
    Merge entities from similar events with different entities $e7
    Not merge entities when they come from different unstruct event types $e8
  """

  def e1 = {
    val input = new EnrichedEvent

    val expectedKey = Metadata.MetadataEvent(
      schema = None,
      source = None,
      tracker = None,
      platform = None,
      scenarioId = None,
      scenarioVersion = None,
      eventSpecInferred = false,
      eventSpecValid = true
    )
    val expectedValue = Metadata.EntitiesAndCount(Set.empty, 1)

    val result = Metadata.extractsForBatch(List(input))
    result must beEqualTo(Map(expectedKey -> expectedValue))
  }

  def e2 = {
    val unstructKey = SchemaKey.fromUri("iglu:myvendor/myschema/jsonschema/1-0-42").toOption.get
    val contextKey1 = SchemaKey.fromUri("iglu:myvendor/mycontext1/jsonschema/1-0-42").toOption.get
    val contextKey2 = SchemaKey.fromUri("iglu:myvendor/mycontext2/jsonschema/1-0-42").toOption.get

    val input = new EnrichedEvent
    input.app_id = "myapp"
    input.v_tracker = "mytracker"
    input.platform = "myplatform"
    input.unstruct_event = Some(SelfDescribingData(unstructKey, json"""{"x": "y"}"""))
    input.contexts = List(SelfDescribingData(contextKey1, json"""{"x": "y"}"""), SelfDescribingData(contextKey2, json"""{"x": "y"}"""))

    val expectedKey = Metadata.MetadataEvent(
      schema = Some(unstructKey),
      source = Some("myapp"),
      tracker = Some("mytracker"),
      platform = Some("myplatform"),
      scenarioId = None,
      scenarioVersion = None,
      eventSpecInferred = false,
      eventSpecValid = true
    )
    val expectedValue = Metadata.EntitiesAndCount(Set(contextKey1, contextKey2), 1)

    val result = Metadata.extractsForBatch(List(input))
    result must beEqualTo(Map(expectedKey -> expectedValue))
  }

  def e3 = {
    val contextKey1 = SchemaKey.fromUri("iglu:myvendor/mycontext1/jsonschema/1-0-42").toOption.get
    val contextKey2 = SchemaKey.fromUri("iglu:myvendor/mycontext2/jsonschema/1-0-42").toOption.get

    val input = new EnrichedEvent
    input.contexts = List(SelfDescribingData(contextKey1, json"""{"x": "y"}"""))
    input.derived_contexts = List(SelfDescribingData(contextKey2, json"""{"x": "y"}"""))

    val expectedKey = Metadata.MetadataEvent(
      schema = None,
      source = None,
      tracker = None,
      platform = None,
      scenarioId = None,
      scenarioVersion = None,
      eventSpecInferred = false,
      eventSpecValid = true
    )
    val expectedValue = Metadata.EntitiesAndCount(Set(contextKey1, contextKey2), 1)

    val result = Metadata.extractsForBatch(List(input))
    result must beEqualTo(Map(expectedKey -> expectedValue))
  }

  def e4a = {
    val contextKey1 = SchemaKey.fromUri("iglu:myvendor/mycontext1/jsonschema/1-0-42").toOption.get
    val contextKey2 = SchemaKey.fromUri("iglu:myvendor/mycontext2/jsonschema/1-0-42").toOption.get
    val contextKey3 = SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/event_specification/jsonschema/1-0-0").toOption.get

    val input = new EnrichedEvent
    input.contexts = List(
      SelfDescribingData(contextKey1, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey2, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey3, json"""{"id": "18056e39-6147-4824-bd67-5f6f6f7e18ee"}""")
    )

    val expectedKey = Metadata.MetadataEvent(
      schema = None,
      source = None,
      tracker = None,
      platform = None,
      scenarioId = Some("18056e39-6147-4824-bd67-5f6f6f7e18ee"),
      scenarioVersion = None,
      eventSpecInferred = false,
      eventSpecValid = true
    )
    val expectedValue = Metadata.EntitiesAndCount(Set(contextKey1, contextKey2, contextKey3), 1)

    val result = Metadata.extractsForBatch(List(input))
    result must beEqualTo(Map(expectedKey -> expectedValue))
  }

  def e4b = {
    val contextKey1 = SchemaKey.fromUri("iglu:myvendor/mycontext1/jsonschema/1-0-42").toOption.get
    val contextKey2 = SchemaKey.fromUri("iglu:myvendor/mycontext2/jsonschema/1-0-42").toOption.get
    val contextKey3 = SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/event_specification/jsonschema/1-0-0").toOption.get

    val input = new EnrichedEvent
    input.derived_contexts = List(
      SelfDescribingData(contextKey1, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey2, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey3, json"""{"id": "18056e39-6147-4824-bd67-5f6f6f7e18ee"}""")
    )

    val expectedKeyInferred = Metadata.MetadataEvent(
      schema = None,
      source = None,
      tracker = None,
      platform = None,
      scenarioId = Some("18056e39-6147-4824-bd67-5f6f6f7e18ee"),
      scenarioVersion = None,
      eventSpecInferred = true,
      eventSpecValid = true
    )
    // we have to send both inferred as well as plain one
    // plain one is used to count total number of events without multiplying the number by inference
    val expectedKeyPlain = Metadata.MetadataEvent(
      schema = None,
      source = None,
      tracker = None,
      platform = None,
      scenarioId = None,
      scenarioVersion = None,
      eventSpecInferred = false,
      eventSpecValid = true
    )
    val expectedValue = Metadata.EntitiesAndCount(Set(contextKey1, contextKey2, contextKey3), 1)

    val result = Metadata.extractsForBatch(List(input))
    result must beEqualTo(Map(expectedKeyInferred -> expectedValue, expectedKeyPlain -> expectedValue))
  }

  def e4c = {
    val contextKey1 = SchemaKey.fromUri("iglu:myvendor/mycontext1/jsonschema/1-0-42").toOption.get
    val contextKey2 = SchemaKey.fromUri("iglu:myvendor/mycontext2/jsonschema/1-0-42").toOption.get
    val contextKey3 = SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/event_specification/jsonschema/1-0-0").toOption.get

    val input = new EnrichedEvent
    input.contexts = List(
      SelfDescribingData(contextKey1, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey2, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey3, json"""{"id": "18056e39-6147-4824-bd67-5f6f6f7e18ee", "version": 0}""")
    )

    val expectedKey = Metadata.MetadataEvent(
      schema = None,
      source = None,
      tracker = None,
      platform = None,
      scenarioId = Some("18056e39-6147-4824-bd67-5f6f6f7e18ee"),
      scenarioVersion = Some(0),
      eventSpecInferred = false,
      eventSpecValid = true
    )
    val expectedValue = Metadata.EntitiesAndCount(Set(contextKey1, contextKey2, contextKey3), 1)

    val result = Metadata.extractsForBatch(List(input))
    result must beEqualTo(Map(expectedKey -> expectedValue))
  }

  def e4d = {
    val contextKey1 = SchemaKey.fromUri("iglu:myvendor/mycontext1/jsonschema/1-0-42").toOption.get
    val contextKey2 = SchemaKey.fromUri("iglu:myvendor/mycontext2/jsonschema/1-0-42").toOption.get
    val contextKey3 = SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/event_specification/jsonschema/1-0-0").toOption.get
    val contextKey4 =
      SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/event_specification_validation/jsonschema/1-0-0").toOption.get

    val input = new EnrichedEvent
    input.contexts = List(
      SelfDescribingData(contextKey1, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey2, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey3, json"""{"id": "18056e39-6147-4824-bd67-5f6f6f7e18ee", "version": 0}""")
    )
    input.derived_contexts = List(
      SelfDescribingData(contextKey1, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey2, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey4, json"""{"x": "y"}""")
    )

    val expectedKey = Metadata.MetadataEvent(
      schema = None,
      source = None,
      tracker = None,
      platform = None,
      scenarioId = Some("18056e39-6147-4824-bd67-5f6f6f7e18ee"),
      scenarioVersion = Some(0),
      eventSpecInferred = false,
      eventSpecValid = false
    )
    val expectedValue = Metadata.EntitiesAndCount(Set(contextKey1, contextKey2, contextKey3, contextKey4), 1)

    val result = Metadata.extractsForBatch(List(input))
    result must beEqualTo(Map(expectedKey -> expectedValue))
  }

  def e4e = {
    val contextKey1 = SchemaKey.fromUri("iglu:myvendor/mycontext1/jsonschema/1-0-42").toOption.get
    val contextKey2 = SchemaKey.fromUri("iglu:myvendor/mycontext2/jsonschema/1-0-42").toOption.get
    val contextKey3 = SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/event_specification/jsonschema/1-0-0").toOption.get
    val contextKey4 =
      SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/event_specification_validation/jsonschema/1-0-0").toOption.get

    val input = new EnrichedEvent
    input.contexts = List(
      SelfDescribingData(contextKey1, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey2, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey3, json"""{"id": "18056e39-6147-4824-bd67-5f6f6f7e18ee", "version": 0}""")
    )
    input.derived_contexts = List(
      SelfDescribingData(contextKey1, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey2, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey4, json"""{"isValid": true}""")
    )

    val expectedKey = Metadata.MetadataEvent(
      schema = None,
      source = None,
      tracker = None,
      platform = None,
      scenarioId = Some("18056e39-6147-4824-bd67-5f6f6f7e18ee"),
      scenarioVersion = Some(0),
      eventSpecInferred = false,
      eventSpecValid = false // currently only send event_specification_validation on failures
      // if that ever changes, this test should be updates to reflect that
    )
    val expectedValue = Metadata.EntitiesAndCount(Set(contextKey1, contextKey2, contextKey3, contextKey4), 1)

    val result = Metadata.extractsForBatch(List(input))
    result must beEqualTo(Map(expectedKey -> expectedValue))
  }

  def e4f = {
    val contextKey1 = SchemaKey.fromUri("iglu:myvendor/mycontext1/jsonschema/1-0-42").toOption.get
    val eventSpecKey = SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/event_specification/jsonschema/1-0-0").toOption.get

    val input = new EnrichedEvent
    input.derived_contexts = List(
      SelfDescribingData(contextKey1, json"""{"x": "y"}"""),
      SelfDescribingData(eventSpecKey, json"""{"id": "11111111-1111-1111-1111-111111111111"}"""),
      SelfDescribingData(eventSpecKey, json"""{"id": "22222222-2222-2222-2222-222222222222"}""")
    )

    val expectedKeyInferred1 = Metadata.MetadataEvent(
      schema = None,
      source = None,
      tracker = None,
      platform = None,
      scenarioId = Some("11111111-1111-1111-1111-111111111111"),
      scenarioVersion = None,
      eventSpecInferred = true,
      eventSpecValid = true
    )
    val expectedKeyInferred2 = expectedKeyInferred1.copy(scenarioId = Some("22222222-2222-2222-2222-222222222222"))
    val expectedKeyPlain = expectedKeyInferred1.copy(scenarioId = None, eventSpecInferred = false)
    val expectedValue = Metadata.EntitiesAndCount(Set(contextKey1, eventSpecKey), 1)

    val result = Metadata.extractsForBatch(List(input))
    result must beEqualTo(
      Map(
        expectedKeyInferred1 -> expectedValue,
        expectedKeyInferred2 -> expectedValue,
        expectedKeyPlain -> expectedValue
      )
    )
  }

  def e4g = {
    val eventSpecKey = SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/event_specification/jsonschema/1-0-0").toOption.get

    val input = new EnrichedEvent
    input.contexts = List(
      SelfDescribingData(eventSpecKey, json"""{"id": "18056e39-6147-4824-bd67-5f6f6f7e18ee", "version": 1}""")
    )

    val expectedKey = Metadata.MetadataEvent(
      schema = None,
      source = None,
      tracker = None,
      platform = None,
      scenarioId = Some("18056e39-6147-4824-bd67-5f6f6f7e18ee"),
      scenarioVersion = Some(1),
      eventSpecInferred = false,
      eventSpecValid = true
    )
    val expectedValue = Metadata.EntitiesAndCount(Set(eventSpecKey), 1)

    val result = Metadata.extractsForBatch(List(input))
    result must beEqualTo(Map(expectedKey -> expectedValue))
  }

  def e4h = {
    val eventSpecKey = SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/event_specification/jsonschema/1-0-0").toOption.get

    val input1 = new EnrichedEvent
    input1.derived_contexts = List(
      SelfDescribingData(eventSpecKey, json"""{"id": "18056e39-6147-4824-bd67-5f6f6f7e18ee"}""")
    )

    val input2 = new EnrichedEvent

    val expectedKeyInferred = Metadata.MetadataEvent(
      schema = None,
      source = None,
      tracker = None,
      platform = None,
      scenarioId = Some("18056e39-6147-4824-bd67-5f6f6f7e18ee"),
      scenarioVersion = None,
      eventSpecInferred = true,
      eventSpecValid = true
    )
    val expectedKeyPlain = expectedKeyInferred.copy(scenarioId = None, eventSpecInferred = false)

    val result = Metadata.extractsForBatch(List(input1, input2))
    result must beEqualTo(
      Map(
        expectedKeyInferred -> Metadata.EntitiesAndCount(Set(eventSpecKey), 1),
        expectedKeyPlain -> Metadata.EntitiesAndCount(Set(eventSpecKey), 2)
      )
    )
  }

  def e5 = {
    val contextKey1 = SchemaKey.fromUri("iglu:myvendor/mycontext1/jsonschema/1-0-42").toOption.get
    val contextKey2 = SchemaKey.fromUri("iglu:myvendor/mycontext2/jsonschema/1-0-42").toOption.get
    val contextKey3 = SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/event_specification/jsonschema/1-0-0").toOption.get

    val input = new EnrichedEvent
    input.contexts = List(
      SelfDescribingData(contextKey1, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey2, json"""{"x": "y"}"""),
      SelfDescribingData(contextKey3, json"""{"id": null}""")
    )

    val expectedKey = Metadata.MetadataEvent(
      schema = None,
      source = None,
      tracker = None,
      platform = None,
      scenarioId = None,
      scenarioVersion = None,
      eventSpecInferred = false,
      eventSpecValid = true
    )
    val expectedValue = Metadata.EntitiesAndCount(Set(contextKey1, contextKey2, contextKey3), 1)

    val result = Metadata.extractsForBatch(List(input))
    result must beEqualTo(Map(expectedKey -> expectedValue))
  }

  def e6 = {
    val input = new EnrichedEvent
    input.app_id = "myapp"

    val expectedKey = Metadata.MetadataEvent(
      schema = None,
      source = Some("myapp"),
      tracker = None,
      platform = None,
      scenarioId = None,
      scenarioVersion = None,
      eventSpecInferred = false,
      eventSpecValid = true
    )
    val expectedValue = Metadata.EntitiesAndCount(Set.empty, 5)

    val result = Metadata.extractsForBatch(List(input, input, input, input, input))
    result must beEqualTo(Map(expectedKey -> expectedValue))
  }

  def e7 = {
    val schemaKey1 = SchemaKey.fromUri("iglu:myvendor1/mycontext1/jsonschema/1-0-42").toOption.get
    val schemaKey2 = SchemaKey.fromUri("iglu:myvendor2/mycontext2/jsonschema/42-0-0").toOption.get

    val input1 = new EnrichedEvent
    input1.app_id = "myapp"
    input1.contexts = List(SelfDescribingData(schemaKey1, json"""{"x": "y"}"""))

    val input2 = new EnrichedEvent
    input2.app_id = "myapp"
    input2.contexts = List(SelfDescribingData(schemaKey2, json"""{"x": "y"}"""))

    val expectedKey = Metadata.MetadataEvent(
      schema = None,
      source = Some("myapp"),
      tracker = None,
      platform = None,
      scenarioId = None,
      scenarioVersion = None,
      eventSpecInferred = false,
      eventSpecValid = true
    )
    val expectedValue = Metadata.EntitiesAndCount(Set(schemaKey1, schemaKey2), 2)

    val result = Metadata.extractsForBatch(List(input1, input2))
    result must beEqualTo(Map(expectedKey -> expectedValue))
  }

  def e8 = {
    val unstructKey1 = SchemaKey.fromUri("iglu:myvendor1/myunstruct1/jsonschema/1-0-42").toOption.get
    val unstructKey2 = SchemaKey.fromUri("iglu:myvendor2/myunstruct2/jsonschema/42-0-0").toOption.get
    val contextKey1 = SchemaKey.fromUri("iglu:myvendor1/mycontext1/jsonschema/1-0-42").toOption.get
    val contextKey2 = SchemaKey.fromUri("iglu:myvendor2/mycontext2/jsonschema/42-0-0").toOption.get

    val input1 = new EnrichedEvent
    input1.app_id = "myapp"
    input1.contexts = List(SelfDescribingData(contextKey1, json"""{"x": "y"}"""))
    input1.unstruct_event = Some(SelfDescribingData(unstructKey1, json"""{"x": "y"}"""))

    val input2 = new EnrichedEvent
    input2.app_id = "myapp"
    input2.contexts = List(SelfDescribingData(contextKey2, json"""{"x": "y"}"""))
    input2.unstruct_event = Some(SelfDescribingData(unstructKey2, json"""{"x": "y"}"""))

    val expectedKey1 = Metadata.MetadataEvent(
      schema = Some(unstructKey1),
      source = Some("myapp"),
      tracker = None,
      platform = None,
      scenarioId = None,
      scenarioVersion = None,
      eventSpecInferred = false,
      eventSpecValid = true
    )
    val expectedKey2 = Metadata.MetadataEvent(
      schema = Some(unstructKey2),
      source = Some("myapp"),
      tracker = None,
      platform = None,
      scenarioId = None,
      scenarioVersion = None,
      eventSpecInferred = false,
      eventSpecValid = true
    )

    val expectedValue1 = Metadata.EntitiesAndCount(Set(contextKey1), 1)
    val expectedValue2 = Metadata.EntitiesAndCount(Set(contextKey2), 1)

    val result = Metadata.extractsForBatch(List(input1, input2))
    result must beEqualTo(Map(expectedKey1 -> expectedValue1, expectedKey2 -> expectedValue2))
  }

}
