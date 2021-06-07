/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.extractor

import cats.Id
import cats.implicits._

import io.circe.Json
import io.circe.parser.parse
import io.circe.literal._

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.badrows.Processor

import org.joda.time.DateTime

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.MiscEnrichments
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

class ExtractorEnrichmentSpec extends Specification {
  "process" should {
    "mutate an event adding necessary contexts without erasing" in {
      val enrichment = ExtractorEnrichment(Set(Extractable.MaxMind), false)

      val city = "Krasnoyarsk"
      val latitude = 56.0184f
      val longitude = 92.8672f
      val event = new EnrichedEvent
      event.setGeo_city(city)
      event.setGeo_latitude(latitude)
      event.setGeo_longitude(longitude)

      val expected = SelfDescribingData(
        MiscEnrichments.ContextsSchema,
        json"""[{
          "schema" : "iglu:com.maxmind/context/jsonschema/1-0-0",
          "data" : {
            "city" : "Krasnoyarsk",
            "latitude" : 56.0184,
            "longitude" : 92.8672
          }
        }]"""
      )

      val postProcessing = enrichment.process[Id](Processor("test-processor", "1.0.0"), ExtractorEnrichmentSpec.rawEvent, event).value
      val output = parse(event.derived_contexts).flatMap(_.as[SelfDescribingData[Json]])

      event.geo_city must beEqualTo(city)
      event.geo_latitude must beEqualTo(latitude)
      event.geo_longitude must beEqualTo(longitude)
      postProcessing must beRight
      output must beRight(expected)
    }

    "mutate an event adding necessary contexts with erasing" in {
      val enrichment = ExtractorEnrichment(Set(Extractable.MaxMind), true)

      val city = "Krasnoyarsk"
      val event = new EnrichedEvent
      event.setGeo_city(city)
      event.setGeo_latitude(56.0184f)
      event.setGeo_longitude(92.8672f)

      val expected = SelfDescribingData(
        MiscEnrichments.ContextsSchema,
        json"""[{
          "schema" : "iglu:com.maxmind/context/jsonschema/1-0-0",
          "data" : {
            "city" : "Krasnoyarsk",
            "latitude" : 56.0184,
            "longitude" : 92.8672
          }
        }]"""
      )

      val postProcessing = enrichment.process[Id](Processor("test-processor", "1.0.0"), ExtractorEnrichmentSpec.rawEvent, event).value
      val output = parse(event.derived_contexts).flatMap(_.as[SelfDescribingData[Json]])

      event.geo_city must beNull
      event.geo_latitude must beNull
      event.geo_longitude must beNull
      postProcessing must beRight
      output must beRight(expected)
    }

    "append contexts into existing derived_contexts field" in {
      val enrichment = ExtractorEnrichment(Set(Extractable.MaxMind), false)

      val city = "Krasnoyarsk"
      val latitude = 56.0184f
      val longitude = 92.8672f
      val event = new EnrichedEvent
      val priorContext = json"""{"schema":"iglu:com.acme/one/jsonschema/1-0-0","data":{"value":1}}"""
      event.setGeo_city(city)
      event.setGeo_latitude(latitude)
      event.setGeo_longitude(longitude)
      event.setDerived_contexts(s"""{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[${priorContext.noSpaces}]}""")

      val expected = SelfDescribingData(
        MiscEnrichments.ContextsSchema,
        json"""[
          $priorContext,
          {
            "schema" : "iglu:com.maxmind/context/jsonschema/1-0-0",
            "data" : {
              "city" : "Krasnoyarsk",
              "latitude" : 56.0184,
              "longitude" : 92.8672
            }
        }]"""
      )

      val postProcessing = enrichment.process[Id](Processor("test-processor", "1.0.0"), ExtractorEnrichmentSpec.rawEvent, event).value
      val output = parse(event.derived_contexts).flatMap(_.as[SelfDescribingData[Json]])

      event.geo_city must beEqualTo(city)
      event.geo_latitude must beEqualTo(latitude)
      event.geo_longitude must beEqualTo(longitude)
      postProcessing must beRight
      output must beRight(expected)
    }
  }
}

object ExtractorEnrichmentSpec {
  val api = CollectorPayload.Api("com.snowplowanalytics.snowplow", "tp2")
  val source = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
  val context = CollectorPayload.Context(
    DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
    "37.157.33.123".some,
    None,
    None,
    Nil,
    None
  )
  val rawEvent = RawEvent(api, Map.empty, None, source, context)
}