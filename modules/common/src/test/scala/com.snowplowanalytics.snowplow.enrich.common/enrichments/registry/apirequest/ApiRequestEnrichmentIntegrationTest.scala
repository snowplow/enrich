/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest

import cats.effect.IO
import cats.effect.testing.specs2.CatsIO

import io.circe._
import io.circe.literal._

import org.specs2.Specification
import org.specs2.matcher.Matcher
import org.specs2.matcher.ValidatedMatchers

import com.snowplowanalytics.iglu.client.CirceValidator

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers

object ApiRequestEnrichmentIntegrationTest {
  def continuousIntegration: Boolean =
    sys.env.get("CI") match {
      case Some("true") => true
      case _ => false
    }
}

import ApiRequestEnrichmentIntegrationTest._
class ApiRequestEnrichmentIntegrationTest extends Specification with ValidatedMatchers with CatsIO {
  def is =
    skipAllUnless(continuousIntegration) ^
      s2"""
  This is a integration test for the ApiRequestEnrichment
  Basic Case                                      $e1
  POST, Auth, JSON inputs, cache, several outputs $e2
  API output with number field                    $e3
  """

  object IntegrationTests {
    val configuration = json"""{
      "vendor": "com.snowplowanalytics.snowplow.enrichments",
      "name": "api_request_enrichment_config",
      "enabled": true,
      "parameters": {
        "inputs": [
          {
            "key": "user",
            "pojo": {
              "field": "user_id"
            }
          },
          {
            "key": "client",
            "pojo": {
              "field": "app_id"
            }
          }
        ],
        "api": {
          "http": {
            "method": "GET",
            "uri": "http://localhost:8001/guest/api/{{client}}/{{user}}?format=json",
           "timeout": 5000,
            "authentication": {}
          }
        },
        "outputs": [{
          "schema": "iglu:com.acme/unauth/jsonschema/1-0-0",
          "json": {
            "jsonPath": "$$"
          }
        }],
        "cache": {
          "size": 3000,
          "ttl": 60
        }
      }
    }"""

    val correctResultContext =
      SelfDescribingData(
        SchemaKey("com.acme", "unauth", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"path": "/guest/api/lookup-test/snowplower?format=json", "message": "unauthorized", "method": "GET"}"""
      )

    val configuration2 = json"""{
      "vendor": "com.snowplowanalytics.snowplow.enrichments",
      "name": "api_request_enrichment_config",
      "enabled": true,
      "parameters": {
        "inputs": [
        {
          "key": "user",
          "pojo": {
            "field": "user_id"
          }
        },
        {
          "key": "client",
          "pojo": {
            "field": "app_id"
         }
        },
        {
          "key": "jobflow",
          "json": {
            "field": "unstruct_event",
            "jsonPath": "$$.jobflow_id",
            "schemaCriterion": "iglu:com.snowplowanalytics.monitoring.batch/emr_job_status/jsonschema/*-*-*"
          }
        },
        {
         "key": "latitude",
         "json": {
           "field": "contexts",
           "jsonPath": "$$.latitude",
           "schemaCriterion": "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0"
        }
        },
        {
          "key": "datetime",
          "json": {
            "field": "derived_contexts",
            "jsonPath": "$$.dt",
            "schemaCriterion": "iglu:org.openweathermap/weather/jsonschema/1-*-*"
          }
        }],
        "api": {
          "http": {
            "method": "POST",
            "uri": "http://localhost:8000/api/{{client}}/{{user}}/{{ jobflow }}/{{latitude}}?date={{ datetime }}",
           "timeout": 5000,
            "authentication": {
              "httpBasic": {
                "username": "snowplower",
                "password": "supersecret"
             }
           }
          }
        },
        "outputs": [{
          "schema": "iglu:com.acme/user/jsonschema/1-0-0",
          "json": {
            "jsonPath": "$$.data.lookupArray[0]"
          }
        }, {
          "schema": "iglu:com.acme/onlypath/jsonschema/1-0-0",
          "json": {
            "jsonPath": "$$.data.lookupArray[1]"
          }
        }],
        "cache": {
          "size": 3000,
          "ttl": 60
        }
      }
    }"""

    // NOTE: akka-http 1.0 was sending "2014-11-10T08:38:30.000Z" as is with ':', this behavior was changed in 2.0

    val correctResultContext2 =
      SelfDescribingData(
        SchemaKey("com.acme", "user", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{
          "path": "/api/lookup+test/snowplower/j-ZKIY4CKQRX72/32.1?date=2014-11-10T08%3A38%3A30.000Z",
          "method": "POST",
          "auth_header": "snowplower:supersecret",
          "request": 1
        }"""
      )

    val correctResultContext3 =
      SelfDescribingData(
        SchemaKey("com.acme", "onlypath", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"path": "/api/lookup+test/snowplower/j-ZKIY4CKQRX72/32.1?date=2014-11-10T08%3A38%3A30.000Z", "request": 1}"""
      )

    // Usual self-describing instance
    val weatherContext =
      SelfDescribingData(
        SchemaKey("org.openweathermap", "weather", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{
          "clouds": {
              "all": 0
          },
          "dt": "2014-11-10T08:38:30.000Z",
          "main": {
              "grnd_level": 1021.91,
              "humidity": 90,
              "pressure": 1021.91,
              "sea_level": 1024.77,
              "temp": 301.308,
              "temp_max": 301.308,
              "temp_min": 301.308
          },
          "weather": [ { "description": "Sky is Clear", "icon": "01d", "id": 800, "main": "Clear" } ],
          "wind": {
              "deg": 190.002,
              "speed": 4.39
          }
        }"""
      )

    // JsonSchemaPair built by Shredder
    val customContexts = SelfDescribingData(
      SchemaKey(
        "com.snowplowanalytics.snowplow",
        "geolocation_context",
        "jsonschema",
        SchemaVer.Full(1, 1, 0)
      ),
      json"""{"latitude": 32.1, "longitude": 41.1}"""
    )

    // JsonSchemaPair built by Shredder
    val unstructEvent = SelfDescribingData(
      SchemaKey(
        "com.snowplowanalytics.monitoring.batch",
        "emr_job_status",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ),
      json"""{"name": "Snowplow ETL", "jobflow_id": "j-ZKIY4CKQRX72", "state": "RUNNING", "created_at": "2016-01-21T13:14:10.193+03:00"}"""
    )

    val configuration3 = json"""{
      "vendor": "com.snowplowanalytics.snowplow.enrichments",
      "name": "api_request_enrichment_config",
      "enabled": true,
      "parameters": {
        "inputs": [
          {
            "key": "ip",
            "pojo": {
              "field": "user_ipaddress"
            }
          }
        ],
        "api": {
          "http": {
            "method": "GET",
            "uri": "http://localhost:8001/geo/{{ip}}?format=json",
            "timeout": 5000,
            "authentication": {}
          }
        },
        "outputs": [{
          "schema": "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0",
          "json": {
            "jsonPath": "$$"
          }
        }],
        "cache": {
          "size": 3000,
          "ttl": 60
        }
      }
    }"""

    val correctResultContext4 =
      SelfDescribingData(
        SchemaKey("com.snowplowanalytics.snowplow", "geolocation_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"latitude":32.234,"longitude":33.564}"""
      )

    val schema =
      json"""{ "type": "object", "properties": { "latitude": { "type": [ "number" ] }, "longitude": { "type": [ "number" ] } }, "additionalProperties": false }"""
  }

  val SCHEMA_KEY =
    SchemaKey(
      "com.snowplowanalytics.snowplow.enrichments",
      "api_request_enrichment_config",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )

  /**
   * Helper matcher to print JSON
   */
  def beJson(expected: SelfDescribingData[Json]): Matcher[SelfDescribingData[Json]] = { actual: SelfDescribingData[Json] =>
    val result = actual == expected
    val schemaMatch = actual.schema == expected.schema
    val dataMatch = actual.data == expected.data
    val message =
      if (schemaMatch)
        s"Schema ${actual.schema} matches, data doesn't.\nActual data: ${actual.data.spaces2}\nExpected data: ${expected.data.spaces2}"
      else if (dataMatch)
        s"Data payloads match, schemas don't.\nActual schema: ${actual.schema.toSchemaUri}\nExpected schema: ${expected.schema.toSchemaUri}"
      else "actual:\n" + actual + "\n expected:\n" + expected + "\n"
    (result, message)
  }

  def e1 = {
    val config = ApiRequestEnrichment.parse(IntegrationTests.configuration, SCHEMA_KEY).toOption.get
    SpecHelpers.httpClient.use { http =>
      for {
        enrichment <- config.enrichment[IO](http)
        event = {
          val e = new EnrichedEvent
          e.setApp_id("lookup-test")
          e.setUser_id("snowplower")
          e
        }
        context <- enrichment.lookup(event, Nil, Nil, None)
      } yield context must beValid.like {
        case context =>
          context must contain(IntegrationTests.correctResultContext) and (context must have size 1)
      }
    }
  }

  def e2 = {
    val config = ApiRequestEnrichment.parse(IntegrationTests.configuration2, SCHEMA_KEY).toOption.get
    SpecHelpers.httpClient.use { http =>
      for {
        enrichment <- config.enrichment[IO](http)
        event = {
          val e = new EnrichedEvent
          e.setApp_id("lookup test")
          e.setUser_id("snowplower")
          e
        }
        // Fill cache
        _ <- enrichment.lookup(
               event,
               List(IntegrationTests.weatherContext),
               List(IntegrationTests.customContexts),
               Some(IntegrationTests.unstructEvent)
             )
        _ <- enrichment.lookup(
               event,
               List(IntegrationTests.weatherContext),
               List(IntegrationTests.customContexts),
               Some(IntegrationTests.unstructEvent)
             )
        context <- enrichment.lookup(
                     event,
                     List(IntegrationTests.weatherContext),
                     List(IntegrationTests.customContexts),
                     Some(IntegrationTests.unstructEvent)
                   )
      } yield context must beValid.like {
        case contexts =>
          contexts must contain(
            beJson(IntegrationTests.correctResultContext3),
            beJson(IntegrationTests.correctResultContext2)
          ) and (contexts must have size 2)
      }
    }
  }

  def e3 = {
    val config = ApiRequestEnrichment.parse(IntegrationTests.configuration3, SCHEMA_KEY).toOption.get
    SpecHelpers.httpClient.use { http =>
      for {
        enrichment <- config.enrichment[IO](http)
        event = {
          val e = new EnrichedEvent
          e.setUser_ipaddress("127.0.0.1")
          e
        }
        context <- enrichment.lookup(event, Nil, Nil, None)
      } yield context must beValid.like {
        case contexts =>
          (contexts must have size 1) and (contexts must contain(IntegrationTests.correctResultContext4)) and
            (CirceValidator.validate(contexts.head.data, IntegrationTests.schema) must beRight)
      }
    }
  }
}
