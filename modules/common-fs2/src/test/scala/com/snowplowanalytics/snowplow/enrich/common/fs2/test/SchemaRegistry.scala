/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.fs2.test

import io.circe.Json
import io.circe.literal._

import com.snowplowanalytics.iglu.core.SelfDescribingSchema
import com.snowplowanalytics.iglu.core.circe.implicits._

/**
 * In-memory test registry to avoid unnecessary HTTP and FS IO. All schemas used in [[TestEnvironment]]
 * Iglu Client
 */
object SchemaRegistry {
  val acmeTest: SelfDescribingSchema[Json] =
    json"""{
      "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
      "self": {
        "vendor": "com.acme",
        "name": "test",
        "format": "jsonschema",
        "version": "1-0-1"
      },
      "properties": {
        "path": {
          "properties": {
            "id": {
              "type": "integer"
            }
          }
        }
      }
    }"""

  val acmeOutput: SelfDescribingSchema[Json] =
    json"""{
      "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
      "self": {
        "vendor": "com.acme",
        "name": "output",
        "format": "jsonschema",
        "version": "1-0-0"
      },
      "properties": {
        "output": {
          "type": "string"
        }
      }
    }"""

  // Defined on Iglu Central
  val unstructEvent: SelfDescribingSchema[Json] =
    json"""{
	    "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
	    "self": {
		    "vendor": "com.snowplowanalytics.snowplow",
		    "name": "unstruct_event",
		    "format": "jsonschema",
		    "version": "1-0-0"
	    },
	    "type": "object",
	    "properties": {
	    	"schema": {
	    		"type": "string",
	    		"pattern": "^iglu:[a-zA-Z0-9-_.]+/[a-zA-Z0-9-_]+/[a-zA-Z0-9-_]+/[0-9]+-[0-9]+-[0-9]+$$"
	    	},
	    	"data": {}
	    },
	    "required": ["schema", "data"],
	    "additionalProperties": false
    }"""

  // Defined on Iglu Central
  val contexts: SelfDescribingSchema[Json] =
    json"""{
	    "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
	    "self": {
	    	"vendor": "com.snowplowanalytics.snowplow",
	    	"name": "contexts",
	    	"format": "jsonschema",
	    	"version": "1-0-1"
	    },
	    "type": "array",
	    "items": {
	    	"type": "object",
	    	"properties": {
	    		"schema": {
	    			"type": "string",
	    			"pattern": "^iglu:[a-zA-Z0-9-_.]+/[a-zA-Z0-9-_]+/[a-zA-Z0-9-_]+/[0-9]+-[0-9]+-[0-9]+$$"
	    		},
	    		"data": {}
	    	},
	    	"required": ["schema", "data"],
	    	"additionalProperties": false
	    }
    }"""

  // Defined on Iglu Central
  val geolocationContext: SelfDescribingSchema[Json] =
    json"""{
      	"$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
      	"self": {
      		"vendor": "com.snowplowanalytics.snowplow",
      		"name": "geolocation_context",
      		"format": "jsonschema",
      		"version": "1-1-0"
      	},
      	"type": "object",
      	"properties": {
      		"latitude":                  { "type": "number", "minimum": -90, "maximum": 90 },
      		"longitude":                 { "type": "number", "minimum": -180, "maximum": 180 },
      		"latitudeLongitudeAccuracy": { "type": ["number", "null"] },
      		"altitude":                  { "type": ["number", "null"] },
      		"altitudeAccuracy":          { "type": ["number", "null"] },
      		"bearing":                   { "type": ["number", "null"] },
      		"speed":                     { "type": ["number", "null"] },
      		"timestamp":                 { "type": ["integer", "null"] }
      	},
      	"required": ["latitude", "longitude"],
      	"additionalProperties": false
      }"""

  // Defined on Iglu Central
  val iabAbdRobots: SelfDescribingSchema[Json] =
    json"""{
      "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
      "self": {
          "vendor": "com.iab.snowplow",
          "name": "spiders_and_robots",
          "format": "jsonschema",
          "version": "1-0-0"
      },
      "type": "object",
      "properties": {
          "spiderOrRobot": {"type": "boolean" },
          "category":      {"enum": ["SPIDER_OR_ROBOT", "ACTIVE_SPIDER_OR_ROBOT", "INACTIVE_SPIDER_OR_ROBOT", "BROWSER"]},
          "reason":        {"enum": ["FAILED_IP_EXCLUDE", "FAILED_UA_INCLUDE", "FAILED_UA_EXCLUDE", "PASSED_ALL"]},
          "primaryImpact": {"enum": ["PAGE_IMPRESSIONS", "AD_IMPRESSIONS", "PAGE_AND_AD_IMPRESSIONS", "UNKNOWN", "NONE"]}
      },
      "required": ["spiderOrRobot", "category", "reason", "primaryImpact"],
      "additionalProperties": false
    }"""

  val yauaaContext: SelfDescribingSchema[Json] =
    json"""{
      "$$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
      "self": {
          "vendor": "nl.basjes",
          "name": "yauaa_context",
          "format": "jsonschema",
          "version": "1-0-2"
      },
      "type": "object",
      "properties": {
          "deviceClass":                     {"enum":["Desktop","Anonymized","Unknown","UNKNOWN","Mobile","Tablet","Phone","Watch","Virtual Reality","eReader","Set-top box","TV","Game Console","Handheld Game Console","Voice","Robot","Robot Mobile","Spy","Hacker"]},
          "deviceName":                      {"type":"string","maxLength": 100 },
          "deviceBrand":                     {"type":"string","maxLength": 50 },
          "deviceCpu":                       {"type":"string","maxLength": 50 },
          "deviceCpuBits":                   {"type":"string","maxLength": 20 },
          "deviceFirmwareVersion":           {"type":"string","maxLength": 100 },
          "deviceVersion":                   {"type":"string","maxLength": 100 },
          "operatingSystemClass":            {"enum":["Desktop","Mobile","Cloud","Embedded","Game Console","Hacker","Anonymized","Unknown"] },
          "operatingSystemName":             {"type":"string","maxLength": 100 },
          "operatingSystemVersion":          {"type":"string","maxLength": 50 },
          "operatingSystemNameVersion":      {"type":"string","maxLength": 150 },
          "operatingSystemVersionBuild":     {"type":"string","maxLength": 100 },
          "layoutEngineClass":               {"enum":["Browser", "Mobile App", "Hacker", "Robot", "Unknown"] },
          "layoutEngineName":                {"type":"string","maxLength": 100 },
          "layoutEngineVersion":             {"type":"string","maxLength": 50 },
          "layoutEngineVersionMajor":        {"type":"string","maxLength": 20 },
          "layoutEngineNameVersion":         {"type":"string","maxLength": 150 },
          "layoutEngineNameVersionMajor":    {"type":"string","maxLength": 120 },
          "layoutEngineBuild":               {"type":"string","maxLength": 100 },
          "agentClass":                      {"enum":["Browser", "Browser Webview", "Mobile App", "Robot", "Robot Mobile", "Cloud Application", "Email Client", "Voice", "Special", "Testclient", "Hacker", "Unknown"] },
          "agentName":                       {"type":"string","maxLength": 100 },
          "agentVersion":                    {"type":"string","maxLength": 100 },
          "agentVersionMajor":               {"type":"string","maxLength": 20 },
          "agentNameVersion":                {"type":"string","maxLength": 200 },
          "agentNameVersionMajor":           {"type":"string","maxLength": 120 },
          "agentBuild":                      {"type":"string","maxLength": 100 },
          "agentLanguage":                   {"type":"string","maxLength": 50 },
          "agentLanguageCode":               {"type":"string","maxLength": 20 },
          "agentInformationEmail":           {"type":"string","format": "email" },
          "agentInformationUrl":             {"type":"string"},
          "agentSecurity":                   {"type":"string","enum":["Weak security", "Strong security", "Unknown", "Hacker"] },
          "agentUuid":                       {"type":"string"},
          "webviewAppName":                  {"type":"string"},
          "webviewAppVersion":               {"type":"string"},
          "webviewAppVersionMajor":          {"type":"string","maxLength":50},
          "webviewAppNameVersionMajor":      {"type":"string","maxLength":50},
          "facebookCarrier":                 {"type":"string"},
          "facebookDeviceClass":             {"type":"string","maxLength":1024},
          "facebookDeviceName":              {"type":"string","maxLength":1024},
          "facebookDeviceVersion":           {"type":"string"},
          "facebookFBOP":                    {"type":"string"},
          "facebookFBSS":                    {"type":"string"},
          "facebookOperatingSystemName":     {"type":"string"},
          "facebookOperatingSystemVersion":  {"type":"string"},
          "anonymized":                      {"type":"string"},
          "hackerAttackVector":              {"type":"string"},
          "hackerToolkit":                   {"type":"string"},
          "koboAffiliate":                   {"type":"string"},
          "koboPlatformId":                  {"type":"string"},
          "iECompatibilityVersion":          {"type":"string","maxLength":100},
          "iECompatibilityVersionMajor":     {"type":"string","maxLength":50},
          "iECompatibilityNameVersion":      {"type":"string","maxLength":50},
          "iECompatibilityNameVersionMajor": {"type":"string","maxLength":70},
          "carrier":                         {"type":"string"},
          "gSAInstallationID":               {"type":"string"},
          "networkType":                     {"type":"string"},
          "operatingSystemNameVersionMajor": {"type":"string"},
          "operatingSystemVersionMajor":     {"type":"string"}
      },
      "required": ["deviceClass"],
      "additionalProperties": false
    }"""

  private[test] implicit def jsonToSchema(json: Json): SelfDescribingSchema[Json] =
    SelfDescribingSchema.parse(json).getOrElse(throw new IllegalStateException("InMemory SchemaRegistry JSON cannot be parsed as schema"))
}
