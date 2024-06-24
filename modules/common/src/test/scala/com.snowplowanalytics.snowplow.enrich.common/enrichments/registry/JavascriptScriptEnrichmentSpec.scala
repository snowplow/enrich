/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common

package enrichments.registry

import io.circe.literal._

import org.specs2.Specification

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

class JavascriptScriptEnrichmentSpec extends Specification {
  def is = s2"""
  Javascript enrichment should fail if the function isn't valid                      $e1
  Javascript enrichment should fail if the function doesn't return an array          $e2
  Javascript enrichment should fail if the function doesn't return an array of SDJs  $e3
  Javascript enrichment should be able to access the fields of the enriched event    $e4
  Javascript enrichment should be able to update the fields of the enriched event    $e5
  Javascript enrichment should be able to throw an exception                         $e6
  Javascript enrichment should be able to return no new context                      $e7
  Javascript enrichment should be able to return 2 new contexts                      $e8
  Javascript enrichment should be able to proceed without return statement           $e9
  Javascript enrichment should be able to proceed with return null                   $e10
  Javascript enrichment should be able to update the fields without return statement $e11
  Javascript enrichment should be able to utilize the passed parameters              $e12
  Javascript enrichment should be able to utilize the headers                        $e13
  """

  val schemaKey =
    SchemaKey("com.snowplowanalytics.snowplow", "javascript_script_config", "jsonschema", SchemaVer.Full(1, 0, 0))

  def e1 =
    JavascriptScriptEnrichment(schemaKey, "[").process(buildEnriched(), List.empty) must beLeft(
      failureContains(_: FailureDetails.EnrichmentFailure, "Error compiling")
    )

  def e2 = {
    val function = s"""
      function process(event) {
        return { foo: "bar" }
      }"""
    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty) must beLeft(
      failureContains(_: FailureDetails.EnrichmentFailure, "not read as an array")
    )
  }

  def e3 = {
    val function = s"""
      function process(event) {
        return [ { foo: "bar" } ]
      }"""
    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty) must beLeft(
      failureContains(_: FailureDetails.EnrichmentFailure, "not self-desribing")
    )
  }

  def e4 = {
    val appId = "greatApp"
    val function = s"""
      function process(event) {
        return [ { schema: "iglu:com.acme/foo/jsonschema/1-0-0",
          data:   { appId: event.getApp_id() }
        } ];
      }"""
    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(appId), List.empty) must beRight.like {
      case List(sdj) if sdj.data.noSpaces.contains(appId) => true
      case _ => false
    }
  }

  def e5 = {
    val appId = "greatApp"
    val enriched = buildEnriched(appId)
    val newAppId = "evenBetterApp"
    val function = s"""
      function process(event) {
        event.setApp_id("$newAppId")
        return [ { schema: "iglu:com.acme/foo/jsonschema/1-0-0",
          data:   { foo: "bar" }
        } ];
      }"""
    JavascriptScriptEnrichment(schemaKey, function).process(enriched, List.empty)
    enriched.app_id must beEqualTo(newAppId)
  }

  def e6 = {
    val function = s"""
      function process(event) {
        throw "Error"
      }"""
    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty) must beLeft(
      failureContains(_: FailureDetails.EnrichmentFailure, "Error during execution")
    )
  }

  def e7 = {
    val function = s"""
      function process(event) {
        return [ ];
      }"""
    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty) must beRight
  }

  def e8 = {
    val function = s"""
      function process(event) {
        return [ { schema: "iglu:com.acme/foo/jsonschema/1-0-0",
          data:   { hello: "world" }
        }, { schema: "iglu:com.acme/bar/jsonschema/1-0-0",
          data:   { hello: "world" }
        } ];
      }"""

    val context1 =
      SelfDescribingData(
        SchemaKey("com.acme", "foo", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"hello":"world"}"""
      )
    val context2 =
      SelfDescribingData(
        SchemaKey("com.acme", "bar", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"hello":"world"}"""
      )
    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty) must beRight.like {
      case List(c1, c2) if c1 == context1 && c2 == context2 => true
      case _ => false
    }
  }

  def e9 = {
    val function = s"""
      function process(event) {
        var a = 42     // no-op
      }"""

    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty) must beRight(Nil)
  }

  def e10 = {
    val function = s"""
      function process(event) {
        return null
      }"""

    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty) must beRight(Nil)
  }

  def e11 = {
    val appId = "greatApp"
    val enriched = buildEnriched(appId)
    val newAppId = "evenBetterApp"
    val function = s"""
      function process(event) {
        event.setApp_id("$newAppId")
      }"""
    JavascriptScriptEnrichment(schemaKey, function).process(enriched, List.empty)
    enriched.app_id must beEqualTo(newAppId)
  }

  def e12 = {
    val appId = "greatApp"
    val enriched = buildEnriched(appId)
    val params = json"""{"foo": "bar", "nested": {"foo": "newId"}}""".asObject.get
    val function =
      s"""
      function process(event, params) {
        event.setApp_id(params.nested.foo)
      }"""
    JavascriptScriptEnrichment(schemaKey, function, params).process(enriched, List.empty)
    enriched.app_id must beEqualTo("newId")
  }

  def e13 = {
    val appId = "greatApp"
    val enriched = buildEnriched(appId)
    val function =
      s"""
      function process(event, params, headers) {
        for (header of headers) {
          const jwt = header.match(/X-JWT:(.+)/i)
          if (jwt) {
            event.setApp_id(jwt[1].trim())
          }
        }
      }"""

    JavascriptScriptEnrichment(schemaKey, function).process(enriched, List.empty)
    enriched.app_id must beEqualTo("greatApp")

    JavascriptScriptEnrichment(schemaKey, function).process(enriched, List("x-jwt: newId"))
    enriched.app_id must beEqualTo("newId")
  }

  def buildEnriched(appId: String = "my super app"): EnrichedEvent = {
    val e = new EnrichedEvent()
    e.platform = "server"
    e.app_id = appId
    e
  }

  def failureContains(failure: FailureDetails.EnrichmentFailure, pattern: String): Boolean =
    failure match {
      case FailureDetails.EnrichmentFailure(_, FailureDetails.EnrichmentFailureMessage.Simple(msg)) if msg.contains(pattern) => true
      case _ => false
    }
}
