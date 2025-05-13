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
package com.snowplowanalytics.snowplow.enrich.common

package enrichments.registry

import io.circe.Json
import io.circe.literal._

import org.specs2.Specification
import org.specs2.matcher.MatchResult

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import JavascriptScriptEnrichment.Result

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
  Javascript enrichment should drop event when dropped method is called              $e14
  Javascript enrichment should be able to set 'erase derived contexts' flag to true  $e15
  Javascript enrichment should be able to manipulate unstruct_event                  $e16
  Javascript enrichment should be able to manipulate context                         $e17
  Javascript enrichment should fail if the unstruct_event is set to invalid json     $e18
  Javascript enrichment should fail if the contexts field is set to invalid json     $e19
  """

  val schemaKey =
    SchemaKey("com.snowplowanalytics.snowplow", "javascript_script_config", "jsonschema", SchemaVer.Full(1, 0, 0))

  def e1 =
    JavascriptScriptEnrichment(schemaKey, "[").process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      failureContains("Error compiling")
    }

  def e2 = {
    val function = s"""
      function process(event) {
        return { foo: "bar" }
      }"""
    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike(
      failureContains("not read as an array")
    )
  }

  def e3 = {
    val function = s"""
      function process(event) {
        return [ { foo: "bar" } ]
      }"""
    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike(
      failureContains("not self-desribing")
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
    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(appId), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      case Result.Success(List(sdj), false) if sdj.data.noSpaces.contains(appId) => ok
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
    JavascriptScriptEnrichment(schemaKey, function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth)
    enriched.app_id must beEqualTo(newAppId)
  }

  def e6 = {
    val function = s"""
      function process(event) {
        throw "Error"
      }"""
    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike(
      failureContains("Error during execution")
    )
  }

  def e7 = {
    val function = s"""
      function process(event) {
        return [ ];
      }"""
    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      case Result.Success(_, false) => ok
    }
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
    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      case Result.Success(List(c1, c2), false) if c1 == context1 && c2 == context2 => ok
    }
  }

  def e9 = {
    val function = s"""
      function process(event) {
        var a = 42     // no-op
      }"""

    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beEqualTo(
      Result.Success(Nil, false)
    )
  }

  def e10 = {
    val function = s"""
      function process(event) {
        return null
      }"""

    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beEqualTo(
      Result.Success(Nil, false)
    )
  }

  def e11 = {
    val appId = "greatApp"
    val enriched = buildEnriched(appId)
    val newAppId = "evenBetterApp"
    val function = s"""
      function process(event) {
        event.setApp_id("$newAppId")
      }"""
    JavascriptScriptEnrichment(schemaKey, function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth)
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
    JavascriptScriptEnrichment(schemaKey, function, params).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth)
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

    JavascriptScriptEnrichment(schemaKey, function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth)
    enriched.app_id must beEqualTo("greatApp")

    JavascriptScriptEnrichment(schemaKey, function).process(enriched, List("x-jwt: newId"), SpecHelpers.DefaultMaxJsonDepth)
    enriched.app_id must beEqualTo("newId")
  }

  def e14 = {
    val function = s"""
      function process(event) {
        event.drop()
      }"""
    JavascriptScriptEnrichment(schemaKey, function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beEqualTo(
      Result.Dropped
    )
  }

  def e15 = {
    val function = s"""
      function process(event) {
        event.eraseDerived_contexts()
      }"""
    val enriched = buildEnriched()
    (JavascriptScriptEnrichment(schemaKey, function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth) must beEqualTo(
      Result.Success(List.empty, useDerivedContextsFromJsEnrichmentOnly = true)
    )) and ((enriched.use_derived_contexts_from_js_enrichment_only: Boolean) must beTrue)
  }

  def e16 = {
    val enriched = buildEnriched()

    val clientSession = SelfDescribingData[Json](
      SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-1").toOption.get,
      json"""{
        "userId": "some-fancy-user-session-id",
        "sessionId": "42c8a55b-c0c2-4749-b9ac-09bb0d17d000",
        "sessionIndex": 1,
        "previousSessionId": null,
        "storageMechanism": "COOKIE_1"
      }"""
    )
    enriched.unstruct_event = Some(clientSession)

    val function =
      s"""
      function process(event) {
        const ue = JSON.parse(event.getUnstruct_event())
        ue.data.schema = "iglu:modifiedvendor/modifiedname/jsonschema/1-0-0"
        ue.data.data.userId = "some-modified-user-id"
        event.setUnstruct_event(JSON.stringify(ue))
        return []
      }"""

    val expectedSchemaKey = SchemaKey.fromUri("iglu:modifiedvendor/modifiedname/jsonschema/1-0-0").toOption.get

    JavascriptScriptEnrichment(schemaKey, function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth)
    enriched.unstruct_event must beSome.like {
      case sdj: SelfDescribingData[Json] =>
        List(
          sdj.schema must beEqualTo(expectedSchemaKey),
          sdj.data.hcursor.get[String]("userId") must beRight("some-modified-user-id")
        ).reduce(_ and _)
    }
  }

  def e17 = {
    val enriched = buildEnriched()

    val clientSession = SelfDescribingData[Json](
      SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-1").toOption.get,
      json"""{
        "userId": "some-fancy-user-session-id",
        "sessionId": "42c8a55b-c0c2-4749-b9ac-09bb0d17d000",
        "sessionIndex": 1,
        "previousSessionId": null,
        "storageMechanism": "COOKIE_1"
      }"""
    )
    enriched.contexts = List(clientSession)

    val function =
      s"""
      function process(event) {
        const co = JSON.parse(event.getContexts())
        co.data[0].schema = "iglu:modifiedvendor/modifiedname/jsonschema/1-0-0"
        co.data[0].data.userId = "some-modified-user-id"
        event.setContexts(JSON.stringify(co))
        return []
      }"""

    val expectedSchemaKey = SchemaKey.fromUri("iglu:modifiedvendor/modifiedname/jsonschema/1-0-0").toOption.get

    JavascriptScriptEnrichment(schemaKey, function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth)
    enriched.contexts.lift(0) must beSome.like {
      case sdj: SelfDescribingData[Json] =>
        List(
          sdj.schema must beEqualTo(expectedSchemaKey),
          sdj.data.hcursor.get[String]("userId") must beRight("some-modified-user-id")
        ).reduce(_ and _)
    }
  }

  def e18 = {
    val enriched = buildEnriched()
    val function =
      s"""
      function process(event) {
        const ue = {foo: "bar"}
        event.setUnstruct_event(JSON.stringify(ue))
        return []
      }"""

    JavascriptScriptEnrichment(schemaKey, function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      failureContains("invalid unstruct_event json")
    }
  }

  def e19 = {
    val enriched = buildEnriched()
    val function =
      s"""
      function process(event) {
        const co = {foo: "bar"}
        event.setContexts(JSON.stringify(co))
        return []
      }"""

    JavascriptScriptEnrichment(schemaKey, function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      failureContains("invalid contexts json")
    }
  }

  def buildEnriched(appId: String = "my super app"): EnrichedEvent = {
    val e = new EnrichedEvent()
    e.platform = "server"
    e.app_id = appId
    e
  }

  def failureContains(pattern: String): PartialFunction[Result, MatchResult[Any]] = {
    case Result.Failure(FailureDetails.EnrichmentFailure(_, FailureDetails.EnrichmentFailureMessage.Simple(msg)))
        if msg.contains(pattern) =>
      ok
  }
}
