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

import io.circe.{Json, JsonObject}
import io.circe.literal._

import org.specs2.Specification
import org.specs2.matcher.MatchResult

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import JavascriptScriptEnrichment.Result

class JavascriptScriptEnrichmentSpec extends Specification {
  import JavascriptScriptEnrichmentSpec._

  def is = s2"""
  Processing should fail if the function isn't valid and exitOnCompileError is false $e1_1
  Init should fail if the function isn't valid and exitOnCompileError is true        $e1_2
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
  Javascript enrichment should run successfully with Java packages                   $e20
  Class filtering should block "new <class>" when class is not in allow list         $e21
  Class filtering should allow "new <class>" when class is in allow list             $e22
  Class filtering should block "Java.type(<class>)" when class is not in allow list  $e23
  Class filtering should allow "Java.type(<class>)" when class is in allow list      $e24
  Class filtering should block static methods when class is not in allow list        $e25
  Class filtering should allow static methods when class in allow list               $e26
  Class filtering should allow classes resulting from using allowed classes          $e27
  Wildcard pattern "*" should allow all classes                                      $e28
  Wildcard pattern "java.net.*" should allow URL and URI classes                     $e29
  """

  def e1_1 =
    createJsEnrichment("[", exitOnCompileError = false).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      failureContains("Error compiling")
    }

  def e1_2 =
    JavascriptScriptEnrichment.create(schemaKey, "[", JsonObject.empty, exitOnCompileError = true, Set.empty) must beLike {
      case Left(msg) if msg.contains("Error compiling") => ok
    }

  def e2 = {
    val function = s"""
      function process(event) {
        return { foo: "bar" }
      }"""
    createJsEnrichment(function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike(
      failureContains("not read as an array")
    )
  }

  def e3 = {
    val function = s"""
      function process(event) {
        return [ { foo: "bar" } ]
      }"""
    createJsEnrichment(function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike(
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
    createJsEnrichment(function).process(buildEnriched(appId), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
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
    createJsEnrichment(function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth)
    enriched.app_id must beEqualTo(newAppId)
  }

  def e6 = {
    val function = s"""
      function process(event) {
        throw "Error"
      }"""
    createJsEnrichment(function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike(
      failureContains("Error during execution")
    )
  }

  def e7 = {
    val function = s"""
      function process(event) {
        return [ ];
      }"""
    createJsEnrichment(function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
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
    createJsEnrichment(function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      case Result.Success(List(c1, c2), false) if c1 == context1 && c2 == context2 => ok
    }
  }

  def e9 = {
    val function = s"""
      function process(event) {
        var a = 42     // no-op
      }"""

    createJsEnrichment(function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beEqualTo(
      Result.Success(Nil, false)
    )
  }

  def e10 = {
    val function = s"""
      function process(event) {
        return null
      }"""

    createJsEnrichment(function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beEqualTo(
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
    createJsEnrichment(function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth)
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
    createJsEnrichment(function, params = params).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth)
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

    createJsEnrichment(function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth)
    enriched.app_id must beEqualTo("greatApp")

    createJsEnrichment(function).process(enriched, List("x-jwt: newId"), SpecHelpers.DefaultMaxJsonDepth)
    enriched.app_id must beEqualTo("newId")
  }

  def e14 = {
    val function = s"""
      function process(event) {
        event.drop()
      }"""
    createJsEnrichment(function).process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beEqualTo(
      Result.Dropped
    )
  }

  def e15 = {
    val function = s"""
      function process(event) {
        event.eraseDerived_contexts()
      }"""
    val enriched = buildEnriched()
    (createJsEnrichment(function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth) must beEqualTo(
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

    createJsEnrichment(function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth)
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

    createJsEnrichment(function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth)
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

    createJsEnrichment(function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
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

    createJsEnrichment(function).process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      failureContains("invalid contexts json")
    }
  }

  def e20 = {
    val enriched = buildEnriched()
    val testUrl =
      "https://raw.githubusercontent.com/snowplow/iglu-central/refs/heads/master/schemas/com.snowplowanalytics.snowplow/atomic/jsonschema/1-0-0"
    val testStr = "test"
    // URL constructors are deprecated in Java 21, but they can still be used in JS enrichment script
    val bufferedReader = new java.io.BufferedReader(
      new java.io.InputStreamReader(
        new java.net.URI(testUrl).toURL.openConnection().getInputStream()
      )
    )
    val testQuery = "q1=v1&q2=v2"
    val bufferedReaderExpected = bufferedReader.readLine()
    bufferedReader.close()
    val shaValExpected = org.apache.commons.codec.digest.DigestUtils.sha256Hex(testStr)
    val urlEncodedExpected = java.net.URLEncoder.encode(testUrl, java.nio.charset.StandardCharsets.UTF_8)
    val baseEncoded = new String(java.util.Base64.getEncoder.encode(testStr.getBytes))
    val function =
      s"""
      testUrl = "$testUrl"
      conn = new java.net.URL(testUrl).openConnection()
      reader = new java.io.BufferedReader(new java.io.InputStreamReader(conn.getInputStream()))
      check1 = reader.readLine() === "$bufferedReaderExpected"
      reader.close()

      uri = new java.net.URI(testUrl)
      check2 = uri.toString() === testUrl

      digest = java.security.MessageDigest.getInstance("SHA3-256")
      check3 = digest.getAlgorithm() === "SHA3-256"

      shaVal = org.apache.commons.codec.digest.DigestUtils.sha256Hex("$testStr")
      check4 = shaVal === "$shaValExpected"

      URLEncoder = Java.type('java.net.URLEncoder')
      StandardCharsets = Java.type('java.nio.charset.StandardCharsets')
      urlEncoded = URLEncoder.encode(testUrl, StandardCharsets.UTF_8.toString())
      check5 = urlEncoded === "$urlEncodedExpected"

      conn = new java.net.URL(testUrl).openConnection()
      conn.setRequestMethod("POST")
      conn.setRequestProperty("accept", "application/json")
      conn.setDoOutput(true)
      var writer = new java.io.OutputStreamWriter(conn.getOutputStream())
      writer.write("payload")
      writer.close()

      base64Decoded = new java.lang.String(java.util.Base64.decoder.decode("$baseEncoded"))
      check6 = base64Decoded === "$testStr"

      urlQuery = new java.net.URI("$testUrl?$testQuery").getQuery()
      check7 = urlQuery === "$testQuery"

      check8 = new java.util.ArrayList(["test1", "test2", "test3"]).contains("test3")

      urlDecoded = java.net.URLDecoder.decode(urlEncoded, "UTF-8")
      check9 = urlDecoded === "$testUrl"

      function process(event) {
          return [
            {
              schema: "iglu:com.snowplowanalytics.iglu/anything-a/jsonschema/1-0-0",
              data: {
                 check1: check1,
                 check2: check2,
                 check3: check3,
                 check4: check4,
                 check5: check5,
                 check6: check6,
                 check7: check7,
                 check8: check8,
                 check9: check9,
              }
            }
          ]
      }"""

    val expected =
      SelfDescribingData(
        SchemaKey("com.snowplowanalytics.iglu", "anything-a", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{
          "check1": true,
          "check2": true,
          "check3": true,
          "check4": true,
          "check5": true,
          "check6": true,
          "check7": true,
          "check8": true,
          "check9": true
        }"""
      )

    val allowedClasses = Set(
      "java.lang.String",
      "java.util.ArrayList",
      "java.util.Base64",
      "java.io.BufferedReader",
      "java.io.InputStreamReader",
      "java.io.OutputStreamWriter",
      "java.net.URI",
      "java.net.URL",
      "java.net.URLDecoder",
      "java.net.URLEncoder",
      "java.nio.charset.StandardCharsets",
      "java.security.MessageDigest",
      "org.apache.commons.codec.digest.DigestUtils"
    )

    JavascriptScriptEnrichment.create(schemaKey, function, JsonObject.empty, exitOnCompileError = true, allowedClasses) match {
      case Left(e) => ko(s"Error while creating the JS enrichment: $e")
      case Right(js) =>
        js.process(enriched, List.empty, SpecHelpers.DefaultMaxJsonDepth) match {
          case Result.Success(List(c), false) =>
            c must beEqualTo(expected)
          case Result.Failure(f) => ko(s"Error during the execution of the JS enrichment: ${f.message}")
        }
    }
  }

  def e21 = {
    val function = s"""
      function process(event) {
        var url = new java.net.URL("http://example.com");
        return [];
      }"""
    createJsEnrichment(function, allowedJavaClasses = Set.empty, exitOnCompileError = false)
      .process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      case Result.Failure(FailureDetails.EnrichmentFailure(_, FailureDetails.EnrichmentFailureMessage.Simple(msg)))
          if msg.contains("java.lang.ClassNotFoundException") && msg.contains("java.net.URL") =>
        ok
    }
  }

  def e22 = {
    val function = s"""
      function process(event) {
        var url = new java.net.URL("http://example.com");
        return [{ schema: "iglu:com.test/url/jsonschema/1-0-0", data: { host: url.getHost() } }];
      }"""
    val expected = SelfDescribingData(
      SchemaKey("com.test", "url", "jsonschema", SchemaVer.Full(1, 0, 0)),
      json"""{"host": "example.com"}"""
    )
    createJsEnrichment(function, allowedJavaClasses = Set("java.net.URL"))
      .process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      case Result.Success(List(c), false) => c must beEqualTo(expected)
    }
  }

  def e23 = {
    val function = s"""
      function process(event) {
        var URLClass = Java.type("java.net.URL");
        var url = new URLClass("http://example.com");
        return [];
      }"""
    createJsEnrichment(function, allowedJavaClasses = Set.empty, exitOnCompileError = false)
      .process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      case Result.Failure(FailureDetails.EnrichmentFailure(_, FailureDetails.EnrichmentFailureMessage.Simple(msg)))
          if msg.contains("java.lang.ClassNotFoundException") && msg.contains("java.net.URL") =>
        ok
    }
  }

  def e24 = {
    val function = s"""
      function process(event) {
        var URLClass = Java.type("java.net.URL");
        var url = new URLClass("http://example.com");
        return [{ schema: "iglu:com.test/url/jsonschema/1-0-0", data: { host: url.getHost() } }];
      }"""
    val expected = SelfDescribingData(
      SchemaKey("com.test", "url", "jsonschema", SchemaVer.Full(1, 0, 0)),
      json"""{"host": "example.com"}"""
    )
    createJsEnrichment(function, allowedJavaClasses = Set("java.net.URL"))
      .process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      case Result.Success(List(c), false) => c must beEqualTo(expected)
    }
  }

  def e25 = {
    val function = s"""
      function process(event) {
        var hash = org.apache.commons.codec.digest.DigestUtils.md5Hex("test");
        return [];
      }"""
    createJsEnrichment(function, allowedJavaClasses = Set.empty, exitOnCompileError = false)
      .process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      case Result.Failure(FailureDetails.EnrichmentFailure(_, FailureDetails.EnrichmentFailureMessage.Simple(msg)))
          if msg.contains("java.lang.ClassNotFoundException") && msg.contains("org.apache.commons.codec.digest.DigestUtils") =>
        ok
    }
  }

  def e26 = {
    val function = s"""
      function process(event) {
        var hash = org.apache.commons.codec.digest.DigestUtils.md5Hex("test");
        return [{ schema: "iglu:com.test/hash/jsonschema/1-0-0", data: { md5: hash } }];
      }"""
    val expected = SelfDescribingData(
      SchemaKey("com.test", "hash", "jsonschema", SchemaVer.Full(1, 0, 0)),
      json"""{"md5": "098f6bcd4621d373cade4e832627b4f6"}"""
    )
    createJsEnrichment(function, allowedJavaClasses = Set("org.apache.commons.codec.digest.DigestUtils"))
      .process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      case Result.Success(List(c), false) => c must beEqualTo(expected)
    }
  }

  def e27 = {
    val function = s"""
      function process(event) {
        // the resulting java.net.URLConnection is not explicitly allowed
        var conn = new java.net.URL("http://localhost:9999").openConnection();
        return [{ schema: "iglu:com.test/url/jsonschema/1-0-0", data: { host: conn.getURL().getHost() } }];
      }"""
    val expected = SelfDescribingData(
      SchemaKey("com.test", "url", "jsonschema", SchemaVer.Full(1, 0, 0)),
      json"""{"host": "localhost"}"""
    )
    createJsEnrichment(function, allowedJavaClasses = Set("java.net.URL"))
      .process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      case Result.Success(List(c), false) => c must beEqualTo(expected)
    }
  }

  def e28 = {
    val function = s"""
      function process(event) {
        var url = new java.net.URL("http://example.com");
        var hash = org.apache.commons.codec.digest.DigestUtils.md5Hex("test");
        return [{
          schema: "iglu:com.test/wildcard/jsonschema/1-0-0",
          data: { host: url.getHost(), md5: hash }
        }];
      }"""
    val expected = SelfDescribingData(
      SchemaKey("com.test", "wildcard", "jsonschema", SchemaVer.Full(1, 0, 0)),
      json"""{"host": "example.com", "md5": "098f6bcd4621d373cade4e832627b4f6"}"""
    )
    createJsEnrichment(function, allowedJavaClasses = Set("*"))
      .process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      case Result.Success(List(c), false) => c must beEqualTo(expected)
    }
  }

  def e29 = {
    val function = s"""
      function process(event) {
        var url = new java.net.URL("http://example.com");
        var uri = new java.net.URI("http://example.com/path");
        return [{
          schema: "iglu:com.test/netclasses/jsonschema/1-0-0",
          data: { host: url.getHost(), path: uri.getPath() }
        }];
      }"""
    val expected = SelfDescribingData(
      SchemaKey("com.test", "netclasses", "jsonschema", SchemaVer.Full(1, 0, 0)),
      json"""{"host": "example.com", "path": "/path"}"""
    )
    createJsEnrichment(function, allowedJavaClasses = Set("java.net.*"))
      .process(buildEnriched(), List.empty, SpecHelpers.DefaultMaxJsonDepth) must beLike {
      case Result.Success(List(c), false) => c must beEqualTo(expected)
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

object JavascriptScriptEnrichmentSpec {
  val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "javascript_script_config", "jsonschema", SchemaVer.Full(1, 0, 0))

  def createJsEnrichment(
    rawFunction: String,
    schemaKey: SchemaKey = schemaKey,
    params: JsonObject = JsonObject.empty,
    exitOnCompileError: Boolean = true,
    allowedJavaClasses: Set[String] = Set.empty
  ): JavascriptScriptEnrichment =
    JavascriptScriptEnrichment.create(schemaKey, rawFunction, params, exitOnCompileError, allowedJavaClasses).toOption.get
}
