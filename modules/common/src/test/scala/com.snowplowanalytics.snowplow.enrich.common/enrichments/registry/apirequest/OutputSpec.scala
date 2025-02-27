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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest

import io.circe._
import io.circe.literal._
import org.specs2.Specification

class OutputSpec extends Specification {
  def is = s2"""
  Not found value result in Failure                   $e1
  Successfully generate context                       $e2
  Successfully generate context out of complex object $e3
  """

  def e1 = {
    val output =
      Output("iglu:com.snowplowanalytics/some_schema/jsonschema/1-0-0", Some(JsonOutput("$.value")))
    output.extract(Json.fromJsonObject(JsonObject.empty)) must beLeft
  }

  def e2 = {
    val output = Output(
      "iglu:com.snowplowanalytics/some_schema/jsonschema/1-0-0",
      Some(JsonOutput("$.value"))
    )
    output
      .parseResponse("""{"value": 32}""")
      .flatMap(output.extract)
      .map(output.describeJson) must beRight.like {
      case context =>
        context must be equalTo json"""{
            "schema": "iglu:com.snowplowanalytics/some_schema/jsonschema/1-0-0",
            "data": 32
          }"""
    }
  }

  def e3 = {
    val output = Output(
      "iglu:com.snowplowanalytics/complex_schema/jsonschema/1-0-0",
      Some(JsonOutput("$.objects[1].deepNesting[3]"))
    )
    output
      .parseResponse("""
        |{
        |  "value": 32,
        |  "objects":
        |  [
        |    {"wrongValue": 11},
        |    {"deepNesting": [1,2,3,42]},
        |    {"wrongValue": 10}
        |  ]
        |}
      """.stripMargin)
      .flatMap(output.extract)
      .map(output.describeJson) must beRight.like {
      case context =>
        context must be equalTo json"""{
          "schema": "iglu:com.snowplowanalytics/complex_schema/jsonschema/1-0-0",
          "data": 42
        }"""
    }

  }
}
