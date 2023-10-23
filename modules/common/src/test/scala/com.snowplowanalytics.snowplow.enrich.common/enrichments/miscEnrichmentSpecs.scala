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
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import cats.syntax.either._

import io.circe.literal._

import org.specs2.mutable.{Specification => MutSpecification}
import org.specs2.Specification
import org.specs2.matcher.DataTables

import com.snowplowanalytics.snowplow.badrows.{FailureDetails, Processor}

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

class EtlVersionSpec extends MutSpecification {
  "The ETL version" should {
    "be successfully returned using an x.y.z format" in {
      val artifact = "Enrich"
      val version = "1.2.3"
      MiscEnrichments.etlVersion(Processor(artifact, version)) must beEqualTo(s"$artifact-$version")
    }
  }
}

/** Tests the extractPlatform function. Uses DataTables. */
class ExtractPlatformSpec extends Specification with DataTables {
  val FieldName = "p"
  def err: String => FailureDetails.EnrichmentFailure =
    input =>
      FailureDetails.EnrichmentFailure(
        None,
        FailureDetails.EnrichmentFailureMessage.InputData(
          FieldName,
          Option(input),
          "not recognized as a tracking platform"
        )
      )

  def is = s2"""
  Extracting platforms with extractPlatform should work $e1
  """

  def e1 =
    "SPEC NAME" || "INPUT VAL" | "EXPECTED OUTPUT" |
      "valid web" !! "web" ! "web".asRight |
      "valid mobile/tablet" !! "mob" ! "mob".asRight |
      "valid desktop/laptop/netbook" !! "pc" ! "pc".asRight |
      "valid server-side app" !! "srv" ! "srv".asRight |
      "valid general app" !! "app" ! "app".asRight |
      "valid connected TV" !! "tv" ! "tv".asRight |
      "valid games console" !! "cnsl" ! "cnsl".asRight |
      "valid iot (internet of things)" !! "iot" ! "iot".asRight |
      "invalid empty" !! "" ! err("").asLeft |
      "invalid null" !! null ! err(null).asLeft |
      "invalid platform" !! "ma" ! err("ma").asLeft |> { (_, input, expected) =>
      MiscEnrichments.extractPlatform(FieldName, input) must_== expected
    }
}

class ExtractIpSpec extends Specification with DataTables {

  def is = s2"""
  Extracting ips with extractIp should work $e1
  """

  val nullString: String = null

  def e1 =
    "SPEC NAME" || "INPUT VAL" | "EXPECTED OUTPUT" |
      "single ip" !! "127.0.0.1" ! "127.0.0.1".asRight |
      "ips ', '-separated" !! "127.0.0.1, 127.0.0.2" ! "127.0.0.1".asRight |
      "ips ','-separated" !! "127.0.0.1,127.0.0.2" ! "127.0.0.1".asRight |
      "ips separated out of the spec" !! "1.0.0.1!1.0.0.2" ! "1.0.0.1!1.0.0.2".asRight |
      // ConversionUtils.makeTsvSafe returns null for empty string
      "empty" !! "" ! Right(null) |
      "null" !! null ! Right(null) |> { (_, input, expected) =>
      MiscEnrichments.extractIp("ip", input) must_== expected
    }

}

class FormatContextsSpec extends MutSpecification {

  "extractContexts" should {
    "convert a list of JObjects to a self-describing contexts JSON" in {

      val derivedContextsList = List(
        SelfDescribingData(
          SchemaKey("com.acme", "user", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"type": "tester", "name": "bethany"}"""
        ),
        SelfDescribingData(
          SchemaKey("com.acme", "design", "jsonschema", SchemaVer.Full(1, 0, 0)),
          json"""{"color": "red", "fontSize": 14}"""
        )
      )

      val expected = """
      |{
        |"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
        |"data":[
        |{
          |"schema":"iglu:com.acme/user/jsonschema/1-0-0",
          |"data":{
            |"type":"tester",
            |"name":"bethany"
          |}
        |},
        |{
          |"schema":"iglu:com.acme/design/jsonschema/1-0-0",
          |"data":{
            |"color":"red",
            |"fontSize":14
            |}
          |}
        |]
      |}""".stripMargin.replaceAll("[\n\r]", "")

      MiscEnrichments.formatContexts(derivedContextsList) must beSome(expected)
    }
  }
}

class FormatUnstructEventSpec extends MutSpecification {

  "extractUnstructEvent" should {
    "convert a JObject to a self-describing unstruct event JSON" in {

      val unstructEvent = SelfDescribingData(
        SchemaKey("com.acme", "design", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"color": "red", "fontSize": 14}"""
      )

      val expected = """
      |{
       |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
       |"data":
         |{
           |"schema":"iglu:com.acme/design/jsonschema/1-0-0",
           |"data":{
             |"color":"red",
             |"fontSize":14
           |}
         |}
      |}""".stripMargin.replaceAll("[\n\r]", "")

      MiscEnrichments.formatUnstructEvent(Some(unstructEvent)) must beSome(expected)
    }
  }
}
