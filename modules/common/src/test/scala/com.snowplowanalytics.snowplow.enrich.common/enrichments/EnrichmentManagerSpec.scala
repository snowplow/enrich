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
package com.snowplowanalytics.snowplow.enrich.common

package enrichments

import cats.Id
import cats.implicits._
import cats.data.NonEmptyList

import io.circe.literal._

import org.joda.time.DateTime

import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import loaders._
import adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import utils.Clock._
import utils.ConversionUtils
import enrichments.registry.{IabEnrichment, JavascriptScriptEnrichment, YauaaEnrichment}

import org.specs2.mutable.Specification
import org.specs2.matcher.EitherMatchers

class EnrichmentManagerSpec extends Specification with EitherMatchers {
  import EnrichmentManagerSpec._

  "enrichEvent" should {
    "return a SchemaViolations bad row if the input event contains an invalid context" >> {
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" -> """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "foo": "hello@world.com",
                  "emailAddress2": "foo@bar.org"
                }
              }
            ]
          }
        """
      )
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent(
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent
      )

      enriched.value must beLeft.like {
        case _: BadRow.SchemaViolations => ok
        case br => ko(s"bad row [$br] is not SchemaViolations")
      }
    }

    "return a SchemaViolations bad row if the input unstructured event is invalid" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "ue_pr" -> """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "emailAddress3": "foo@bar.org"
              }
            }
          }"""
      )
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent(
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent
      )
      enriched.value must beLeft.like {
        case _: BadRow.SchemaViolations => ok
        case br => ko(s"bad row [$br] is not SchemaViolations")
      }
    }

    "return an EnrichmentFailures bad row if one of the enrichment (JS enrichment here) fails" >> {
      val script = """
        function process(event) {
          throw "Javascript exception";
          return [ { a: "b" } ];
        }"""

      val config = json"""{
        "parameters": {
          "script": ${ConversionUtils.encodeBase64Url(script)}
        }
      }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "javascript_script_config",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      val jsEnrichConf =
        JavascriptScriptEnrichment.parse(config, schemaKey).toOption.get
      val jsEnrich = JavascriptScriptEnrichment(jsEnrichConf.schemaKey, jsEnrichConf.rawFunction)
      val enrichmentReg = EnrichmentRegistry[Id](javascriptScript = Some(jsEnrich))

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      )
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent(
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent
      )
      enriched.value must beLeft.like {
        case BadRow.EnrichmentFailures(
              _,
              Failure.EnrichmentFailures(
                _,
                NonEmptyList(
                  FailureDetails.EnrichmentFailure(
                    _,
                    _: FailureDetails.EnrichmentFailureMessage.Simple
                  ),
                  Nil
                )
              ),
              _
            ) =>
          ok
        case br =>
          ko(
            s"bad row [$br] is not an EnrichmentFailures containing one EnrichmentFailureMessage.Simple"
          )
      }
    }

    "return an EnrichmentFailures bad row containing one IgluError if one of the contexts added by the enrichments is invalid" >> {
      val script = """
        function process(event) {
          return [ { schema: "iglu:com.acme/email_sent/jsonschema/1-0-0",
                     data: {
                       emailAddress: "hello@world.com",
                       foo: "bar"
                     }
                   } ];
        }"""

      val config = json"""{
        "parameters": {
          "script": ${ConversionUtils.encodeBase64Url(script)}
        }
      }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "javascript_script_config",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      val jsEnrichConf =
        JavascriptScriptEnrichment.parse(config, schemaKey).toOption.get
      val jsEnrich = JavascriptScriptEnrichment(jsEnrichConf.schemaKey, jsEnrichConf.rawFunction)
      val enrichmentReg = EnrichmentRegistry[Id](javascriptScript = Some(jsEnrich))

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      )
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent(
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent
      )
      enriched.value must beLeft.like {
        case BadRow.EnrichmentFailures(
              _,
              Failure.EnrichmentFailures(
                _,
                NonEmptyList(
                  FailureDetails.EnrichmentFailure(
                    _,
                    _: FailureDetails.EnrichmentFailureMessage.IgluError
                  ),
                  Nil
                )
              ),
              payload
            ) if payload.enriched.derived_contexts.isDefined =>
          ok
        case br => ko(s"bad row [$br] is not an EnrichmentFailures containing one IgluError and with derived_contexts defined")
      }
    }

    "emit an EnrichedEvent if everything goes well" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" -> """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "hello@world.com",
                  "emailAddress2": "foo@bar.org"
                }
              }
            ]
          }
        """,
        "ue_pr" -> """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org"
              }
            }
          }"""
      )
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent(
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent
      )
      enriched.value must beRight
    }

    "have a preference of 'ua' query string parameter over user agent of HTTP header" >> {
      val qs_ua = "Mozilla/5.0 (X11; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0"
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "ua" -> qs_ua,
        "p" -> "web"
      )
      val contextWithUa = context.copy(useragent = Some("header-useragent"))
      val rawEvent = RawEvent(api, parameters, None, source, contextWithUa)
      val enriched = EnrichmentManager.enrichEvent(
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent
      )
      enriched.value.map(_.useragent) must beRight(qs_ua)
      enriched.value.map(_.derived_contexts) must beRight((_: String).contains("\"agentName\":\"Firefox\""))
    }

    "use user agent of HTTP header if 'ua' query string parameter is not set" >> {
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      )
      val contextWithUa = context.copy(useragent = Some("header-useragent"))
      val rawEvent = RawEvent(api, parameters, None, source, contextWithUa)
      val enriched = EnrichmentManager.enrichEvent(
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent
      )
      enriched.value.map(_.useragent) must beRight("header-useragent")
    }
  }

  "getIabContext" should {
    "return None if useragent is null" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("127.0.0.1")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      EnrichmentManager.getIabContext(input, iabEnrichment) must beRight(None)
    }

    "return None if user_ipaddress is null" >> {
      val input = new EnrichedEvent()
      input.setUseragent("Firefox")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      EnrichmentManager.getIabContext(input, iabEnrichment) must beRight(None)
    }

    "return None if derived_tstamp is null" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("127.0.0.1")
      input.setUseragent("Firefox")
      EnrichmentManager.getIabContext(input, iabEnrichment) must beRight(None)
    }

    "return None if user_ipaddress is invalid" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("invalid")
      input.setUseragent("Firefox")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      EnrichmentManager.getIabContext(input, iabEnrichment) must beRight(None)
    }

    "return None if user_ipaddress is hostname (don't try to resovle it)" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("localhost")
      input.setUseragent("Firefox")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      EnrichmentManager.getIabContext(input, iabEnrichment) must beRight(None)
    }

    "return Some if all arguments are valid" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("127.0.0.1")
      input.setUseragent("Firefox")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      EnrichmentManager.getIabContext(input, iabEnrichment) must beRight.like { case ctx => ctx must beSome }
    }
  }

  "getCollectorVersionSet" should {
    "return an enrichment failure if v_collector is null or empty" >> {
      val input = new EnrichedEvent()
      EnrichmentManager.getCollectorVersionSet(input) must beLeft.like {
        case _: FailureDetails.EnrichmentFailure => ok
        case other => ko(s"expected EnrichmentFailure but got $other")
      }
      input.v_collector = ""
      EnrichmentManager.getCollectorVersionSet(input) must beLeft.like {
        case _: FailureDetails.EnrichmentFailure => ok
        case other => ko(s"expected EnrichmentFailure but got $other")
      }
    }

    "return Unit if v_collector is set" >> {
      val input = new EnrichedEvent()
      input.v_collector = "v42"
      EnrichmentManager.getCollectorVersionSet(input) must beRight(())
    }
  }
}

object EnrichmentManagerSpec {

  val enrichmentReg = EnrichmentRegistry[Id](yauaa = Some(YauaaEnrichment(None)))
  val client = SpecHelpers.client
  val processor = Processor("ssc-tests", "0.0.0")
  val timestamp = DateTime.now()

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

  val iabEnrichment = IabEnrichment
    .parse(
      json"""{
      "name": "iab_spiders_and_robots_enrichment",
      "vendor": "com.snowplowanalytics.snowplow.enrichments",
      "enabled": false,
      "parameters": {
        "ipFile": {
          "database": "ip_exclude_current_cidr.txt",
          "uri": "s3://my-private-bucket/iab"
        },
        "excludeUseragentFile": {
          "database": "exclude_current.txt",
          "uri": "s3://my-private-bucket/iab"
        },
        "includeUseragentFile": {
          "database": "include_current.txt",
          "uri": "s3://my-private-bucket/iab"
        }
      }
    }""",
      SchemaKey(
        "com.snowplowanalytics.snowplow.enrichments",
        "iab_spiders_and_robots_enrichment",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ),
      true
    )
    .toOption
    .getOrElse(throw new RuntimeException("IAB enrichment couldn't be initialised")) // to make sure it's not none
    .enrichment[Id]
    .some
}
