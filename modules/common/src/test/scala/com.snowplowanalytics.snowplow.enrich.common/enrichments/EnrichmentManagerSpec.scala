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
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import org.apache.commons.codec.digest.DigestUtils

import org.specs2.mutable.Specification
import org.specs2.matcher.EitherMatchers

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect

import cats.implicits._
import cats.data.{Ior, NonEmptyList}

import io.circe.Json
import io.circe.literal._
import io.circe.parser.{parse => jparse}
import io.circe.syntax._

import org.joda.time.DateTime

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.enrich.common.QueryStringParameters
import com.snowplowanalytics.snowplow.enrich.common.loaders._
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.pii.{
  JsonMutators,
  PiiJson,
  PiiPseudonymizerEnrichment,
  PiiStrategyPseudonymize
}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.ConversionUtils
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.{
  CrossNavigationEnrichment,
  HttpHeaderExtractorEnrichment,
  IabEnrichment,
  JavascriptScriptEnrichment,
  YauaaEnrichment
}
import com.snowplowanalytics.snowplow.enrich.common.AcceptInvalid
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class EnrichmentManagerSpec extends Specification with EitherMatchers with CatsEffect {
  import EnrichmentManagerSpec._

  "enrichEvent" should {
    "return a SchemaViolations bad row if the input event contains an invalid context" >> {
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
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
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value map {
        case Ior.Left(_: BadRow.SchemaViolations) => ok
        case other => ko(s"[$other] is not a SchemaViolations bad row")
      }
    }

    "return a SchemaViolations bad row if the input unstructured event is invalid" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "unallowedAdditionalField": "foo@bar.org"
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value map {
        case Ior.Left(_: BadRow.SchemaViolations) => ok
        case other => ko(s"[$other] is not a SchemaViolations bad row")
      }
    }

    "return a SchemaViolations bad row that contains 1 ValidationError for the atomic field and 1 ValidationError for the unstruct event" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "tr_tt" -> "not number",
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "unallowedAdditionalField": "foo@bar.org"
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      EnrichmentManager
        .enrichEvent[IO](
          enrichmentReg,
          client,
          processor,
          timestamp,
          rawEvent,
          AcceptInvalid.featureFlags,
          IO.unit,
          SpecHelpers.registryLookup,
          atomicFieldLimits,
          emitIncomplete
        )
        .value
        .map {
          case Ior.Left(
                BadRow.SchemaViolations(
                  _,
                  Failure.SchemaViolations(_,
                                           NonEmptyList(FailureDetails.SchemaViolation.IgluError(schemaKey1, clientError1),
                                                        List(FailureDetails.SchemaViolation.IgluError(schemaKey2, clientError2))
                                           )
                  ),
                  _
                )
              ) =>
            schemaKey1 must beEqualTo(AtomicFields.atomicSchema)
            clientError1.toString must contain("tr_tt")
            clientError1.toString must contain("Cannot be converted to java.math.BigDecimal")
            schemaKey2 must beEqualTo(emailSentSchema)
            clientError2.toString must contain(
              "unallowedAdditionalField: is not defined in the schema and the schema does not allow additional properties"
            )
          case other =>
            ko(s"[$other] is not a SchemaViolations bad row with 2 expected IgluError")
        }
    }

    "return an EnrichmentFailures bad row if one of the enrichment (JS enrichment here) fails" >> {
      val script =
        """
        function process(event) {
          throw "Javascript exception";
          return [ { a: "b" } ];
        }"""

      val config =
        json"""{
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
      val enrichmentReg = EnrichmentRegistry[IO](javascriptScript = List(jsEnrich))

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value map {
        case Ior.Left(
              BadRow.EnrichmentFailures(
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
              )
            ) =>
          ok
        case other => ko(s"[$other] is not an EnrichmentFailures bad row with one EnrichmentFailureMessage.Simple")
      }
    }

    "return a SchemaViolations bad row containing one IgluError if one of the contexts added by the enrichments is invalid" >> {
      val script =
        """
        function process(event) {
          return [ { schema: "iglu:com.acme/email_sent/jsonschema/1-0-0",
                     data: {
                       emailAddress: "hello@world.com",
                       foo: "bar"
                     }
                   } ];
        }"""

      val config =
        json"""{
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
      val enrichmentReg = EnrichmentRegistry[IO](javascriptScript = List(jsEnrich))

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value map {
        case Ior.Left(
              BadRow.SchemaViolations(
                _,
                Failure.SchemaViolations(
                  _,
                  NonEmptyList(
                    _: FailureDetails.SchemaViolation.IgluError,
                    Nil
                  )
                ),
                _
              )
            ) =>
          ok
        case other => ko(s"[$other] is not a SchemaViolations bad row with one IgluError")
      }
    }

    "emit an EnrichedEvent if everything goes well" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
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
        "ue_pr" ->
          """
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
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value.map {
        case Ior.Right(_) => ok
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "emit an EnrichedEvent if a PII value that needs to be hashed is an empty string" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
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
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "emailAddress3": ""
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enrichmentReg = EnrichmentRegistry[IO](
        piiPseudonymizer = PiiPseudonymizerEnrichment(
          List(
            PiiJson(
              fieldMutator = JsonMutators("unstruct_event"),
              schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0, 0),
              jsonPath = "$.emailAddress3"
            )
          ),
          false,
          PiiStrategyPseudonymize(
            "MD5",
            hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
            "pepper123"
          )
        ).some
      )
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value.map {
        case Ior.Right(_) => ok
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "emit an EnrichedEvent if a PII value that needs to be hashed is null" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
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
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/2-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "emailAddress3": null
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enrichmentReg = EnrichmentRegistry[IO](
        piiPseudonymizer = PiiPseudonymizerEnrichment(
          List(
            PiiJson(
              fieldMutator = JsonMutators("unstruct_event"),
              schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0, 0),
              jsonPath = "$.emailAddress3"
            )
          ),
          false,
          PiiStrategyPseudonymize(
            "MD5",
            hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
            "pepper123"
          )
        ).some
      )
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value.map {
        case Ior.Right(_) => ok
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "fail to emit an EnrichedEvent if a PII value that needs to be hashed is an empty object" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
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
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "emailAddress3": {}
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enrichmentReg = EnrichmentRegistry[IO](
        piiPseudonymizer = PiiPseudonymizerEnrichment(
          List(
            PiiJson(
              fieldMutator = JsonMutators("unstruct_event"),
              schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0, 0),
              jsonPath = "$.emailAddress3"
            )
          ),
          false,
          PiiStrategyPseudonymize(
            "MD5",
            hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
            "pepper123"
          )
        ).some
      )
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value.map {
        case Ior.Left(_) => ok
        case other => ko(s"[$other] is not a bad row")
      }
    }

    "fail to emit an EnrichedEvent if a context PII value that needs to be hashed is an empty object" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "hello@world.com",
                  "emailAddress2": "foo@bar.org",
                  "emailAddress3": {}
                }
              }
            ]
          }
        """,
        "ue_pr" ->
          """
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
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enrichmentReg = EnrichmentRegistry[IO](
        piiPseudonymizer = PiiPseudonymizerEnrichment(
          List(
            PiiJson(
              fieldMutator = JsonMutators("contexts"),
              schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0, 0),
              jsonPath = "$.emailAddress3"
            )
          ),
          false,
          PiiStrategyPseudonymize(
            "MD5",
            hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
            "pepper123"
          )
        ).some
      )
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value.map {
        case Ior.Left(_) => ok
        case other => ko(s"[$other] is not a bad row")
      }
    }

    "fail to emit an EnrichedEvent if a PII value needs to be hashed in both co and ue and is invalid in one of them" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "hello@world.com",
                  "emailAddress2": "foo@bar.org",
                  "emailAddress3": {}
                }
              }
            ]
          }
        """,
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "emailAddress3": ""
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enrichmentReg = EnrichmentRegistry[IO](
        piiPseudonymizer = PiiPseudonymizerEnrichment(
          List(
            PiiJson(
              fieldMutator = JsonMutators("contexts"),
              schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0, 0),
              jsonPath = "$.emailAddress3"
            ),
            PiiJson(
              fieldMutator = JsonMutators("unstruct_event"),
              schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0, 0),
              jsonPath = "$.emailAddress3"
            )
          ),
          false,
          PiiStrategyPseudonymize(
            "MD5",
            hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
            "pepper123"
          )
        ).some
      )
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value.map {
        case Ior.Left(_) => ok
        case other => ko(s"[$other] is not a bad row")
      }
    }

    "emit an EnrichedEvent for valid integer fields" >> {
      val integers = List("42", "-42", "null")
      val fields = List("tid", "vid", "ti_qu", "pp_mix", "pp_max", "pp_miy", "pp_may")

      integers
        .flatTraverse { integer =>
          fields.traverse { field =>
            val parameters = Map(
              "e" -> "ue",
              "tv" -> "js-0.13.1",
              "p" -> "web",
              field -> integer
            ).toOpt
            val rawEvent = RawEvent(api, parameters, None, source, context)
            val enriched = EnrichmentManager.enrichEvent[IO](
              enrichmentReg,
              client,
              processor,
              timestamp,
              rawEvent,
              AcceptInvalid.featureFlags,
              IO.unit,
              SpecHelpers.registryLookup,
              atomicFieldLimits,
              emitIncomplete
            )
            enriched.value.map {
              case Ior.Right(_) => ok
              case other => ko(s"[$other] is not an enriched event")
            }
          }
        }
    }

    "emit an EnrichedEvent for valid decimal fields" >> {
      val decimals = List("42", "42.5", "null")
      val fields = List("ev_va", "se_va", "tr_tt", "tr_tx", "tr_sh", "ti_pr")

      decimals
        .flatTraverse { decimal =>
          fields.traverse { field =>
            val parameters = Map(
              "e" -> "ue",
              "tv" -> "js-0.13.1",
              "p" -> "web",
              field -> decimal
            ).toOpt
            val rawEvent = RawEvent(api, parameters, None, source, context)
            val enriched = EnrichmentManager.enrichEvent[IO](
              enrichmentReg,
              client,
              processor,
              timestamp,
              rawEvent,
              AcceptInvalid.featureFlags,
              IO.unit,
              SpecHelpers.registryLookup,
              atomicFieldLimits,
              emitIncomplete
            )
            enriched.value.map {
              case Ior.Right(_) => ok
              case other => ko(s"[$other] is not an enriched event")
            }
          }
        }
    }

    "create an EnrichedEvent with correct BigDecimal field values" >> {
      val decimals = List(
        // input, expected
        ("42", "42"),
        ("42.5", "42.5"),
        ("137777104559", "137777104559"),
        ("-137777104559", "-137777104559"),
        ("1E7", "10000000"),
        ("1.2E9", "1200000000"),
        ("0.000001", "0.000001"),
        ("0.0000001", "1E-7") // unavoidable consequence, due to BigDecimal internal representation
      )

      decimals
        .traverse {
          case (input, expected) =>
            val parameters = Map(
              "e" -> "ue",
              "tv" -> "js-0.13.1",
              "p" -> "web",
              "ev_va" -> input
            ).toOpt
            val rawEvent = RawEvent(api, parameters, None, source, context)
            val enriched = EnrichmentManager.enrichEvent[IO](
              enrichmentReg,
              client,
              processor,
              timestamp,
              rawEvent,
              AcceptInvalid.featureFlags,
              IO.unit,
              SpecHelpers.registryLookup,
              atomicFieldLimits,
              emitIncomplete
            )
            enriched.value.map {
              case Ior.Right(enriched) => enriched.se_value.toString must_== expected
              case other => ko(s"[$other] is not an enriched event")
            }
        }
    }

    "have a preference of 'ua' query string parameter over user agent of HTTP header" >> {
      val qs_ua = "Mozilla/5.0 (X11; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0"
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "ua" -> qs_ua,
        "p" -> "web"
      ).toOpt
      val contextWithUa = context.copy(useragent = Some("header-useragent"))
      val rawEvent = RawEvent(api, parameters, None, source, contextWithUa)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value.map {
        case Ior.Right(enriched) =>
          enriched.useragent must_== qs_ua
          enriched.derived_contexts must contain("\"agentName\":\"Firefox\"")
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "use user agent of HTTP header if 'ua' query string parameter is not set" >> {
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      ).toOpt
      val contextWithUa = context.copy(useragent = Some("header-useragent"))
      val rawEvent = RawEvent(api, parameters, None, source, contextWithUa)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value.map {
        case Ior.Right(enriched) => enriched.useragent must_== "header-useragent"
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "accept user agent of HTTP header when it is not URL decodable" >> {
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      ).toOpt
      val ua = "Mozilla/5.0 (X11; Linux x86_64; rv:75.0) Gecko/20100101 %1$s/%2$s Firefox/75.0"
      val contextWithUa = context.copy(useragent = Some(ua))
      val rawEvent = RawEvent(api, parameters, None, source, contextWithUa)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value.map {
        case Ior.Right(enriched) => enriched.useragent must_== ua
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "accept 'ua' in query string when it is not URL decodable" >> {
      val qs_ua = "Mozilla/5.0 (X11; Linux x86_64; rv:75.0) Gecko/20100101 %1$s/%2$s Firefox/75.0"
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "ua" -> qs_ua,
        "p" -> "web"
      ).toOpt
      val contextWithUa = context.copy(useragent = Some("header-useragent"))
      val rawEvent = RawEvent(api, parameters, None, source, contextWithUa)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value.map {
        case Ior.Right(enriched) =>
          enriched.useragent must_== qs_ua
          enriched.derived_contexts must contain("\"agentName\":\"%1$S\"")
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "pass derived contexts generated by previous enrichments to the JavaScript enrichment" >> {
      val script =
        """
        function process(event) {
          var derivedContexts = JSON.parse(event.getDerived_contexts());
          var firstHeaderValue = derivedContexts.data[0].data.value;
          event.setApp_id(firstHeaderValue);
          return [];
        }"""

      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "javascript_script_config",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      val enrichmentReg = EnrichmentRegistry[IO](
        javascriptScript = List(JavascriptScriptEnrichment(schemaKey, script)),
        httpHeaderExtractor = Some(HttpHeaderExtractorEnrichment(".*"))
      )

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      ).toOpt
      val headerContext = context.copy(headers = List("X-Tract-Me: moo"))
      val rawEvent = RawEvent(api, parameters, None, source, headerContext)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value.map {
        case Ior.Right(enriched) => enriched.app_id must_== "moo"
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "run multiple JavaScript enrichments" >> {
      val script1 =
        """
        function process(event) {
          event.setApp_id("test_app_id");
          return [];
        }"""

      val script2 =
        """
        function process(event) {
          event.setPlatform("test_platform");
          return [];
        }"""

      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "javascript_script_config",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      val enrichmentReg = EnrichmentRegistry[IO](
        javascriptScript = List(
          JavascriptScriptEnrichment(schemaKey, script1),
          JavascriptScriptEnrichment(schemaKey, script2)
        )
      )

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )
      enriched.value.map {
        case Ior.Right(enriched) =>
          enriched.app_id must_== "test_app_id"
          enriched.platform must_== "test_platform"
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "emit an EnrichedEvent with superseded schemas" >> {
      val expectedContexts = jparse(
        """
        {
          "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
          "data": [
            {
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org"
              }
            },
            {
              "schema":"iglu:com.acme/superseding_example/jsonschema/1-0-1",
              "data": {
                "field_a": "value_a",
                "field_b": "value_b"
              }
            },
            {
              "schema":"iglu:com.acme/superseding_example/jsonschema/1-0-1",
              "data": {
                "field_a": "value_a",
                "field_b": "value_b",
                "field_d": "value_d"
              }
            },
            {
              "schema":"iglu:com.acme/superseding_example/jsonschema/1-0-1",
              "data": {
                "field_a": "value_a",
                "field_b": "value_b",
                "field_c": "value_c",
                "field_d": "value_d"
              }
            }
          ]
        }
        """
      ).toOption.get
      val expectedDerivedContexts = jparse(
        """
        {
          "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
          "data": [
            {
              "schema":"iglu:com.snowplowanalytics.iglu/validation_info/jsonschema/1-0-0",
              "data":{
                "originalSchema":"iglu:com.acme/superseding_example/jsonschema/1-0-0",
                "validatedWith":"1-0-1"
              }
            },
            {
              "schema":"iglu:com.snowplowanalytics.iglu/validation_info/jsonschema/1-0-0",
              "data":{
                "originalSchema":"iglu:com.acme/superseding_example/jsonschema/2-0-0",
                "validatedWith":"2-0-1"
              }
            }
          ]
        }
        """
      ).toOption.get
      val expectedUnstructEvent = jparse(
        """
        {
          "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
          "data":{
            "schema":"iglu:com.acme/superseding_example/jsonschema/2-0-1",
            "data": {
              "field_e": "value_e",
              "field_f": "value_f",
              "field_g": "value_g"
            }
          }
        }
        """
      ).toOption.get
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "hello@world.com",
                  "emailAddress2": "foo@bar.org"
                }
              },
              {
                "schema":"iglu:com.acme/superseding_example/jsonschema/1-0-0",
                "data": {
                  "field_a": "value_a",
                  "field_b": "value_b"
                }
              },
              {
                "schema":"iglu:com.acme/superseding_example/jsonschema/1-0-0",
                "data": {
                  "field_a": "value_a",
                  "field_b": "value_b",
                  "field_d": "value_d"
                }
              },
              {
                "schema":"iglu:com.acme/superseding_example/jsonschema/1-0-1",
                "data": {
                  "field_a": "value_a",
                  "field_b": "value_b",
                  "field_c": "value_c",
                  "field_d": "value_d"
                }
              }
            ]
          }
        """,
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/superseding_example/jsonschema/2-0-0",
              "data": {
                "field_e": "value_e",
                "field_f": "value_f",
                "field_g": "value_g"
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg.copy(yauaa = None),
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete
      )

      enriched.value.map {
        case Ior.Right(enriched) =>
          val p = EnrichedEvent.toPartiallyEnrichedEvent(enriched)
          val contextsJson = jparse(p.contexts.get).toOption.get
          val derivedContextsJson = jparse(p.derived_contexts.get).toOption.get
          val ueJson = jparse(p.unstruct_event.get).toOption.get
          (contextsJson must beEqualTo(expectedContexts)) and
            (derivedContextsJson must beEqualTo(expectedDerivedContexts)) and
            (ueJson must beEqualTo(expectedUnstructEvent))
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "remove the invalid unstructured event and enrich the event if emitIncomplete is set to true" >> {
      val script =
        s"""
        function process(event) {
          return [ ${emailSent} ];
        }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "javascript_script_config",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      val enrichmentReg = EnrichmentRegistry[IO](
        javascriptScript = List(
          JavascriptScriptEnrichment(schemaKey, script)
        )
      )

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          s"""
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              $clientSession
            ]
          }
        """,
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "unallowedAdditionalField": "foo@bar.org"
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete = true
      )
      enriched.value.map {
        case Ior.Both(_: BadRow.SchemaViolations, enriched)
            if Option(enriched.unstruct_event).isEmpty &&
              SpecHelpers.listContextsSchemas(enriched.contexts) == List(clientSessionSchema) &&
              SpecHelpers.listContextsSchemas(enriched.derived_contexts).contains(emailSentSchema) =>
          ok
        case other => ko(s"[$other] is not a SchemaViolations bad row and an enriched event without the unstructured event")
      }
    }

    "remove the invalid context and enrich the event if emitIncomplete is set to true" >> {
      val script =
        s"""
        function process(event) {
          return [ ${emailSent} ];
        }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "javascript_script_config",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      val enrichmentReg = EnrichmentRegistry[IO](
        javascriptScript = List(
          JavascriptScriptEnrichment(schemaKey, script)
        )
      )

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
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
        """,
        "ue_pr" ->
          s"""
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data": $clientSession
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete = true
      )
      enriched.value.map {
        case Ior.Both(_: BadRow.SchemaViolations, enriched)
            if Option(enriched.contexts).isEmpty &&
              SpecHelpers.getUnstructSchema(enriched.unstruct_event) == clientSessionSchema &&
              SpecHelpers.listContextsSchemas(enriched.derived_contexts).contains(emailSentSchema) =>
          ok
        case other => ko(s"[$other] is not a SchemaViolations bad row and an enriched event with no input contexts")
      }
    }

    "remove one invalid context (out of 2) and enrich the event if emitIncomplete is set to true" >> {
      val script =
        s"""
        function process(event) {
          return [ ${emailSent} ];
        }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "javascript_script_config",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      val enrichmentReg = EnrichmentRegistry[IO](
        javascriptScript = List(
          JavascriptScriptEnrichment(schemaKey, script)
        )
      )

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          s"""
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "foo": "hello@world.com",
                  "emailAddress2": "foo@bar.org"
                }
              },
              $clientSession
            ]
          }
        """,
        "ue_pr" ->
          s"""
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data": $clientSession
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete = true
      )
      enriched.value.map {
        case Ior.Both(_: BadRow.SchemaViolations, enriched)
            if SpecHelpers.getUnstructSchema(enriched.unstruct_event) == clientSessionSchema &&
              SpecHelpers.listContextsSchemas(enriched.contexts) == List(clientSessionSchema) &&
              SpecHelpers.listContextsSchemas(enriched.derived_contexts).contains(emailSentSchema) =>
          ok
        case other => ko(s"[$other] is not a SchemaViolations bad row and an enriched event with 1 input context")
      }
    }

    "return the enriched event after an enrichment exception if emitIncomplete is set to true" >> {
      val script =
        s"""
        function process(event) {
          throw "Javascript exception";
          return [ $emailSent ];
        }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "javascript_script_config",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      val enrichmentReg = EnrichmentRegistry[IO](
        yauaa = Some(YauaaEnrichment(None)),
        javascriptScript = List(
          JavascriptScriptEnrichment(schemaKey, script)
        )
      )

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "ue_pr" ->
          s"""
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data": $clientSession
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete = true
      )
      enriched.value.map {
        case Ior.Both(_: BadRow.EnrichmentFailures, enriched)
            if SpecHelpers.getUnstructSchema(enriched.unstruct_event) == clientSessionSchema &&
              !SpecHelpers.listContextsSchemas(enriched.derived_contexts).contains(emailSentSchema) =>
          ok
        case other => ko(s"[$other] is not an EnrichmentFailures bad row and an enriched event")
      }
    }

    "return a SchemaViolations bad row in the Left in case of both a schema violation and an enrichment failure if emitIncomplete is set to true" >> {
      val script =
        s"""
        function process(event) {
          throw "Javascript exception";
          return [ $emailSent ];
        }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "javascript_script_config",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      val enrichmentReg = EnrichmentRegistry[IO](
        javascriptScript = List(
          JavascriptScriptEnrichment(schemaKey, script)
        )
      )

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "tr_tt" -> "foo",
        "ue_pr" ->
          s"""
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data": $clientSession
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete = true
      )
      enriched.value.map {
        case Ior.Both(_: BadRow.SchemaViolations, _) => ok
        case other => ko(s"[$other] doesn't have a SchemaViolations bad row in the Left")
      }
    }

    "remove an invalid enrichment context and return the enriched event if emitIncomplete is set to true" >> {
      val script =
        s"""
        function process(event) {
          return [
            {
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "foo": "hello@world.com",
                "emailAddress2": "foo@bar.org"
              }
            }
          ];
        }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "javascript_script_config",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      val enrichmentReg = EnrichmentRegistry[IO](
        yauaa = Some(YauaaEnrichment(None)),
        javascriptScript = List(
          JavascriptScriptEnrichment(schemaKey, script)
        )
      )

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "ue_pr" ->
          s"""
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data": $clientSession
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitIncomplete = true
      )
      enriched.value.map {
        case Ior.Both(_: BadRow.SchemaViolations, enriched)
            if SpecHelpers.getUnstructSchema(enriched.unstruct_event) == clientSessionSchema &&
              !SpecHelpers.listContextsSchemas(enriched.derived_contexts).contains(emailSentSchema) =>
          ok
        case other => ko(s"[$other] is not a SchemaViolations bad row and an enriched event without the faulty enrichment context")
      }
    }
  }

  "getCrossDomain" should {
    val schemaKey = SchemaKey(
      CrossNavigationEnrichment.supportedSchema.vendor,
      CrossNavigationEnrichment.supportedSchema.name,
      CrossNavigationEnrichment.supportedSchema.format,
      SchemaVer.Full(1, 0, 0)
    )

    "do nothing if none query string parameters - crossNavigation enabled" >> {
      val crossNavigationEnabled = Some(new CrossNavigationEnrichment(schemaKey))
      val qsMap: Option[QueryStringParameters] = None
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationEnabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beNone) and
                (p.refr_dvce_tstamp must beNone) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEmpty)
          }
        )
    }

    "do nothing if none query string parameters - crossNavigation disabled" >> {
      val crossNavigationDisabled = None
      val qsMap: Option[QueryStringParameters] = None
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationDisabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beNone) and
                (p.refr_dvce_tstamp must beNone) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEmpty)
          }
        )
    }

    "do nothing if _sp is empty - crossNavigation enabled" >> {
      val crossNavigationEnabled = Some(new CrossNavigationEnrichment(schemaKey))
      val qsMap: Option[QueryStringParameters] = Some(List(("_sp" -> Some(""))))
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationEnabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beNone) and
                (p.refr_dvce_tstamp must beNone) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEmpty)
          }
        )
    }

    "do nothing if _sp is empty - crossNavigation disabled" >> {
      val crossNavigationDisabled = None
      val qsMap: Option[QueryStringParameters] = Some(List(("_sp" -> Some(""))))
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationDisabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beNone) and
                (p.refr_dvce_tstamp must beNone) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEmpty)
          }
        )
    }

    "add atomic props and ctx with original _sp format and cross navigation enabled" >> {
      val crossNavigationEnabled = Some(new CrossNavigationEnrichment(schemaKey))
      val qsMap: Option[QueryStringParameters] = Some(
        List(
          ("_sp" -> Some("abc.1697175843762"))
        )
      )
      val expectedRefrDuid = Some("abc")
      val expectedRefrTstamp = Some("2023-10-13 05:44:03.762")
      val expectedCtx: List[SelfDescribingData[Json]] = List(
        SelfDescribingData(
          CrossNavigationEnrichment.outputSchema,
          Map(
            "domain_user_id" -> Some("abc"),
            "timestamp" -> Some("2023-10-13T05:44:03.762Z"),
            "session_id" -> None,
            "user_id" -> None,
            "source_id" -> None,
            "source_platform" -> None,
            "reason" -> None
          ).asJson
        )
      )
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationEnabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beEqualTo(expectedRefrDuid)) and
                (p.refr_dvce_tstamp must beEqualTo(expectedRefrTstamp)) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEqualTo(expectedCtx))
          }
        )
    }

    "add atomic props but no ctx with original _sp format and cross navigation disabled" >> {
      val crossNavigationDisabled = None
      val qsMap: Option[QueryStringParameters] = Some(
        List(
          ("_sp" -> Some("abc.1697175843762"))
        )
      )
      val expectedRefrDuid = Some("abc")
      val expectedRefrTstamp = Some("2023-10-13 05:44:03.762")
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationDisabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beEqualTo(expectedRefrDuid)) and
                (p.refr_dvce_tstamp must beEqualTo(expectedRefrTstamp)) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEmpty)
          }
        )
    }

    "add atomic props and ctx with extended _sp format and cross navigation enabled" >> {
      val crossNavigationEnabled = Some(new CrossNavigationEnrichment(schemaKey))
      val qsMap: Option[QueryStringParameters] = Some(
        List(
          ("_sp" -> Some("abc.1697175843762.176ff68a-4769-4566-ad0e-3792c1c8148f.dGVzdGVy.c29tZVNvdXJjZUlk.web.dGVzdGluZ19yZWFzb24"))
        )
      )
      val expectedRefrDuid = Some("abc")
      val expectedRefrTstamp = Some("2023-10-13 05:44:03.762")
      val expectedCtx: List[SelfDescribingData[Json]] = List(
        SelfDescribingData(
          CrossNavigationEnrichment.outputSchema,
          Map(
            "domain_user_id" -> Some("abc"),
            "timestamp" -> Some("2023-10-13T05:44:03.762Z"),
            "session_id" -> Some("176ff68a-4769-4566-ad0e-3792c1c8148f"),
            "user_id" -> Some("tester"),
            "source_id" -> Some("someSourceId"),
            "source_platform" -> Some("web"),
            "reason" -> Some("testing_reason")
          ).asJson
        )
      )
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationEnabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beEqualTo(expectedRefrDuid)) and
                (p.refr_dvce_tstamp must beEqualTo(expectedRefrTstamp)) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEqualTo(expectedCtx))
          }
        )
    }

    "add atomic props but no ctx with extended _sp format and cross navigation disabled" >> {
      val crossNavigationDisabled = None
      val qsMap: Option[QueryStringParameters] = Some(
        List(
          ("_sp" -> Some("abc.1697175843762.176ff68a-4769-4566-ad0e-3792c1c8148f.dGVzdGVy.c29tZVNvdXJjZUlk.web.dGVzdGluZ19yZWFzb24"))
        )
      )
      val expectedRefrDuid = Some("abc")
      val expectedRefrTstamp = Some("2023-10-13 05:44:03.762")
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationDisabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beEqualTo(expectedRefrDuid)) and
                (p.refr_dvce_tstamp must beEqualTo(expectedRefrTstamp)) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEmpty)
          }
        )
    }

    "error with info if parsing failed and cross navigation is enabled" >> {
      val crossNavigationEnabled = Some(new CrossNavigationEnrichment(schemaKey))
      // causing a parsing failure by providing invalid tstamp
      val qsMap: Option[QueryStringParameters] = Some(
        List(
          ("_sp" -> Some("abc.some_invalid_timestamp_value"))
        )
      )
      val input = new EnrichedEvent()
      val expectedFail = FailureDetails.EnrichmentFailure(
        FailureDetails
          .EnrichmentInformation(
            schemaKey,
            "cross-navigation"
          )
          .some,
        FailureDetails.EnrichmentFailureMessage.InputData(
          "sp_dtm",
          "some_invalid_timestamp_value".some,
          "Not in the expected format: ms since epoch"
        )
      )
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationEnabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation =>
              (acc.errors must not beEmpty) and
                (acc.errors must beEqualTo(List(expectedFail))) and
                (acc.contexts must beEmpty)
          }
        )
    }

    "error without info if parsing failed and cross navigation is disabled" >> {
      val crossNavigationDisabled = None
      // causing a parsing failure by providing invalid tstamp
      val qsMap: Option[QueryStringParameters] = Some(
        List(
          ("_sp" -> Some("abc.some_invalid_timestamp_value"))
        )
      )
      val input = new EnrichedEvent()
      val expectedFail = FailureDetails.EnrichmentFailure(
        None,
        FailureDetails.EnrichmentFailureMessage.InputData(
          "sp_dtm",
          "some_invalid_timestamp_value".some,
          "Not in the expected format: ms since epoch"
        )
      )
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationDisabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation =>
              (acc.errors must not beEmpty) and
                (acc.errors must beEqualTo(List(expectedFail))) and
                (acc.contexts must beEmpty)
          }
        )
    }
  }

  "getIabContext" should {
    "return no context if useragent is null" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("127.0.0.1")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      for {
        iab <- iabEnrichment
        result <- EnrichmentManager
                    .getIabContext[IO](Some(iab))
                    .runS(inputState)
      } yield result must beLike {
        case acc: EnrichmentManager.Accumulation =>
          (acc.errors must beEmpty) and (acc.contexts must beEmpty)
      }
    }

    "return no context if user_ipaddress is null" >> {
      val input = new EnrichedEvent()
      input.setUseragent("Firefox")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      for {
        iab <- iabEnrichment
        result <- EnrichmentManager
                    .getIabContext[IO](Some(iab))
                    .runS(inputState)
      } yield result must beLike {
        case acc: EnrichmentManager.Accumulation =>
          (acc.errors must beEmpty) and (acc.contexts must beEmpty)
      }
    }

    "return no context if derived_tstamp is null" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("127.0.0.1")
      input.setUseragent("Firefox")
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      for {
        iab <- iabEnrichment
        result <- EnrichmentManager
                    .getIabContext[IO](Some(iab))
                    .runS(inputState)
      } yield result must beLike {
        case acc: EnrichmentManager.Accumulation =>
          (acc.errors must beEmpty) and (acc.contexts must beEmpty)
      }
    }

    "return no context if user_ipaddress is invalid" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("invalid")
      input.setUseragent("Firefox")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      for {
        iab <- iabEnrichment
        result <- EnrichmentManager
                    .getIabContext[IO](Some(iab))
                    .runS(inputState)
      } yield result must beLike {
        case acc: EnrichmentManager.Accumulation =>
          (acc.errors must beEmpty) and (acc.contexts must beEmpty)
      }
    }

    "return no context if user_ipaddress is hostname (don't try to resovle it)" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("localhost")
      input.setUseragent("Firefox")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      for {
        iab <- iabEnrichment
        result <- EnrichmentManager
                    .getIabContext[IO](Some(iab))
                    .runS(inputState)
      } yield result must beLike {
        case acc: EnrichmentManager.Accumulation =>
          (acc.errors must beEmpty) and (acc.contexts must beEmpty)
      }
    }

    "return Some if all arguments are valid" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("127.0.0.1")
      input.setUseragent("Firefox")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      for {
        iab <- iabEnrichment
        result <- EnrichmentManager
                    .getIabContext[IO](Some(iab))
                    .runS(inputState)
      } yield result must beLike {
        case acc: EnrichmentManager.Accumulation =>
          (acc.errors must beEmpty) and (acc.contexts must not beEmpty)
      }
    }
  }

  "getCollectorVersionSet" should {
    "return an enrichment failure if v_collector is null" >> {
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      EnrichmentManager
        .getCollectorVersionSet[IO]
        .runS(inputState)
        .map(_ must beLike {
          case acc: EnrichmentManager.Accumulation =>
            acc.errors must not beEmpty
        })
    }

    "return an enrichment failure if v_collector is empty" >> {
      val input = new EnrichedEvent()
      input.v_collector = ""
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      EnrichmentManager
        .getCollectorVersionSet[IO]
        .runS(inputState)
        .map(_ must beLike {
          case acc: EnrichmentManager.Accumulation =>
            acc.errors must not beEmpty
        })
    }

    "return Unit if v_collector is set" >> {
      val input = new EnrichedEvent()
      input.v_collector = "v42"
      val inputState = EnrichmentManager.Accumulation(input, Nil, Nil)
      EnrichmentManager
        .getCollectorVersionSet[IO]
        .runS(inputState)
        .map(_ must beLike {
          case acc: EnrichmentManager.Accumulation =>
            acc.errors must beEmpty
        })
    }
  }

  "validateEnriched" should {
    "create a SchemaViolations bad row if an atomic field is oversized" >> {
      EnrichmentManager
        .enrichEvent[IO](
          enrichmentReg,
          client,
          processor,
          timestamp,
          RawEvent(api, fatBody, None, source, context),
          featureFlags = AcceptInvalid.featureFlags.copy(acceptInvalid = false),
          IO.unit,
          SpecHelpers.registryLookup,
          atomicFieldLimits,
          emitIncomplete
        )
        .value
        .map {
          case Ior.Left(
                BadRow.SchemaViolations(
                  _,
                  Failure.SchemaViolations(_, NonEmptyList(FailureDetails.SchemaViolation.IgluError(schemaKey, clientError), Nil)),
                  _
                )
              ) =>
            schemaKey must beEqualTo(AtomicFields.atomicSchema)
            clientError.toString must contain("v_tracker")
            clientError.toString must contain("Field is longer than maximum allowed size")
          case other =>
            ko(s"[$other] is not a SchemaViolations bad row with one IgluError")
        }
    }

    "not create a bad row if an atomic field is oversized and acceptInvalid is set to true" >> {
      EnrichmentManager
        .enrichEvent[IO](
          enrichmentReg,
          client,
          processor,
          timestamp,
          RawEvent(api, fatBody, None, source, context),
          featureFlags = AcceptInvalid.featureFlags.copy(acceptInvalid = true),
          IO.unit,
          SpecHelpers.registryLookup,
          atomicFieldLimits,
          emitIncomplete
        )
        .value
        .map {
          case Ior.Right(_) => ok
          case other => ko(s"[$other] is not an enriched event")
        }
    }

    "return a SchemaViolations bad row containing both the atomic field length error and the invalid enrichment context error" >> {
      val script =
        """
        function process(event) {
          return [ { schema: "iglu:com.acme/email_sent/jsonschema/1-0-0",
                     data: {
                       emailAddress: "hello@world.com",
                       foo: "bar"
                     }
                   } ];
        }"""

      val config =
        json"""{
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
      val enrichmentReg = EnrichmentRegistry[IO](javascriptScript = List(jsEnrich))

      val rawEvent = RawEvent(api, fatBody, None, source, context)
      EnrichmentManager
        .enrichEvent[IO](
          enrichmentReg,
          client,
          processor,
          timestamp,
          rawEvent,
          AcceptInvalid.featureFlags,
          IO.unit,
          SpecHelpers.registryLookup,
          atomicFieldLimits,
          emitIncomplete
        )
        .value
        .map {
          case Ior.Left(
                BadRow.SchemaViolations(
                  _,
                  Failure.SchemaViolations(_,
                                           NonEmptyList(FailureDetails.SchemaViolation.IgluError(schemaKey1, clientError1),
                                                        List(FailureDetails.SchemaViolation.IgluError(schemaKey2, clientError2))
                                           )
                  ),
                  _
                )
              ) =>
            schemaKey1 must beEqualTo(emailSentSchema)
            clientError1.toString must contain("emailAddress2: is missing but it is required")
            schemaKey2 must beEqualTo(AtomicFields.atomicSchema)
            clientError2.toString must contain("v_tracker")
            clientError2.toString must contain("Field is longer than maximum allowed size")
          case other =>
            ko(s"[$other] is not a SchemaViolations bad row with 2 IgluError")
        }
    }
  }
}

object EnrichmentManagerSpec {

  val enrichmentReg = EnrichmentRegistry[IO](yauaa = Some(YauaaEnrichment(None)))
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

  val atomicFieldLimits = AtomicFields.from(Map("v_tracker" -> 100))

  val leanBody = Map(
    "e" -> "pp",
    "tv" -> "js-0.13.1",
    "p" -> "web"
  ).toOpt

  val fatBody = Map(
    "e" -> "pp",
    "tv" -> s"${"s" * 500}",
    "p" -> "web"
  ).toOpt

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
    .enrichment[IO]

  val emailSentSchema =
    SchemaKey(
      "com.acme",
      "email_sent",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )

  val emailSent = s"""{
    "schema": "${emailSentSchema.toSchemaUri}",
    "data": {
      "emailAddress": "hello@world.com",
      "emailAddress2": "foo@bar.org"
    }
  }"""

  val clientSessionSchema =
    SchemaKey(
      "com.snowplowanalytics.snowplow",
      "client_session",
      "jsonschema",
      SchemaVer.Full(1, 0, 1)
    )

  val clientSession = s"""{
    "schema": "${clientSessionSchema.toSchemaUri}",
    "data": {
      "sessionIndex": 1,
      "storageMechanism": "LOCAL_STORAGE",
      "firstEventId": "5c33fccf-6be5-4ce6-afb1-e34026a3ca75",
      "sessionId": "21c2a0dd-892d-42d1-b156-3a9d4e147eef",
      "previousSessionId": null,
      "userId": "20d631b8-7837-49df-a73e-6da73154e6fd"
    }
  }"""
}
