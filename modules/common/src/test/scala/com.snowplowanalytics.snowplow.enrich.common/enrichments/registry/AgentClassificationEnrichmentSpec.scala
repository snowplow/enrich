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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import cats.data.EitherT
import cats.syntax.either._

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect

import io.circe.Json
import io.circe.literal._

import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.AgentClassificationEnrichment.{
  AgentClassification,
  AgentClassificationOverride
}

class AgentClassificationEnrichmentSpec extends Specification with ValidatedMatchers with CatsEffect {

  def is = s2"""
  AgentClassificationEnrichment.getAgentClassificationContext should
    attach the entity when the YAUAA agentName matches a record           $matchKnownAgent
    match case-insensitively against YAUAA agentName                     $matchCaseInsensitive
    omit purpose when the record has no purpose                          $matchNoPurpose
    attach nothing when the agentName does not match                      $noMatch
    attach nothing when the YAUAA context has no agentName                $noAgentName
    attach nothing when there are no classifications loaded               $noClassifications
  AgentClassification decoder should
    decode a record with an absent purpose field to None                  $decodeAbsentPurpose
    decode a record with an explicit null purpose to None                 $decodeNullPurpose
  AgentClassificationEnrichment.merge should
    let an override replace the operator of a file record                 $overrideOperator
    let an override add a purpose to a file record                        $overrideAddPurpose
    let an override introduce a brand new agent                           $overrideNewAgent
    drop an override that has a purpose but no operator anywhere          $overridePurposeOnlyDropped
  AgentClassificationEnrichment.parse should
    parse a config with inline classification only                        $parseInlineOnly
    parse a config with a classification file                             $parseFile
    merge the classification file with the inline overrides               $parseMerge
    reject a config with the wrong schema                                 $parseWrongSchema
    reject an override that specifies neither operator nor purpose        $parseInvalidOverride
  """

  private val schemaKey = SchemaKey(
    "com.snowplowanalytics.snowplow.enrichments",
    "agent_classification_enrichment_config",
    "jsonschema",
    SchemaVer.Full(1, 0, 0)
  )

  private val yauaaSchema = YauaaEnrichment.outputSchema

  private def mkYauaaContext(agentName: String): SelfDescribingData[Json] =
    SelfDescribingData(yauaaSchema, json"""{"agentName": $agentName, "deviceClass": "Robot"}""")

  private val enrichment =
    AgentClassificationEnrichment(
      Map(
        "chatgpt-user" -> AgentClassification("OpenAI", Some("AI data retrieval")),
        "claudebot" -> AgentClassification("Anthropic", None)
      )
    )

  // --- getAgentClassificationContext ---

  def matchKnownAgent = {
    val result = enrichment.getAgentClassificationContext(mkYauaaContext("ChatGPT-User"))
    result must beLike {
      case List(sdj) =>
        (sdj.schema must beEqualTo(AgentClassificationEnrichment.outputSchema)) and
          (sdj.data.hcursor.get[String]("operator") must beRight("OpenAI")) and
          (sdj.data.hcursor.get[Option[String]]("purpose") must beRight(Option("AI data retrieval")))
    }
  }

  def matchCaseInsensitive = {
    val result = enrichment.getAgentClassificationContext(mkYauaaContext("CHATGPT-USER"))
    result must beLike {
      case List(sdj) =>
        (sdj.data.hcursor.get[String]("operator") must beRight("OpenAI")) and
          (sdj.data.hcursor.get[Option[String]]("purpose") must beRight(Option("AI data retrieval")))
    }
  }

  def matchNoPurpose = {
    val result = enrichment.getAgentClassificationContext(mkYauaaContext("ClaudeBot"))
    result must beLike {
      case List(sdj) =>
        (sdj.data.hcursor.get[String]("operator") must beRight("Anthropic")) and
          (sdj.data.hcursor.keys.toList.flatten must not(contain("purpose")))
    }
  }

  def noMatch =
    enrichment.getAgentClassificationContext(mkYauaaContext("Firefox")) must beEmpty

  def noAgentName =
    enrichment.getAgentClassificationContext(SelfDescribingData(yauaaSchema, json"""{"deviceClass": "Robot"}""")) must beEmpty

  def noClassifications =
    AgentClassificationEnrichment(Map.empty).getAgentClassificationContext(mkYauaaContext("ChatGPT-User")) must beEmpty

  // --- AgentClassification decoder (config file parsing) ---

  def decodeAbsentPurpose =
    json"""{"operator": "OpenAI"}""".as[AgentClassification] must beRight(AgentClassification("OpenAI", None))

  def decodeNullPurpose =
    json"""{"operator": "OpenAI", "purpose": null}""".as[AgentClassification] must beRight(AgentClassification("OpenAI", None))

  // --- merge ---

  private val fileRecords = Map(
    "ChatGPT-User" -> AgentClassification("OpenAI", Some("AI data retrieval")),
    "ClaudeBot" -> AgentClassification("Anthropic", None)
  )

  def overrideOperator = {
    val merged = AgentClassificationEnrichment.merge(
      fileRecords,
      Map("ChatGPT-User" -> AgentClassificationOverride(Some("OpenAI Inc."), None))
    )
    merged.get("chatgpt-user") must beSome(AgentClassification("OpenAI Inc.", Some("AI data retrieval")))
  }

  def overrideAddPurpose = {
    val merged = AgentClassificationEnrichment.merge(
      fileRecords,
      Map("ClaudeBot" -> AgentClassificationOverride(None, Some("AI crawler")))
    )
    merged.get("claudebot") must beSome(AgentClassification("Anthropic", Some("AI crawler")))
  }

  def overrideNewAgent = {
    val merged = AgentClassificationEnrichment.merge(
      fileRecords,
      Map("PerplexityBot" -> AgentClassificationOverride(Some("Perplexity"), Some("AI crawler")))
    )
    merged.get("perplexitybot") must beSome(AgentClassification("Perplexity", Some("AI crawler")))
  }

  def overridePurposeOnlyDropped = {
    val merged = AgentClassificationEnrichment.merge(
      fileRecords,
      Map("UnknownBot" -> AgentClassificationOverride(None, Some("AI crawler")))
    )
    merged.get("unknownbot") must beNone
  }

  // --- parse ---

  private def parseConf(config: Json, localMode: Boolean) =
    EitherT.fromEither[IO](
      AgentClassificationEnrichment.parse(config, schemaKey, localMode).toEither.leftMap(_.head)
    )

  def parseInlineOnly = {
    val config = json"""{
      "name": "agent_classification_enrichment_config",
      "vendor": "com.snowplowanalytics.snowplow.enrichments",
      "enabled": true,
      "parameters": {
        "classification": {
          "ChatGPT-User": {"operator": "OpenAI", "purpose": "AI data retrieval"}
        }
      }
    }"""
    (for {
      conf <- parseConf(config, localMode = false)
      enr <- conf.enrichment[IO]
    } yield enr.classifications).value.map { result =>
      result must beRight(Map("chatgpt-user" -> AgentClassification("OpenAI", Some("AI data retrieval"))))
    }
  }

  def parseFile = {
    val config = json"""{
      "name": "agent_classification_enrichment_config",
      "vendor": "com.snowplowanalytics.snowplow.enrichments",
      "enabled": true,
      "parameters": {
        "classificationFile": {
          "uri": "http://example.com",
          "database": "agent-classification-test.json"
        }
      }
    }"""
    (for {
      conf <- parseConf(config, localMode = true)
      enr <- conf.enrichment[IO]
    } yield enr.classifications).value.map { result =>
      result must beRight(
        Map(
          "chatgpt-user" -> AgentClassification("OpenAI", Some("AI data retrieval")),
          "gptbot" -> AgentClassification("OpenAI", Some("AI crawler")),
          "claudebot" -> AgentClassification("Anthropic", None)
        )
      )
    }
  }

  def parseMerge = {
    val config = json"""{
      "name": "agent_classification_enrichment_config",
      "vendor": "com.snowplowanalytics.snowplow.enrichments",
      "enabled": true,
      "parameters": {
        "classificationFile": {
          "uri": "http://example.com",
          "database": "agent-classification-test.json"
        },
        "classification": {
          "ClaudeBot": {"purpose": "AI assistant"},
          "PerplexityBot": {"operator": "Perplexity", "purpose": "AI search"}
        }
      }
    }"""
    (for {
      conf <- parseConf(config, localMode = true)
      enr <- conf.enrichment[IO]
    } yield enr.classifications).value.map { result =>
      result must beRight.like {
        case classifications =>
          (classifications.get("claudebot") must beSome(AgentClassification("Anthropic", Some("AI assistant")))) and
            (classifications.get("perplexitybot") must beSome(AgentClassification("Perplexity", Some("AI search")))) and
            (classifications.get("chatgpt-user") must beSome(AgentClassification("OpenAI", Some("AI data retrieval"))))
      }
    }
  }

  def parseWrongSchema = {
    val config = json"""{
      "name": "wrong_enrichment_config",
      "vendor": "com.snowplowanalytics.snowplow.enrichments",
      "enabled": true,
      "parameters": {}
    }"""
    val wrongKey = SchemaKey(
      "com.snowplowanalytics.snowplow.enrichments",
      "wrong_enrichment_config",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )
    AgentClassificationEnrichment.parse(config, wrongKey, localMode = false) must beInvalid
  }

  def parseInvalidOverride = {
    val config = json"""{
      "name": "agent_classification_enrichment_config",
      "vendor": "com.snowplowanalytics.snowplow.enrichments",
      "enabled": true,
      "parameters": {
        "classification": {
          "UnknownBot": {}
        }
      }
    }"""
    AgentClassificationEnrichment.parse(config, schemaKey, localMode = false) must beInvalid
  }
}
