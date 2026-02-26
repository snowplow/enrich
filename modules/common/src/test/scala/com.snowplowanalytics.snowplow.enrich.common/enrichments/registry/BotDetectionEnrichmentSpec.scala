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

import io.circe.Json
import io.circe.literal._

import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.BotDetectionConf

class BotDetectionEnrichmentSpec extends Specification with ValidatedMatchers {

  def is = s2"""
  BotDetectionEnrichment
    detect bot from YAUAA when only deviceClass=Robot                 $yauaaDeviceClassRobot
    detect bot from YAUAA when deviceClass=Robot Mobile               $yauaaDeviceClassRobotMobile
    detect bot from YAUAA when deviceClass=Robot Imitator             $yauaaDeviceClassRobotImitator
    detect bot from YAUAA when only agentClass=Robot                 $yauaaAgentClassRobot
    detect bot from YAUAA when agentClass=Robot Mobile                $yauaaAgentClassRobotMobile
    not detect bot from YAUAA when classes are not Robot              $yauaaNonRobot
    detect bot from IAB spiderOrRobot=true                           $iabSpider
    not detect bot from IAB spiderOrRobot=false                      $iabNonSpider
    combine multiple indicators                                      $multipleIndicators
    YAUAA positive overrides IAB negative                            $yauaaPositiveIabNegative
    IAB positive overrides YAUAA negative                            $iabPositiveYauaaNegative
    return bot=false when no signals detected                        $noSignals
    handle YAUAA context with missing bot fields                     $yauaaMissingFields
    skip YAUAA check when useYauaa=false                             $skipYauaa
    skip IAB check when useIab=false                                 $skipIab
    handle missing contexts gracefully                               $missingContexts
    detect bot from ASN likelyBot=true                               $asnLikelyBot
    not detect bot from ASN likelyBot=false                          $asnNotLikelyBot
    combine all three indicators when positive                       $allThreeIndicators
    ASN positive overrides YAUAA/IAB negative                        $asnPositiveOthersNegative
    skip ASN check when useAsnLookups=false                          $skipAsn
    handle ASN context without likelyBot field                       $asnMissingLikelyBot
    parse valid configuration                                        $parseValidConfig
    reject configuration with wrong schema                           $parseWrongSchema
  """

  private val yauaaSchema = YauaaEnrichment.outputSchema
  private val iabSchema = IabEnrichment.outputSchema
  private val asnSchema = IpLookupsEnrichment.asnSchema

  private def mkYauaaContext(deviceClass: String, agentClass: String): SelfDescribingData[Json] =
    SelfDescribingData(
      yauaaSchema,
      json"""{"deviceClass": $deviceClass, "agentClass": $agentClass}"""
    )

  private def mkIabContext(spiderOrRobot: Boolean): SelfDescribingData[Json] =
    SelfDescribingData(
      iabSchema,
      json"""{"spiderOrRobot": $spiderOrRobot, "category": "BROWSER", "reason": "PASSED_ALL", "primaryImpact": "NONE"}"""
    )

  private def mkAsnContext(likelyBot: Boolean): SelfDescribingData[Json] =
    SelfDescribingData(
      asnSchema,
      json"""{"asn": 12345, "organization": "Example ISP", "likelyBot": $likelyBot}"""
    )

  private val allEnabled = BotDetectionEnrichment(useYauaa = true, useIab = true, useAsnLookups = true)

  def yauaaDeviceClassRobot = {
    val contexts = List(mkYauaaContext("Robot", "Browser"))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("yauaa")))
  }

  def yauaaDeviceClassRobotMobile = {
    val contexts = List(mkYauaaContext("Robot Mobile", "Browser"))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("yauaa")))
  }

  def yauaaDeviceClassRobotImitator = {
    val contexts = List(mkYauaaContext("Robot Imitator", "Browser"))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("yauaa")))
  }

  def yauaaAgentClassRobot = {
    val contexts = List(mkYauaaContext("Desktop", "Robot"))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("yauaa")))
  }

  def yauaaAgentClassRobotMobile = {
    val contexts = List(mkYauaaContext("Desktop", "Robot Mobile"))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("yauaa")))
  }

  def yauaaNonRobot = {
    val contexts = List(mkYauaaContext("Desktop", "Browser"))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(Nil))
  }

  def iabSpider = {
    val contexts = List(mkIabContext(true))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("iab")))
  }

  def iabNonSpider = {
    val contexts = List(mkIabContext(false))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(Nil))
  }

  def multipleIndicators = {
    val contexts = List(mkYauaaContext("Robot", "Robot"), mkIabContext(true))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("yauaa", "iab")))
  }

  def yauaaPositiveIabNegative = {
    val contexts = List(mkYauaaContext("Robot", "Browser"), mkIabContext(false))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("yauaa")))
  }

  def iabPositiveYauaaNegative = {
    val contexts = List(mkYauaaContext("Desktop", "Browser"), mkIabContext(true))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("iab")))
  }

  def noSignals = {
    val contexts = List(mkYauaaContext("Desktop", "Browser"), mkIabContext(false))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(Nil))
  }

  def yauaaMissingFields = {
    val yauaaCtx = SelfDescribingData(
      yauaaSchema,
      json"""{"deviceName": "Phone", "operatingSystemClass": "Mobile"}"""
    )
    val result = allEnabled.getBotDetectionContext(List(yauaaCtx))
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(Nil))
  }

  def skipYauaa = {
    val enrichment = BotDetectionEnrichment(useYauaa = false, useIab = true, useAsnLookups = false)
    val contexts = List(mkYauaaContext("Robot", "Robot"))
    val result = enrichment.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(Nil))
  }

  def skipIab = {
    val enrichment = BotDetectionEnrichment(useYauaa = true, useIab = false, useAsnLookups = false)
    val contexts = List(mkIabContext(true))
    val result = enrichment.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(Nil))
  }

  def missingContexts = {
    val result = allEnabled.getBotDetectionContext(Nil)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(Nil))
  }

  def asnLikelyBot = {
    val contexts = List(mkAsnContext(true))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("asnLookups")))
  }

  def asnNotLikelyBot = {
    val contexts = List(mkAsnContext(false))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(Nil))
  }

  def allThreeIndicators = {
    val contexts = List(mkYauaaContext("Robot", "Robot"), mkIabContext(true), mkAsnContext(true))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("yauaa", "iab", "asnLookups")))
  }

  def asnPositiveOthersNegative = {
    val contexts = List(mkYauaaContext("Desktop", "Browser"), mkIabContext(false), mkAsnContext(true))
    val result = allEnabled.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("asnLookups")))
  }

  def skipAsn = {
    val enrichment = BotDetectionEnrichment(useYauaa = false, useIab = false, useAsnLookups = false)
    val contexts = List(mkAsnContext(true))
    val result = enrichment.getBotDetectionContext(contexts)
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(Nil))
  }

  def asnMissingLikelyBot = {
    val asnCtx = SelfDescribingData(
      asnSchema,
      json"""{"asn": 12345, "organization": "Example ISP"}"""
    )
    val result = allEnabled.getBotDetectionContext(List(asnCtx))
    val data = result.head.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(Nil))
  }

  def parseValidConfig = {
    val config = json"""{
      "enabled": true,
      "parameters": {
        "useYauaa": true,
        "useIab": true,
        "useAsnLookups": false
      }
    }"""
    val schemaKey = SchemaKey(
      "com.snowplowanalytics.snowplow.enrichments",
      "bot_detection_enrichment_config",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )
    val result = BotDetectionEnrichment.parse(config, schemaKey)
    result must beValid(BotDetectionConf(schemaKey, useYauaa = true, useIab = true, useAsnLookups = false))
  }

  def parseWrongSchema = {
    val config = json"""{
      "enabled": true,
      "parameters": {
        "useYauaa": true,
        "useIab": true,
        "useAsnLookups": false
      }
    }"""
    val schemaKey = SchemaKey(
      "com.snowplowanalytics.snowplow.enrichments",
      "wrong_enrichment_config",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )
    val result = BotDetectionEnrichment.parse(config, schemaKey)
    result must beInvalid
  }
}
