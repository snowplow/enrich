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
    combine all four indicators when positive                        $allFourIndicators
    ASN positive overrides YAUAA/IAB negative                        $asnPositiveOthersNegative
    skip ASN check when useAsnLookups=false                          $skipAsn
    handle ASN context without likelyBot field                       $asnMissingLikelyBot
    parse valid configuration                                        $parseValidConfig
    reject configuration with wrong schema                           $parseWrongSchema
    detect bot from client-side detection bot=true                   $clientSideDetectionBotTrue
    not detect bot from client-side detection bot=false              $clientSideDetectionBotFalse
    skip client-side detection when disabled                         $skipClientSideDetection
    combine client-side detection with derived indicators            $clientSideCombinedWithDerived
    handle client-side detection context without bot field           $clientSideMissingBotField
    parse config with useClientSideDetection                        $parseConfigWithClientSide
    parse config without useClientSideDetection (backwards compat)  $parseConfigWithoutClientSide
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

  private val clientSideSchema =
    SchemaKey("com.snowplowanalytics.snowplow", "client_side_bot_detection", "jsonschema", SchemaVer.Full(1, 0, 0))

  private def mkClientSideContext(bot: Boolean): SelfDescribingData[Json] =
    SelfDescribingData(
      clientSideSchema,
      json"""{"bot": $bot}"""
    )

  private val allEnabled = BotDetectionEnrichment(useYauaa = true, useIab = true, useAsnLookups = true, useClientSideDetection = true)

  def yauaaDeviceClassRobot = {
    val result = allEnabled.getBotDetectionContext(Some(mkYauaaContext("Robot", "Browser")), None, None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("yauaa")))
  }

  def yauaaDeviceClassRobotMobile = {
    val result = allEnabled.getBotDetectionContext(Some(mkYauaaContext("Robot Mobile", "Browser")), None, None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("yauaa")))
  }

  def yauaaDeviceClassRobotImitator = {
    val result = allEnabled.getBotDetectionContext(Some(mkYauaaContext("Robot Imitator", "Browser")), None, None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("yauaa")))
  }

  def yauaaAgentClassRobot = {
    val result = allEnabled.getBotDetectionContext(Some(mkYauaaContext("Desktop", "Robot")), None, None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("yauaa")))
  }

  def yauaaAgentClassRobotMobile = {
    val result = allEnabled.getBotDetectionContext(Some(mkYauaaContext("Desktop", "Robot Mobile")), None, None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("yauaa")))
  }

  def yauaaNonRobot = {
    val result = allEnabled.getBotDetectionContext(Some(mkYauaaContext("Desktop", "Browser")), None, None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List.empty[String]))
  }

  def iabSpider = {
    val result = allEnabled.getBotDetectionContext(None, Some(mkIabContext(true)), None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("iab")))
  }

  def iabNonSpider = {
    val result = allEnabled.getBotDetectionContext(None, Some(mkIabContext(false)), None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List.empty[String]))
  }

  def multipleIndicators = {
    val result = allEnabled.getBotDetectionContext(Some(mkYauaaContext("Robot", "Robot")), Some(mkIabContext(true)), None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("iab", "yauaa")))
  }

  def yauaaPositiveIabNegative = {
    val result = allEnabled.getBotDetectionContext(Some(mkYauaaContext("Robot", "Browser")), Some(mkIabContext(false)), None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("yauaa")))
  }

  def iabPositiveYauaaNegative = {
    val result = allEnabled.getBotDetectionContext(Some(mkYauaaContext("Desktop", "Browser")), Some(mkIabContext(true)), None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("iab")))
  }

  def noSignals = {
    val result = allEnabled.getBotDetectionContext(Some(mkYauaaContext("Desktop", "Browser")), Some(mkIabContext(false)), None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List.empty[String]))
  }

  def yauaaMissingFields = {
    val yauaaCtx = SelfDescribingData(
      yauaaSchema,
      json"""{"deviceName": "Phone", "operatingSystemClass": "Mobile"}"""
    )
    val result = allEnabled.getBotDetectionContext(Some(yauaaCtx), None, None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List.empty[String]))
  }

  def skipYauaa = {
    val enrichment = BotDetectionEnrichment(useYauaa = false, useIab = true, useAsnLookups = false, useClientSideDetection = false)
    val result = enrichment.getBotDetectionContext(Some(mkYauaaContext("Robot", "Robot")), None, None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List.empty[String]))
  }

  def skipIab = {
    val enrichment = BotDetectionEnrichment(useYauaa = true, useIab = false, useAsnLookups = false, useClientSideDetection = false)
    val result = enrichment.getBotDetectionContext(None, Some(mkIabContext(true)), None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List.empty[String]))
  }

  def missingContexts = {
    val result = allEnabled.getBotDetectionContext(None, None, None, Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List.empty[String]))
  }

  def asnLikelyBot = {
    val result = allEnabled.getBotDetectionContext(None, None, Some(mkAsnContext(true)), Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("asnLookups")))
  }

  def asnNotLikelyBot = {
    val result = allEnabled.getBotDetectionContext(None, None, Some(mkAsnContext(false)), Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List.empty[String]))
  }

  def allFourIndicators = {
    val inputContexts = List(mkClientSideContext(true))
    val result = allEnabled.getBotDetectionContext(
      Some(mkYauaaContext("Robot", "Robot")),
      Some(mkIabContext(true)),
      Some(mkAsnContext(true)),
      inputContexts
    )
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("clientSideDetection", "asnLookups", "iab", "yauaa")))
  }

  def asnPositiveOthersNegative = {
    val result = allEnabled.getBotDetectionContext(
      Some(mkYauaaContext("Desktop", "Browser")),
      Some(mkIabContext(false)),
      Some(mkAsnContext(true)),
      Nil
    )
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("asnLookups")))
  }

  def skipAsn = {
    val enrichment = BotDetectionEnrichment(useYauaa = false, useIab = false, useAsnLookups = false, useClientSideDetection = false)
    val result = enrichment.getBotDetectionContext(None, None, Some(mkAsnContext(true)), Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List.empty[String]))
  }

  def asnMissingLikelyBot = {
    val asnCtx = SelfDescribingData(
      asnSchema,
      json"""{"asn": 12345, "organization": "Example ISP"}"""
    )
    val result = allEnabled.getBotDetectionContext(None, None, Some(asnCtx), Nil)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List.empty[String]))
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
    result must beValid(BotDetectionConf(schemaKey, useYauaa = true, useIab = true, useAsnLookups = false, useClientSideDetection = false))
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

  def clientSideDetectionBotTrue = {
    val enrichment = BotDetectionEnrichment(useYauaa = false, useIab = false, useAsnLookups = false, useClientSideDetection = true)
    val inputContexts = List(mkClientSideContext(true))
    val result = enrichment.getBotDetectionContext(None, None, None, inputContexts)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("clientSideDetection")))
  }

  def clientSideDetectionBotFalse = {
    val enrichment = BotDetectionEnrichment(useYauaa = false, useIab = false, useAsnLookups = false, useClientSideDetection = true)
    val inputContexts = List(mkClientSideContext(false))
    val result = enrichment.getBotDetectionContext(None, None, None, inputContexts)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List.empty[String]))
  }

  def skipClientSideDetection = {
    val enrichment = BotDetectionEnrichment(useYauaa = false, useIab = false, useAsnLookups = false, useClientSideDetection = false)
    val inputContexts = List(mkClientSideContext(true))
    val result = enrichment.getBotDetectionContext(None, None, None, inputContexts)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List.empty[String]))
  }

  def clientSideCombinedWithDerived = {
    val enrichment = BotDetectionEnrichment(useYauaa = true, useIab = false, useAsnLookups = false, useClientSideDetection = true)
    val inputContexts = List(mkClientSideContext(true))
    val result = enrichment.getBotDetectionContext(Some(mkYauaaContext("Robot", "Browser")), None, None, inputContexts)
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(true)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List("clientSideDetection", "yauaa")))
  }

  def clientSideMissingBotField = {
    val enrichment = BotDetectionEnrichment(useYauaa = false, useIab = false, useAsnLookups = false, useClientSideDetection = true)
    val ctx = SelfDescribingData(
      clientSideSchema,
      json"""{"someOtherField": "value"}"""
    )
    val result = enrichment.getBotDetectionContext(None, None, None, List(ctx))
    val data = result.data
    (data.hcursor.downField("bot").as[Boolean] must beRight(false)) and
      (data.hcursor.downField("indicators").as[List[String]] must beRight(List.empty[String]))
  }

  def parseConfigWithClientSide = {
    val config = json"""{
      "enabled": true,
      "parameters": {
        "useYauaa": true,
        "useIab": false,
        "useAsnLookups": false,
        "useClientSideDetection": true
      }
    }"""
    val schemaKey = SchemaKey(
      "com.snowplowanalytics.snowplow.enrichments",
      "bot_detection_enrichment_config",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )
    val result = BotDetectionEnrichment.parse(config, schemaKey)
    result must beValid(BotDetectionConf(schemaKey, useYauaa = true, useIab = false, useAsnLookups = false, useClientSideDetection = true))
  }

  def parseConfigWithoutClientSide = {
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
    result must beValid(BotDetectionConf(schemaKey, useYauaa = true, useIab = true, useAsnLookups = false, useClientSideDetection = false))
  }
}
