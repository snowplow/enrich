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

import cats.data.ValidatedNel
import cats.syntax.either._

import io.circe.Json
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.BotDetectionConf
import com.snowplowanalytics.snowplow.enrich.common.utils.CirceUtils

object BotDetectionEnrichment extends ParseableEnrichment {

  val supportedSchema: SchemaCriterion =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "bot_detection_enrichment_config",
      "jsonschema",
      1,
      0
    )

  val outputSchema: SchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow", "bot_detection", "jsonschema", SchemaVer.Full(1, 0, 1))

  private val botDeviceClasses: Set[String] = Set("Robot", "Robot Mobile", "Robot Imitator")
  private val botAgentClasses: Set[String] = Set("Robot", "Robot Mobile")

  private val clientSideSchema: SchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow", "client_side_bot_detection", "jsonschema", SchemaVer.Full(1, 0, 0))

  /** Bot signal from the YAUAA context: a robot device or agent class. */
  private def isYauaaBot(json: Json): Boolean = {
    val cursor = json.hcursor
    val deviceClass = cursor.downField("deviceClass").as[String].getOrElse("")
    val agentClass = cursor.downField("agentClass").as[String].getOrElse("")
    botDeviceClasses.contains(deviceClass) || botAgentClasses.contains(agentClass)
  }

  /** Bot signal from the IAB context. */
  private def isIabBot(json: Json): Boolean =
    json.hcursor.downField("spiderOrRobot").as[Boolean].getOrElse(false)

  /** Bot signal from the ASN context, set by the ASN lookups enrichment. */
  private def isAsnBot(json: Json): Boolean =
    json.hcursor.downField("likelyBot").as[Boolean].getOrElse(false)

  /** Bot signal from the client-side detection input context. */
  private def isClientSideBot(json: Json): Boolean =
    json.hcursor.downField("bot").as[Boolean].getOrElse(false)

  override def parse(
    config: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, BotDetectionConf] =
    (for {
      _ <- isParseable(config, schemaKey)
      useYauaa <- CirceUtils.extract[Boolean](config, "parameters", "useYauaa").toEither
      useIab <- CirceUtils.extract[Boolean](config, "parameters", "useIab").toEither
      useAsnLookups <- CirceUtils.extract[Boolean](config, "parameters", "useAsnLookups").toEither
      useClientSideDetection <- CirceUtils.extract[Option[Boolean]](config, "parameters", "useClientSideDetection").toEither
    } yield BotDetectionConf(schemaKey, useYauaa, useIab, useAsnLookups, useClientSideDetection.getOrElse(false))).toValidatedNel
}

final case class BotDetectionEnrichment(
  useYauaa: Boolean,
  useIab: Boolean,
  useAsnLookups: Boolean,
  useClientSideDetection: Boolean
) {
  import BotDetectionEnrichment._

  /**
   * Consolidates the bot signals into a single context.
   *
   * The derived-context signals (YAUAA, IAB, ASN) are passed in directly by the enrichment
   * pipeline. The client-side detection signal arrives in the list of input contexts attached to
   * the event, so it is located by its schema.
   */
  def getBotDetectionContext(
    yauaaContext: Option[SelfDescribingData[Json]],
    iabContext: Option[SelfDescribingData[Json]],
    asnContext: Option[SelfDescribingData[Json]],
    inputContexts: List[SelfDescribingData[Json]]
  ): SelfDescribingData[Json] = {
    val clientSideContext = inputContexts.find(_.schema == clientSideSchema)

    // Each enabled source adds its name to the indicators when its context signals a bot.
    val indicators = List(
      (useClientSideDetection, clientSideContext, "clientSideDetection", isClientSideBot _),
      (useAsnLookups, asnContext, "asnLookups", isAsnBot _),
      (useIab, iabContext, "iab", isIabBot _),
      (useYauaa, yauaaContext, "yauaa", isYauaaBot _)
    ).collect { case (true, Some(ctx), name, isBot) if isBot(ctx.data) => name }

    val resultJson = Json.obj(
      "bot" -> indicators.nonEmpty.asJson,
      "indicators" -> indicators.asJson
    )
    SelfDescribingData(outputSchema, resultJson)
  }
}
