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

  /** Defines how to extract a bot signal from one source context. */
  private case class Indicator(
    schemaKey: SchemaKey,
    name: String,
    isBot: Json => Boolean
  )

  private val yauaaIndicator: Indicator = Indicator(
    YauaaEnrichment.outputSchema,
    "yauaa",
    json => {
      val cursor = json.hcursor
      val deviceClass = cursor.downField("deviceClass").as[String].getOrElse("")
      val agentClass = cursor.downField("agentClass").as[String].getOrElse("")
      botDeviceClasses.contains(deviceClass) || botAgentClasses.contains(agentClass)
    }
  )

  private val iabIndicator: Indicator = Indicator(
    IabEnrichment.outputSchema,
    "iab",
    _.hcursor.downField("spiderOrRobot").as[Boolean].getOrElse(false)
  )

  private val asnLookupsIndicator: Indicator = Indicator(
    IpLookupsEnrichment.asnSchema,
    "asnLookups",
    _.hcursor.downField("likelyBot").as[Boolean].getOrElse(false)
  )

  private val clientSideSchema: SchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow", "client_side_bot_detection", "jsonschema", SchemaVer.Full(1, 0, 0))

  private val clientSideDetectionIndicator: Indicator = Indicator(
    clientSideSchema,
    "clientSideDetection",
    _.hcursor.downField("bot").as[Boolean].getOrElse(false)
  )

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

  /** Only the indicators that are enabled — built once at construction, not per event. */
  private val enabledDerivedIndicators: Map[SchemaKey, Indicator] =
    List(
      (useYauaa, yauaaIndicator),
      (useIab, iabIndicator),
      (useAsnLookups, asnLookupsIndicator)
    ).collect { case (true, ind) => ind.schemaKey -> ind }.toMap

  private val enabledInputIndicators: Map[SchemaKey, Indicator] =
    List(
      (useClientSideDetection, clientSideDetectionIndicator)
    ).collect { case (true, ind) => ind.schemaKey -> ind }.toMap

  def getBotDetectionContext(
    derivedContexts: List[SelfDescribingData[Json]],
    inputContexts: List[SelfDescribingData[Json]]
  ): List[SelfDescribingData[Json]] = {
    def collectTriggeredIndicators(contexts: List[SelfDescribingData[Json]], bySchema: Map[SchemaKey, Indicator]): List[String] =
      contexts.foldLeft(List.empty[String]) { (acc, ctx) =>
        bySchema
          .get(ctx.schema)
          .fold(acc)(ind =>
            if (ind.isBot(ctx.data)) ind.name :: acc
            else acc
          )
      }
    val derivedIndicators =
      if (enabledDerivedIndicators.nonEmpty) collectTriggeredIndicators(derivedContexts, enabledDerivedIndicators) else Nil
    val inputIndicators = if (enabledInputIndicators.nonEmpty) collectTriggeredIndicators(inputContexts, enabledInputIndicators) else Nil
    val indicatorList = inputIndicators ::: derivedIndicators

    val resultJson = Json.obj(
      "bot" -> (indicatorList.nonEmpty).asJson,
      "indicators" -> indicatorList.asJson
    )
    List(SelfDescribingData(outputSchema, resultJson))
  }
}
