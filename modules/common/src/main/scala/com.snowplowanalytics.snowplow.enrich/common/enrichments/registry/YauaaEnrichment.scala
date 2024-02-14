/**
 * Copyright (c) 2019-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import scala.jdk.CollectionConverters._

import cats.data.ValidatedNel
import cats.syntax.either._

import io.circe.Json
import io.circe.syntax._

import nl.basjes.parse.useragent.{UserAgent, UserAgentAnalyzer}

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.YauaaConf
import com.snowplowanalytics.snowplow.enrich.common.utils.CirceUtils

/** Companion object to create an instance of YauaaEnrichment from the configuration. */
object YauaaEnrichment extends ParseableEnrichment {
  val supportedSchema: SchemaCriterion =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "yauaa_enrichment_config",
      "jsonschema",
      1,
      0
    )

  val DefaultDeviceClass = "Unknown"
  val DefaultResult = Map(decapitalize(UserAgent.DEVICE_CLASS) -> DefaultDeviceClass)

  val outputSchema: SchemaKey = SchemaKey("nl.basjes", "yauaa_context", "jsonschema", SchemaVer.Full(1, 0, 4))

  /**
   * Creates a YauaaConf instance from a JValue containing the configuration of the enrichment.
   *
   * @param c         JSON containing configuration for YAUAA enrichment.
   * @param schemaKey SchemaKey provided for this enrichment.
   *                  Must be a supported SchemaKey for this enrichment.
   * @return Configuration for YAUAA enrichment
   */
  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, YauaaConf] =
    (for {
      _ <- isParseable(c, schemaKey)
      cacheSize <- CirceUtils.extract[Option[Int]](c, "parameters", "cacheSize").toEither
    } yield YauaaConf(schemaKey, cacheSize)).toValidatedNel

  /** Helper to decapitalize a string. Used for the names of the fields returned in the context. */
  def decapitalize(s: String): String =
    s match {
      case _ if s.isEmpty => s
      case _ if s.length == 1 => s.toLowerCase
      case _ => Character.toString(s.charAt(0).toLower) + s.substring(1)
    }
}

/**
 * Class for YAUAA enrichment, which tries to parse and analyze the user agent string
 * and extract as many relevant attributes as possible, like for example the device class.
 * @param cacheSize Amount of user agents already parsed that stay in cache for faster parsing.
 */
final case class YauaaEnrichment(cacheSize: Option[Int]) extends Enrichment {
  import YauaaEnrichment.decapitalize

  private val uaa: UserAgentAnalyzer = {
    val a = UserAgentAnalyzer
      .newBuilder()
      .build()
    cacheSize.foreach(a.setCacheSize)
    a
  }

  /**
   * Gets the result of YAUAA user agent analysis as self-describing JSON, for a specific event.
   * @param userAgent User agent of the event.
   * @return Attributes retrieved thanks to the user agent (if any), as self-describing JSON.
   */
  def getYauaaContext(userAgent: String, headers: List[String]): SelfDescribingData[Json] =
    SelfDescribingData(YauaaEnrichment.outputSchema, analyzeUserAgent(userAgent, headers).asJson)

  /**
   * Gets the map of attributes retrieved by YAUAA from the user agent.
   * @return Map with all the fields extracted by YAUAA by parsing the user agent.
   *         If the input is null or empty, a map with just the DeviceClass set to Unknown is returned.
   */
  def analyzeUserAgent(userAgent: String, headers: List[String]): Map[String, String] =
    userAgent match {
      case null | "" =>
        YauaaEnrichment.DefaultResult
      case _ =>
        val headerMap = headers
          .map(_.split(": ", 2))
          .collect {
            case Array(key, value) => key -> value.replaceAll("^\\s+", "")
          }
          .toMap ++ Map("User-Agent" -> userAgent)
        val parsedUA = uaa.parse(headerMap.asJava)
        parsedUA.getAvailableFieldNamesSorted.asScala
          .map(field => decapitalize(field) -> parsedUA.getValue(field))
          .toMap
          .view
          .filterKeys(validFields)
          .toMap
    }

  /** Yauaa 7.x added many new fields which are not in the 1-0-4 schema */
  private val validFields = Set(
    "deviceClass",
    "deviceName",
    "deviceBrand",
    "deviceCpu",
    "deviceCpuBits",
    "deviceFirmwareVersion",
    "deviceVersion",
    "operatingSystemClass",
    "operatingSystemName",
    "operatingSystemVersion",
    "operatingSystemNameVersion",
    "operatingSystemVersionBuild",
    "layoutEngineClass",
    "layoutEngineName",
    "layoutEngineVersion",
    "layoutEngineVersionMajor",
    "layoutEngineNameVersion",
    "layoutEngineNameVersionMajor",
    "layoutEngineBuild",
    "agentClass",
    "agentName",
    "agentVersion",
    "agentVersionMajor",
    "agentNameVersion",
    "agentNameVersionMajor",
    "agentBuild",
    "agentLanguage",
    "agentLanguageCode",
    "agentInformationEmail",
    "agentInformationUrl",
    "agentSecurity",
    "agentUuid",
    "webviewAppName",
    "webviewAppVersion",
    "webviewAppVersionMajor",
    "webviewAppNameVersionMajor",
    "facebookCarrier",
    "facebookDeviceClass",
    "facebookDeviceName",
    "facebookDeviceVersion",
    "facebookFBOP",
    "facebookFBSS",
    "facebookOperatingSystemName",
    "facebookOperatingSystemVersion",
    "anonymized",
    "hackerAttackVector",
    "hackerToolkit",
    "koboAffiliate",
    "koboPlatformId",
    "iECompatibilityVersion",
    "iECompatibilityVersionMajor",
    "iECompatibilityNameVersion",
    "iECompatibilityNameVersionMajor",
    "carrier",
    "gSAInstallationID",
    "networkType",
    "operatingSystemNameVersionMajor",
    "operatingSystemVersionMajor"
  )
}
