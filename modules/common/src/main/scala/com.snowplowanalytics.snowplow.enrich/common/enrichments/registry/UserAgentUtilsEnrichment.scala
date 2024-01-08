/*Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import scala.util.control.NonFatal

import cats.data.ValidatedNel
import cats.syntax.either._
import cats.syntax.option._

import io.circe._

import org.slf4j.LoggerFactory

import eu.bitwalker.useragentutils._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.UserAgentUtilsConf

object UserAgentUtilsEnrichmentConfig extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "user_agent_utils_config", "jsonschema", 1, 0)

  private val log = LoggerFactory.getLogger(getClass())

  override def parse(
    config: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, UserAgentUtilsConf] = {
    log.warn(
      s"user_agent_utils enrichment is deprecated. Please visit here for more information: " +
        s"https://github.com/snowplow/snowplow/wiki/user-agent-utils-enrichment"
    )
    isParseable(config, schemaKey)
      .map(_ => UserAgentUtilsConf(schemaKey))
      .toValidatedNel
  }
}

/**
 * Case class to wrap everything we can extract from the useragent using UserAgentUtils.
 * Not to be declared inside a class Object
 * http://stackoverflow.com/questions/17270003/why-are-classes-inside-scala-package-objects-dispreferred
 */
final case class ClientAttributes(
  // Browser
  browserName: String,
  browserFamily: String,
  browserVersion: Option[String],
  browserType: String,
  browserRenderEngine: String,
  // OS the browser is running on
  osName: String,
  osFamily: String,
  osManufacturer: String,
  // Hardware the OS is running on
  deviceType: String,
  deviceIsMobile: Boolean
)

final case class UserAgentUtilsEnrichment(schemaKey: SchemaKey) extends Enrichment {
  private val mobileDeviceTypes = Set(DeviceType.MOBILE, DeviceType.TABLET, DeviceType.WEARABLE)
  private val enrichmentInfo =
    FailureDetails.EnrichmentInformation(schemaKey, "ua-parser").some

  /**
   * Extracts the client attributes from a useragent string, using UserAgentUtils.
   * TODO: rewrite this when we swap out UserAgentUtils for ua-parser
   * @param useragent to extract from. Should be encoded, i.e. not previously decoded.
   * @return the ClientAttributes or the message of the exception, boxed in a Scalaz Validation
   */
  def extractClientAttributes(useragent: String): Either[FailureDetails.EnrichmentFailure, ClientAttributes] =
    try {
      val ua = UserAgent.parseUserAgentString(useragent)
      val b = ua.getBrowser
      val v = Option(ua.getBrowserVersion)
      val os = ua.getOperatingSystem
      ClientAttributes(
        browserName = b.getName,
        browserFamily = b.getGroup.getName,
        browserVersion = v map { _.getVersion },
        browserType = b.getBrowserType.getName,
        browserRenderEngine = b.getRenderingEngine.toString,
        osName = os.getName,
        osFamily = os.getGroup.getName,
        osManufacturer = os.getManufacturer.getName,
        deviceType = os.getDeviceType.getName,
        deviceIsMobile = mobileDeviceTypes.contains(os.getDeviceType)
      ).asRight
    } catch {
      case NonFatal(e) =>
        val msg = s"could not parse useragent: ${e.getMessage}"
        val f = FailureDetails.EnrichmentFailureMessage.InputData(
          "useragent",
          useragent.some,
          msg
        )
        FailureDetails.EnrichmentFailure(enrichmentInfo, f).asLeft
    }
}
