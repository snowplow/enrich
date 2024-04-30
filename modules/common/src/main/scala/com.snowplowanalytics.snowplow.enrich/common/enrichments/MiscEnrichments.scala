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

import cats.syntax.either._

import io.circe._

import com.snowplowanalytics.snowplow.badrows.Processor

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils => CU, AtomicFieldValidationError}

/** Miscellaneous enrichments which don't fit into one of the other modules. */
object MiscEnrichments {

  val ContextsSchema =
    SchemaKey("com.snowplowanalytics.snowplow", "contexts", "jsonschema", SchemaVer.Full(1, 0, 1))

  val UnstructEventSchema =
    SchemaKey("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", SchemaVer.Full(1, 0, 0))

  /**
   * The version of this ETL. Appends this version to the supplied "host" ETL.
   * @param processor The version of the host ETL running this library
   * @return the complete ETL version
   */
  def etlVersion(processor: Processor): String =
    s"${processor.artifact}-${processor.version}"

  /**
   * Validate the specified platform.
   * @param field The name of the field being processed
   * @param platform The code for the platform generating this event.
   * @return a Scalaz ValidatedString.
   */
  val extractPlatform: (String, String) => Either[AtomicFieldValidationError, String] =
    (field, platform) =>
      platform match {
        case "web" => "web".asRight // Web, including Mobile Web
        case "iot" => "iot".asRight // Internet of Things (e.g. Arduino tracker)
        case "app" => "app".asRight // General App
        case "mob" => "mob".asRight // Mobile / Tablet
        case "pc" => "pc".asRight // Desktop / Laptop / Netbook
        case "cnsl" => "cnsl".asRight // Games Console
        case "tv" => "tv".asRight // Connected TV
        case "srv" => "srv".asRight // Server-side App
        case "headset" => "headset".asRight // AR/VR Headset
        case _ =>
          val msg = "Not a valid platform"
          AtomicFieldValidationError(msg, field, AtomicFieldValidationError.ParseError).asLeft
      }

  /** Make a String TSV safe */
  val toTsvSafe: (String, String) => Either[AtomicFieldValidationError, String] =
    (_, value) => CU.makeTsvSafe(value).asRight

  /**
   * The X-Forwarded-For header can contain a comma-separated list of IPs especially if it has
   * gone through multiple load balancers.
   * Here we retrieve the first one as it is supposed to be the client one, c.f.
   * https://en.m.wikipedia.org/wiki/X-Forwarded-For#Format
   */
  val extractIp: (String, String) => Either[AtomicFieldValidationError, String] =
    (_, value) => {
      val lastIp = Option(value).map(_.split("[,|, ]").head).orNull
      CU.makeTsvSafe(lastIp).asRight
    }

  /** Turn a list of custom contexts into a self-describing JSON property */
  def formatContexts(contexts: List[SelfDescribingData[Json]]): Option[String] =
    if (contexts.isEmpty) None
    else Some(SelfDescribingData(ContextsSchema, Json.arr(contexts.map(_.normalize): _*)).asString)

  /** Turn a unstruct event into a self-describing JSON property */
  def formatUnstructEvent(unstructEvent: Option[SelfDescribingData[Json]]): Option[String] =
    unstructEvent.map(e => SelfDescribingData(UnstructEventSchema, e.normalize).asString)
}
