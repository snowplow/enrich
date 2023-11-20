/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import java.lang.{Integer => JInteger}

import cats.syntax.either._

import com.snowplowanalytics.snowplow.badrows._

/**
 * Contains enrichments related to the client - where the client is the software which is using the
 * Snowplow tracker. Enrichments relate to browser resolution.
 */
object ClientEnrichments {

  /**
   * The Tracker Protocol's pattern for a screen resolution - for details see:
   * https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#wiki-browserandos
   */
  private val ResRegex = """(\d+)x(\d+)""".r

  /**
   * Extracts view dimensions (e.g. screen resolution, browser/app viewport) stored as per the
   * Tracker Protocol:
   * https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#wiki-browserandos
   * @param field The name of the field holding the screen dimensions
   * @param res The packed string holding the screen dimensions
   * @return the ResolutionTuple or an error message, boxed in a Scalaz Validation
   */
  val extractViewDimensions: (String, String) => Either[FailureDetails.EnrichmentFailure, (JInteger, JInteger)] =
    (field, res) =>
      (res match {
        case ResRegex(width, height) =>
          Either
            .catchNonFatal((width.toInt: JInteger, height.toInt: JInteger))
            .leftMap(_ => "could not be converted to java.lang.Integer s")
        case _ => s"does not conform to regex ${ResRegex.toString}".asLeft
      }).leftMap { msg =>
        val f = FailureDetails.EnrichmentFailureMessage.InputData(
          field,
          Option(res),
          msg
        )
        FailureDetails.EnrichmentFailure(None, f)
      }

}
