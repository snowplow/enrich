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
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import java.lang.{Integer => JInteger}

import cats.syntax.either._

import com.snowplowanalytics.snowplow.enrich.common.utils.AtomicError

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
  val extractViewDimensions: (String, String) => Either[AtomicError.ParseError, (JInteger, JInteger)] =
    (field, res) =>
      (res match {
        case ResRegex(width, height) =>
          Either
            .catchNonFatal((width.toInt: JInteger, height.toInt: JInteger))
            .leftMap(_ => "Could not be converted to java.lang.Integer s")
        case _ => s"Does not conform to regex ${ResRegex.toString}".asLeft
      }).leftMap { msg =>
        AtomicError.ParseError(msg, field, Option(res))
      }

}
