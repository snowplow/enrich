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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.web

import java.net.URI

import cats.syntax.either._

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils => CU}

/** Holds enrichments related to the web page URL, and the document object contained in the page. */
object PageEnrichments {

  /**
   * Extracts the page URI from either the collector's referer* or the appropriate tracker variable.
   * Tracker variable takes precedence as per #268
   * @param fromReferer The page URI reported as the referer to the collector
   * @param fromTracker The page URI reported by the tracker
   * @return either the chosen page URI, or an error, wrapped in a Validation
   */
  def extractPageUri(fromReferer: Option[String], fromTracker: Option[String]): Either[FailureDetails.EnrichmentFailure, Option[URI]] =
    ((fromReferer, fromTracker) match {
      case (Some(r), None) => CU.stringToUri(r)
      case (None, Some(t)) => CU.stringToUri(t)
      // Tracker URL takes precedence
      case (Some(_), Some(t)) => CU.stringToUri(t)
      case (None, None) => None.asRight // No page URI available. Not a failable offence
    }).leftMap(f =>
      FailureDetails.EnrichmentFailure(
        None,
        FailureDetails.EnrichmentFailureMessage.Simple(f)
      )
    )
}
