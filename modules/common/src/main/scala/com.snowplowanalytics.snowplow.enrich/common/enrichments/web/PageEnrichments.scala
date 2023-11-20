/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.web

import java.net.URI

import cats.syntax.either._
import cats.syntax.option._

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils => CU}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EventEnrichments
import com.snowplowanalytics.snowplow.enrich.common.QueryStringParameters

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

  /**
   * Extract the referrer domain user ID and timestamp from the "_sp={{DUID}}.{{TSTAMP}}"
   * portion of the querystring
   * @param qsMap The querystring parameters
   * @return Validation boxing a pair of optional strings corresponding to the two fields
   */
  def parseCrossDomain(qsMap: QueryStringParameters): Either[FailureDetails.EnrichmentFailure, (Option[String], Option[String])] =
    qsMap.toMap
      .map { case (k, v) => (k, v.getOrElse("")) }
      .get("_sp") match {
      case Some("") => (None, None).asRight
      case Some(sp) =>
        val crossDomainElements = sp.split("\\.")
        val duid = CU.makeTsvSafe(crossDomainElements(0)).some
        val tstamp = crossDomainElements.lift(1) match {
          case Some(spDtm) => EventEnrichments.extractTimestamp("sp_dtm", spDtm).map(_.some)
          case None => None.asRight
        }
        tstamp.map(duid -> _)
      case None => (None -> None).asRight
    }
}
