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

import java.util.UUID

import scala.util.control.NonFatal

import cats.syntax.either._
import cats.syntax.option._
import org.joda.time.{DateTime, DateTimeZone, Period}
import org.joda.time.format.DateTimeFormat

import com.snowplowanalytics.snowplow.enrich.common.utils.AtomicError

import com.snowplowanalytics.snowplow.badrows._

/** Holds the enrichments related to events. */
object EventEnrichments {

  /** A Redshift-compatible timestamp format */
  private val TstampFormat =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(DateTimeZone.UTC)

  /**
   * Converts a Joda DateTime into a Redshift-compatible timestamp String.
   * @param datetime The Joda DateTime to convert to a timestamp String
   * @return the timestamp String
   */
  def toTimestamp(datetime: DateTime): String = TstampFormat.print(datetime)

  /**
   * Converts a Redshift-compatible timestamp String back into a Joda DateTime.
   * @param timestamp The timestamp String to convert
   * @return the Joda DateTime
   */
  def fromTimestamp(timestamp: String): DateTime = TstampFormat.parseDateTime(timestamp)

  /**
   * Make a collector_tstamp Redshift-compatible
   * @param Optional collectorTstamp
   * @return Validation boxing the result of making the timestamp Redshift-compatible
   */
  def formatCollectorTstamp(collectorTstamp: Option[DateTime]): Either[AtomicError.ParseError, String] =
    collectorTstamp match {
      case None => AtomicError.ParseError("Field not set", "collector_tstamp", None).asLeft
      case Some(t) =>
        val formattedTimestamp = toTimestamp(t)
        if (formattedTimestamp.startsWith("-") || t.getYear > 9999 || t.getYear < 0) {
          val msg = s"Formatted as $formattedTimestamp is not Redshift-compatible"
          AtomicError.ParseError(msg, "collector_tstamp", Option(t).map(_.toString)).asLeft
        } else
          formattedTimestamp.asRight
    }

  /**
   * Calculate the derived timestamp
   * If dvce_sent_tstamp and dvce_created_tstamp are not null and the former is after the latter,
   * add the difference between the two to the collector_tstamp.
   * Otherwise just return the collector_tstamp.
   * TODO: given missing collectorTstamp is invalid, consider updating this signature to
   * `..., collectorTstamp: String): Validation[String, String]` and making the call to this
   * function in the EnrichmentManager dependent on a Success(collectorTstamp).
   * @param dvceSentTstamp
   * @param dvceCreatedTstamp
   * @param collectorTstamp
   * @return derived timestamp
   */
  def getDerivedTimestamp(
    dvceSentTstamp: Option[String],
    dvceCreatedTstamp: Option[String],
    collectorTstamp: Option[String],
    trueTstamp: Option[String]
  ): Either[FailureDetails.EnrichmentFailure, Option[String]] =
    trueTstamp match {
      case Some(ttm) => Some(ttm).asRight
      case None =>
        try {
          ((dvceSentTstamp, dvceCreatedTstamp, collectorTstamp) match {
            case (Some(dst), Some(dct), Some(ct)) =>
              val startTstamp = fromTimestamp(dct)
              val endTstamp = fromTimestamp(dst)
              if (startTstamp.isBefore(endTstamp))
                toTimestamp(fromTimestamp(ct).minus(new Period(startTstamp, endTstamp))).some
              else
                ct.some
            case _ => collectorTstamp
          }).asRight
        } catch {
          case NonFatal(e) =>
            FailureDetails
              .EnrichmentFailure(
                None,
                FailureDetails.EnrichmentFailureMessage.Simple(
                  s"Exception calculating derived timestamp: ${e.getMessage}"
                )
              )
              .asLeft
        }
    }

  /**
   * Extracts the timestamp from the format as laid out in the Tracker Protocol:
   * https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#wiki-common-params
   * @param tstamp The timestamp as stored in the Tracker Protocol
   * @return a Tuple of two Strings (date and time), or an error message if the format was invalid
   */
  val extractTimestamp: (String, String) => Either[AtomicError.ParseError, String] =
    (field, tstamp) =>
      try {
        val dt = new DateTime(tstamp.toLong)
        val timestampString = toTimestamp(dt)
        if (timestampString.startsWith("-") || dt.getYear > 9999 || dt.getYear < 0)
          AtomicError
            .ParseError(
              s"Formatting as $timestampString is not Redshift-compatible",
              field,
              Option(tstamp)
            )
            .asLeft
        else
          timestampString.asRight
      } catch {
        case _: NumberFormatException =>
          AtomicError
            .ParseError(
              "Not in the expected format: ms since epoch",
              field,
              Option(tstamp)
            )
            .asLeft
      }

  /**
   * Turns an event code into a valid event type, e.g. "pv" -> "page_view". See the Tracker
   * Protocol for details:
   * https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#wiki-event2
   * @param eventCode The event code
   * @return the event type, or an error message if not recognised, boxed in a Scalaz Validation
   */
  val extractEventType: (String, String) => Either[AtomicError.ParseError, String] =
    (field, code) =>
      code match {
        case "se" => "struct".asRight
        case "ev" => "struct".asRight // Leave in for legacy.
        case "ue" => "unstruct".asRight
        case "ad" => "ad_impression".asRight // Leave in for legacy.
        case "tr" => "transaction".asRight
        case "ti" => "transaction_item".asRight
        case "pv" => "page_view".asRight
        case "pp" => "page_ping".asRight
        case _ =>
          val msg = "Not a valid event type"
          AtomicError.ParseError(msg, field, Option(code)).asLeft
      }

  /**
   * Returns a unique event ID. The event ID is generated as a type 4 UUID, then converted
   * to a String.
   * () on the function signature because it's not pure
   * @return the unique event ID
   */
  def generateEventId(): String = UUID.randomUUID().toString
}
