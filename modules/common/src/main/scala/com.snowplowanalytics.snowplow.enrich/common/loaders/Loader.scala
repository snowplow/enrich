/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.common.loaders

import java.net.URI
import java.nio.charset.Charset

import scala.collection.JavaConverters._

import cats.data.ValidatedNel
import cats.syntax.either._
import cats.syntax.option._

import org.apache.http.NameValuePair
import org.apache.http.client.utils.URLEncodedUtils
import org.joda.time.DateTime

import com.snowplowanalytics.snowplow.badrows.{BadRow, FailureDetails, Processor}

import com.snowplowanalytics.snowplow.enrich.common.utils.JsonUtils

/** All loaders must implement this abstract base class. */
abstract class Loader[T] {

  /**
   * Converts the source string into a CanonicalInput.
   * @param line line (or binary payload) of data to convert
   * @param processor processing asset (e.g. Spark enrich)
   * @return a CanonicalInput object, Option-boxed, or None if no input was extractable.
   */
  def toCollectorPayload(line: T, processor: Processor): ValidatedNel[BadRow.CPFormatViolation, Option[CollectorPayload]]

  def toCollectorPayload(
    line: T,
    processor: Processor,
    @annotation.unused tryBase64Decoding: Boolean
  ): ValidatedNel[BadRow.CPFormatViolation, Option[CollectorPayload]] =
    toCollectorPayload(line, processor)

  /**
   * Converts a querystring String into a non-empty list of NameValuePairs.
   * Returns a non-empty list of NameValuePairs on Success, or a Failure String.
   * @param qs Option-boxed querystring String to extract name-value pairs from, or None
   * @param encoding The encoding used by this querystring
   * @return either a NEL of NameValuePairs or an error message
   */
  protected[loaders] def parseQuerystring(
    qs: Option[String],
    encoding: Charset
  ): Either[FailureDetails.CPFormatViolationMessage, List[NameValuePair]] =
    qs match {
      case Some(q) =>
        Either
          .catchNonFatal(URLEncodedUtils.parse(URI.create("http://localhost/?" + q), encoding))
          .map(_.asScala.toList)
          .leftMap { e =>
            val msg = s"could not extract name-value pairs from querystring with encoding $encoding: " +
              JsonUtils.stripInstanceEtc(e.getMessage).orNull
            FailureDetails.CPFormatViolationMessage
              .InputData("querystring", qs, msg)
          }
      case None => Nil.asRight
    }

  /**
   * Converts a CloudFront log-format date and a time to a timestamp.
   * @param date The CloudFront log-format date
   * @param time The CloudFront log-format time
   * @return either the timestamp as a Joda DateTime or an error String
   */
  protected[loaders] def toTimestamp(date: String, time: String): Either[FailureDetails.CPFormatViolationMessage, DateTime] =
    Either
      .catchNonFatal(DateTime.parse("%sT%s+00:00".format(date, time)))
      .leftMap { e =>
        val msg = s"could not convert timestamp: ${e.getMessage}"
        FailureDetails.CPFormatViolationMessage.InputData(
          "dateTime",
          s"$date $time".some,
          msg
        )
      }
}
