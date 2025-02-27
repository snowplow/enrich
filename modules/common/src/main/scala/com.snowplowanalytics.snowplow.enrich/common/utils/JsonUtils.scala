/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import java.math.{BigInteger => JBigInteger}

import cats.data.NonEmptyList
import cats.syntax.either._

import io.circe.Json
import io.circe.syntax._

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/** Contains general purpose extractors and other utilities for JSONs. Jackson-based. */
object JsonUtils {

  type DateTimeFields = Option[(NonEmptyList[String], DateTimeFormatter)]

  // Defines the maximalist JSON Schema-compatible date-time format
  private val JsonSchemaDateTimeFormat =
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(DateTimeZone.UTC)

  /** Validates a String as correct JSON. */
  val extractUnencJson: (String, String) => Either[AtomicError.ParseError, String] =
    (_, str) => Right(str)

  /** Decodes a Base64 (URL safe)-encoded String then validates it as correct JSON. */
  val extractBase64EncJson: (String, String) => Either[AtomicError.ParseError, String] =
    (field, str) =>
      ConversionUtils
        .decodeBase64Url(str)
        .leftMap { e =>
          AtomicError.ParseError(e, field, Option(str))
        }

  /**
   * Converts a boolean-like String of value "true" or "false" to a JBool value of true or false
   * respectively. Any other value becomes a JString.
   * No erroring if the String is not boolean-like, leave it to eventual JSON Schema validation
   * to enforce that.
   * @param str The boolean-like String to convert
   * @return true, false, and otherwise a JString wrapping the original String
   */
  private[utils] def booleanToJson(str: String): Json =
    str match {
      case "true" => Json.True
      case "false" => Json.False
      case _ => Json.fromString(str)
    }

  /**
   * Converts an integer-like String to a JInt value. Any other value becomes a JString.
   * No erroring if the String is not integer-like, leave it to eventual JSON Schema validation
   * to enforce that.
   * @param str The integer-like String to convert
   * @return a JInt if the String was integer-like, otherwise a JString wrapping the original.
   */
  private[utils] def integerToJson(str: String): Json =
    Either.catchNonFatal(new JBigInteger(str)) match {
      case Right(bigInt) => Json.fromBigInt(bigInt)
      case _ => Json.fromString(str)
    }

  /**
   * Reformats a non-standard date-time into a format compatible with JSON Schema's date-time
   * format validation. If the String does not match the expected date format, then return the
   * original String.
   * @param str The date-time-like String to reformat to pass JSON Schema validation
   * @return the reformatted date-time String if possible, or otherwise the original String
   */
  def toJsonSchemaDateTime(str: String, fromFormat: DateTimeFormatter): String =
    try {
      val dt = DateTime.parse(str, fromFormat)
      JsonSchemaDateTimeFormat.print(dt)
    } catch {
      case _: IllegalArgumentException => str
    }

  /**
   * Converts an incoming key, value into a Json. Uses the lists of keys which should
   * contain bools, ints and dates to apply specific processing to those values when found.
   * @param key The key of the field to generate. Also used to determine what additional
   * processing should be applied to the value
   * @param value The value of the field
   * @param bools A List of keys whose values should be processed as boolean-like Strings
   * @param ints A List of keys whose values should be processed as integer-like Strings
   * @param dateTimes If Some, a NEL of keys whose values should be treated as date-time-like Strings,
   * which will require processing from the specified format
   * @return a JField, containing the original key and the processed String, now as a JValue
   */
  def toJson(
    key: String,
    value: Option[String],
    bools: List[String],
    ints: List[String],
    dateTimes: DateTimeFields
  ): (String, Json) = {
    val v = (value, dateTimes) match {
      case (Some(""), _) => Json.Null
      case (None, _) => Json.Null
      case (Some(bool), _) if bools.contains(key) => booleanToJson(bool)
      case (Some(nb), _) if ints.contains(key) => integerToJson(nb)
      case (Some(datetime), Some((nel, fmt))) if nel.toList.contains(key) =>
        Json.fromString(toJsonSchemaDateTime(datetime, fmt))
      case (Some(str), _) => Json.fromString(str)
    }
    (key, v)
  }

  /**
   * Converts a JSON string into an EIther[String, Json]
   * @param instance The JSON string to parse
   * @param maxJsonDepth The maximum allowed JSON depth
   * @return either an error String or the extracted Json
   */
  def extractJson(instance: String, maxJsonDepth: Int): Either[String, Json] =
    for {
      json <- io.circe.parser
                .parse(instance)
                .leftMap(e => s"invalid json: ${e.message}")
      _ <- if (checkIfExceedMaxDepth(json, maxJsonDepth)) Left("invalid json: maximum allowed JSON depth exceeded")
           else Right(json)
    } yield json

  private def checkIfExceedMaxDepth(json: Json, maxJsonDepth: Int): Boolean =
    if (maxJsonDepth <= 0) true
    else
      json.fold(
        jsonNull = false,
        jsonBoolean = _ => false,
        jsonNumber = _ => false,
        jsonString = _ => false,
        jsonArray = _.exists(checkIfExceedMaxDepth(_, maxJsonDepth - 1)),
        jsonObject = _.toList.exists { case (_, j) => checkIfExceedMaxDepth(j.asJson, maxJsonDepth - 1) }
      )

  /**
   * Strips the instance information from a Jackson
   * parsing exception message:
   * "... at [Source: java.io.StringReader@1fe7a8f8; line: 1, column: 2]""
   *                                       ^^^^^^^^
   * Also removes any control characters and replaces
   * tabs with 4 spaces.
   * @param message The exception message which needs tidying up
   * @return the same exception message, but with instance information etc removed. Option-boxed
   * because the message can be null
   */
  def stripInstanceEtc(message: String): Option[String] =
    for (m <- Option(message))
      yield m
        .replaceAll("@[0-9a-z]+;", "@xxxxxx;")
        .replaceAll("\\t", "    ")
        .replaceAll("\\p{Cntrl}", "") // Any other control character
        .trim
}
