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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest

import cats.syntax.either._
import io.circe._
import io.circe.syntax._

import io.circe.jackson.enrich.parse

import com.snowplowanalytics.snowplow.enrich.common.utils.JsonPath.{query, wrapArray}

/**
 * Base trait for API output format. Primary intention of these classes is to perform transformation
 * of API raw output to self-describing JSON instance
 */
final case class Output(schema: String, json: Option[JsonOutput]) {

  /**
   * Transforming raw API response (text) to JSON (in future A => JSON) and extracting value by
   * output's path
   * @param apiResponse response taken from `ApiMethod`
   * @return parsed extracted JSON
   */
  def parseResponse(apiResponse: String): Either[Throwable, Json] =
    json match {
      case Some(jsonOutput) => jsonOutput.parseResponse(apiResponse)
      case None =>
        new InvalidStateException(s"Error: output key is missing").asLeft // Cannot happen now
    }

  /**
   * Extract value specified by output's path
   * @param value parsed API response
   * @return extracted validated JSON
   */
  def extract(value: Json): Either[Throwable, Json] =
    json match {
      case Some(jsonOutput) => jsonOutput.extract(value)
      case output =>
        new InvalidStateException(s"Error: Unknown output [$output]").asLeft // Cannot happen now
    }

  /**
   * Add `schema` (Iglu URI) to parsed instance
   * @param json JValue parsed from API
   * @return self-describing JSON instance
   */
  def describeJson(json: Json): Json =
    Json.obj(
      "schema" := schema,
      "data" := json
    )
}

/**
 * Common trait for all API output formats
 * @tparam A type of API response (XML, JSON, etc)
 */
sealed trait ApiOutput[A] {
  val path: String

  /**
   * Parse raw response into validated Output format (XML, JSON)
   * @param response API response assumed to be JSON
   * @return validated JSON
   */
  def parseResponse(response: String): Either[Throwable, A]

  /**
   * Extract value specified by `path` and transform to context-ready JSON data
   * @param response parsed API response
   * @return extracted by `path` value mapped to JSON
   */
  def extract(response: A): Either[Throwable, Json]

  /**
   * Try to parse string as JSON and extract value by JSON PAth
   * @param response API response assumed to be JSON
   * @return validated extracted value
   */
  def get(response: String): Either[Throwable, Json] =
    for {
      validated <- parseResponse(response)
      result <- extract(validated)
    } yield result
}

/**
 * Preference for extracting JSON from API output
 * @param jsonPath JSON Path to required value
 */
final case class JsonOutput(jsonPath: String) extends ApiOutput[Json] {
  val path = jsonPath

  /**
   * Proxy function for `query` which wrap missing value in error
   * @param json JSON value to look in
   * @return validated found JSON, with absent value treated like failure
   */
  def extract(json: Json): Either[Throwable, Json] =
    query(path, json).map(wrapArray) match {
      case Right(js) if js.asArray.map(_.isEmpty).getOrElse(false) =>
        ValueNotFoundException(
          s"Error: no values were found by JSON Path [$jsonPath] in [${json.noSpaces}]"
        ).asLeft
      case other => other.leftMap(JsonPathException.apply)
    }

  def parseResponse(response: String): Either[Throwable, Json] =
    parse(response)
}
