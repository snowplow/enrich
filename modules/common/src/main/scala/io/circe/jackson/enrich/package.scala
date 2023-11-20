/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */

package io.circe.jackson

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import com.snowplowanalytics.snowplow.enrich.common.utils.CirceUtils
import io.circe.{Json, ParsingFailure}

import scala.util.control.NonFatal

/**
 * This package is created to be able to change semantics of
 * circe-jackson library in special cases by creating a custom
 * ObjectMapper.
 *
 * These special cases include circe-jackson #217, #65 and #49
 *
 * This package is to be removed either when jackson is replaced
 * with another parser, possibly jawn, or when circe-jackson has
 * a new version that we can use out of the box.
 */
package object enrich extends JacksonCompat {

  val jsonFactory: JsonFactory = new JsonFactory(CirceUtils.mapper)

  def jsonStringParser(input: String): JsonParser = jsonFactory.createParser(input)

  def parse(input: String): Either[ParsingFailure, Json] =
    try Right(CirceUtils.mapper.readValue(jsonStringParser(input), classOf[Json]))
    catch {
      case NonFatal(error) => Left(ParsingFailure(error.getMessage, error))
    }
}
