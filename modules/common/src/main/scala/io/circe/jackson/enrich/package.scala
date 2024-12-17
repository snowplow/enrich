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
