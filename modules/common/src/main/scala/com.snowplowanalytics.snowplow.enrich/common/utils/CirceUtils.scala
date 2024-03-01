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
package com.snowplowanalytics.snowplow.enrich.common.utils

import cats.data.Validated
import cats.syntax.either._
import com.fasterxml.jackson.databind.ObjectMapper
import io.circe._
import io.circe.jackson.enrich.CirceJsonModule

object CirceUtils {

  /**
   * Returns a field of type A at the end of a JSON path
   * @tparam A Type of the field to extract
   * @param head The first field in the JSON path. Exists to ensure the path is nonempty
   * @param tail The rest of the fields in the JSON path
   * @return the list extracted from the JSON on success or an error String on failure
   */
  def extract[A: Decoder: Manifest](
    config: Json,
    head: String,
    tail: String*
  ): Validated[String, A] = {
    val path = head :: tail.toList
    path
      .foldLeft(config.hcursor: ACursor) { case (c, f) => c.downField(f) }
      .as[A]
      .toValidated
      .leftMap { e =>
        val pathStr = path.mkString(".")
        val clas = manifest[A]
        s"Could not extract $pathStr as $clas from supplied JSON due to ${e.getMessage}"
      }
  }

  /**
   * A custom ObjectMapper specific to Circe JSON AST
   *
   * The only difference from the original mapper `io.circe.jackson.mapper` is
   * how `Long` is deserialized. The original mapper maps a `Long` to `JsonBigDecimal`
   * whereas this custom mapper deserializes a `Long` to `JsonLong`.
   *
   * This customization saves Snowplow events from failing when derived contexts are
   * validated post-enrichment. If output schema of API Request Enrichment has an integer
   * field, `JsonBigDecimal` representation of a Long results in a bad row
   * with message `number found, integer expected` in Iglu Scala Client, since jackson
   * treats `DecimalNode` as number in all cases.
   */
  final val mapper: ObjectMapper = (new ObjectMapper).registerModule(CirceJsonModule)
}
