/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.test

import cats.syntax.either._

import io.circe.parser

import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

object Utils {

  def parseBadRow(s: String): Either[String, BadRow] =
    for {
      json <- parser.parse(s).leftMap(_.message)
      sdj <- SelfDescribingData.parse(json).leftMap(_.message("Can't decode JSON as SDJ"))
      br <- sdj.data.as[BadRow].leftMap(_.getMessage())
    } yield br
}
