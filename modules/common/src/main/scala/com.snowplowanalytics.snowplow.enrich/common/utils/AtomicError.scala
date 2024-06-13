/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import com.snowplowanalytics.iglu.client.validator.ValidatorReport

sealed trait AtomicError {
  def message: String
  def field: String
  def value: Option[String]
  def keyword: String
  // IMPORTANT: `value` should never be put in ValidatorReport
  def toValidatorReport: ValidatorReport =
    ValidatorReport(message, Some(field), Nil, Some(keyword))
}

object AtomicError {

  val source = "atomic_field"
  val keywordParse = s"${source}_parse_error"
  val keywordLength = s"${source}_length_exceeded"

  case class ParseError(
    message: String,
    field: String,
    value: Option[String]
  ) extends AtomicError {
    override def keyword: String = keywordParse
  }

  case class FieldLengthError(
    message: String,
    field: String,
    value: Option[String]
  ) extends AtomicError {
    override def keyword: String = keywordLength
  }
}
