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

case class AtomicFieldValidationError(
  message: String,
  field: String,
  errorType: AtomicFieldValidationError.ErrorType
) {
  def toValidatorReport: ValidatorReport =
    ValidatorReport(message, Some(field), Nil, Some(errorType.repr))
}

object AtomicFieldValidationError {
  sealed trait ErrorType {
    def repr: String
  }
  case object ParseError extends ErrorType {
    override def repr: String = "ParseError"
  }
  case object AtomicFieldLengthExceeded extends ErrorType {
    override def repr: String = "AtomicFieldLengthExceeded"
  }
}
