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
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import cats.data.NonEmptyList
import cats.syntax.option._

import io.circe.Json
import io.circe.syntax._

import com.snowplowanalytics.iglu.client.ClientError.ValidationError
import com.snowplowanalytics.iglu.client.validator.{ValidatorError, ValidatorReport}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.utils.AtomicError

import org.specs2.mutable.Specification

class AtomicFieldsSpec extends Specification {

  "errorsToSchemaViolation" should {
    "convert AtomicErrors to SchemaViolation correctly" >> {
      val error1 = AtomicError.ParseError("message1", "field1", Some("value1"))
      val error2 = AtomicError.ParseError("message2", "field2", Some(""))
      val error3 = AtomicError.FieldLengthError("message3", "field3", Some("value3"))
      val error4 = AtomicError.ParseError("message4", "field4", None)

      val vrList = NonEmptyList(
        ValidatorReport(error1.message, error1.field.some, Nil, AtomicError.keywordParse.some),
        List(
          ValidatorReport(error2.message, error2.field.some, Nil, AtomicError.keywordParse.some),
          ValidatorReport(error3.message, error3.field.some, Nil, AtomicError.keywordLength.some),
          ValidatorReport(error4.message, error4.field.some, Nil, AtomicError.keywordParse.some)
        )
      )

      val schemaViolation = FailureDetails.SchemaViolation.IgluError(
        schemaKey = AtomicFields.atomicSchema,
        error = ValidationError(ValidatorError.InvalidData(vrList), None)
      )
      val source = AtomicError.source
      val data = Json.obj(
        error1.field := error1.value,
        error2.field := error2.value,
        error3.field := error3.value,
        error4.field := Json.Null
      )

      val result = AtomicFields.errorsToSchemaViolation(NonEmptyList(error1, List(error2, error3, error4)))
      result.schemaViolation must beEqualTo(schemaViolation)
      result.source must beEqualTo(source)
      result.data must beEqualTo(data)
    }
  }
}
