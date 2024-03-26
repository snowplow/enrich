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

import org.specs2.mutable.Specification

class AtomicFieldsSpec extends Specification {

  "errorsToSchemaViolation" should {
    "convert ValidatorReports to SchemaViolation correctly" >> {
      val vrList = NonEmptyList(
        ValidatorReport(message = "testMessage", path = "testPath1".some, targets = List("t1, t2"), keyword = "testKeyword1".some),
        List(
          ValidatorReport(message = "testMessage", path = None, targets = List.empty, keyword = "testKeyword2".some),
          ValidatorReport(message = "testMessage", path = "testPath3".some, targets = List("t1", "t2"), keyword = None),
          ValidatorReport(message = "testMessage", path = "testPath4".some, targets = List.empty, keyword = "testKeyword4".some)
        )
      )
      val expected = Failure.SchemaViolation(
        schemaViolation = FailureDetails.SchemaViolation.IgluError(
          schemaKey = AtomicFields.atomicSchema,
          error = ValidationError(ValidatorError.InvalidData(vrList), None)
        ),
        source = "atomic_field",
        data = Json.obj(
          "testPath1" := "testKeyword1",
          "testPath3" := Json.Null,
          "testPath4" := "testKeyword4"
        )
      )
      val result = AtomicFields.errorsToSchemaViolation(vrList)
      result must beEqualTo(expected)
    }
  }
}
