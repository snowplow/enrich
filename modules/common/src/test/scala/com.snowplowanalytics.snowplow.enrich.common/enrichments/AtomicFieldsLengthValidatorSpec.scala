/*
 * Copyright (c) 2026-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import cats.data.{NonEmptyList, State}

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.AtomicError

class AtomicFieldsLengthValidatorSpec extends Specification {
  import AtomicFieldsLengthValidatorSpec._

  "validate" should {
    "return no failures and leave the event unchanged when all fields are valid" >> {
      val event = buildEvent(appId = "abc", platform = "web", userId = "u")
      val (count, failures) = run(event, acceptInvalid = false, emitFailed = true)

      count must beEqualTo(0)
      failures must beEmpty
      event.app_id must beEqualTo("abc")
      event.platform must beEqualTo("web")
      event.user_id must beEqualTo("u")
    }

    "report every over-length field in atomicFields order" >> {
      val event = buildEvent(appId = "abcd", platform = "web", userId = "user")
      val (count, failures) = run(event, acceptInvalid = false, emitFailed = false)

      val expectedErrors = NonEmptyList.of(
        AtomicError.FieldLengthError("Field is longer than maximum allowed size 3", "app_id", Some("abcd")),
        AtomicError.FieldLengthError("Field is longer than maximum allowed size 3", "user_id", Some("user"))
      )

      count must beEqualTo(0)
      failures must beEqualTo(List(AtomicFields.errorsToSchemaViolation(expectedErrors, SpecHelpers.etlTstamp)))
    }

    "nullify every over-length field when emitFailed is true" >> {
      val event = buildEvent(appId = "abcd", platform = "mobile", userId = "user")
      val (_, failures) = run(event, acceptInvalid = false, emitFailed = true)

      failures must haveSize(1)
      event.app_id must beNull
      event.platform must beNull
      event.user_id must beNull
    }

    "nullify no field when emitFailed is false" >> {
      val event = buildEvent(appId = "abcd", platform = "mobile", userId = "user")
      val (_, failures) = run(event, acceptInvalid = false, emitFailed = false)

      failures must haveSize(1)
      event.app_id must beEqualTo("abcd")
      event.platform must beEqualTo("mobile")
      event.user_id must beEqualTo("user")
    }

    "not run invalidCount when acceptInvalid is true and all fields are valid" >> {
      val event = buildEvent(appId = "abc", platform = "web", userId = "u")
      val (count, failures) = run(event, acceptInvalid = true, emitFailed = true)

      count must beEqualTo(0)
      failures must beEmpty
    }

    "run invalidCount and return no failures when acceptInvalid is true" >> {
      val event = buildEvent(appId = "abcd", platform = "web", userId = "user")
      val (count, failures) = run(event, acceptInvalid = true, emitFailed = false)

      count must beEqualTo(1)
      failures must beEmpty
    }
  }
}

object AtomicFieldsLengthValidatorSpec {
  type Counter[A] = State[Int, A]

  val atomicFields: AtomicFields = AtomicFields.from(Map("app_id" -> 3, "platform" -> 3, "user_id" -> 3))

  def buildEvent(
    appId: String,
    platform: String,
    userId: String
  ): EnrichedEvent = {
    val event = new EnrichedEvent
    event.app_id = appId
    event.platform = platform
    event.user_id = userId
    event
  }

  /** Returns how many times `invalidCount` ran, and the failures */
  def run(
    event: EnrichedEvent,
    acceptInvalid: Boolean,
    emitFailed: Boolean
  ): (Int, List[Failure.SchemaViolation]) = {
    val (count, (failures, _)) = AtomicFieldsLengthValidator
      .validate[Counter](
        event,
        acceptInvalid,
        State.modify[Int](_ + 1),
        atomicFields,
        emitFailed,
        SpecHelpers.etlTstamp
      )
      .run
      .run(0)
      .value
    (count, failures)
  }
}
