/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import java.time.Instant

import org.slf4j.LoggerFactory

import cats.Monad
import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, WriterT}
import cats.implicits._

import com.snowplowanalytics.snowplow.enrich.common.enrichments.AtomicFields.LimitedAtomicField
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.AtomicError

/**
 * Atomic fields length validation inspired by
 * https://github.com/snowplow/snowplow-scala-analytics-sdk/blob/master/src/main/scala/com.snowplowanalytics.snowplow.analytics.scalasdk/validate/package.scala
 */
object AtomicFieldsLengthValidator {

  private val logger = LoggerFactory.getLogger("InvalidEnriched")

  def validate[F[_]: Monad](
    event: EnrichedEvent,
    acceptInvalid: Boolean,
    invalidCount: F[Unit],
    atomicFields: AtomicFields,
    emitFailed: Boolean,
    etlTstamp: Instant
  ): WriterT[F, List[Failure.SchemaViolation], Unit] =
    WriterT {
      atomicFields.value
        .map(validateField(event, _, emitFailed).toValidatedNel)
        .combineAll match {
        case Invalid(errors) if acceptInvalid =>
          handleAcceptableErrors(invalidCount, event, errors).as((Nil, ()))
        case Invalid(errors) =>
          Monad[F].pure((List(AtomicFields.errorsToSchemaViolation(errors, etlTstamp)), ()))
        case Valid(()) =>
          Monad[F].pure((Nil, ()))
      }
    }

  private def validateField(
    event: EnrichedEvent,
    atomicField: LimitedAtomicField,
    emitFailed: Boolean
  ): Either[AtomicError.FieldLengthError, Unit] = {
    val actualValue = atomicField.value.enrichedValueExtractor(event)
    if (actualValue != null && actualValue.length > atomicField.limit) {
      if (emitFailed) atomicField.value.nullify(event)
      AtomicError
        .FieldLengthError(
          s"Field is longer than maximum allowed size ${atomicField.limit}",
          atomicField.value.name,
          Option(actualValue)
        )
        .asLeft
    } else
      Right(())
  }

  private def handleAcceptableErrors[F[_]: Monad](
    invalidCount: F[Unit],
    event: EnrichedEvent,
    errors: NonEmptyList[AtomicError.FieldLengthError]
  ): F[Unit] =
    invalidCount *>
      Monad[F].pure(
        logger.debug(
          s"Enriched event not valid against atomic schema. Event id: ${event.event_id}. Invalid fields: ${errors.map(_.field).toList.flatten.mkString(", ")}"
        )
      )

}
