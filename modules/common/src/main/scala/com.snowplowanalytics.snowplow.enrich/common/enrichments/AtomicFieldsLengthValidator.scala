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

import org.slf4j.LoggerFactory

import cats.Monad
import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyList, ValidatedNel}

import cats.implicits._

import com.snowplowanalytics.snowplow.badrows.FailureDetails.EnrichmentFailure
import com.snowplowanalytics.snowplow.badrows.{BadRow, FailureDetails, Processor}

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.AtomicFields.LimitedAtomicField
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

/**
 * Atomic fields length validation inspired by
 * https://github.com/snowplow/snowplow-scala-analytics-sdk/blob/master/src/main/scala/com.snowplowanalytics.snowplow.analytics.scalasdk/validate/package.scala
 */
object AtomicFieldsLengthValidator {

  private val logger = LoggerFactory.getLogger("InvalidEnriched")

  def validate[F[_]: Monad](
    event: EnrichedEvent,
    rawEvent: RawEvent,
    processor: Processor,
    acceptInvalid: Boolean,
    invalidCount: F[Unit],
    atomicFields: AtomicFields
  ): F[Either[BadRow.EnrichmentFailures, Unit]] =
    atomicFields.value
      .map(validateField(event))
      .combineAll match {
      case Invalid(errors) if acceptInvalid =>
        handleAcceptableBadRow(invalidCount, event, errors) *> Monad[F].pure(Right(()))
      case Invalid(errors) =>
        Monad[F].pure(buildBadRow(event, rawEvent, processor, errors).asLeft)
      case Valid(()) =>
        Monad[F].pure(Right(()))
    }

  private def validateField(
    event: EnrichedEvent
  )(
    atomicField: LimitedAtomicField
  ): ValidatedNel[String, Unit] = {
    val actualValue = atomicField.value.enrichedValueExtractor(event)
    if (actualValue != null && actualValue.length > atomicField.limit)
      s"Field ${atomicField.value.name} longer than maximum allowed size ${atomicField.limit}".invalidNel
    else
      Valid(())
  }

  private def buildBadRow(
    event: EnrichedEvent,
    rawEvent: RawEvent,
    processor: Processor,
    errors: NonEmptyList[String]
  ): BadRow.EnrichmentFailures =
    EnrichmentManager.buildEnrichmentFailuresBadRow(
      NonEmptyList(
        asEnrichmentFailure("Enriched event does not conform to atomic schema field's length restrictions"),
        errors.toList.map(asEnrichmentFailure)
      ),
      EnrichedEvent.toPartiallyEnrichedEvent(event),
      RawEvent.toRawEvent(rawEvent),
      processor
    )

  private def handleAcceptableBadRow[F[_]: Monad](
    invalidCount: F[Unit],
    event: EnrichedEvent,
    errors: NonEmptyList[String]
  ): F[Unit] =
    invalidCount *>
      Monad[F].pure(
        logger.debug(
          s"Enriched event not valid against atomic schema. Event id: ${event.event_id}. Invalid fields: ${errors.toList.mkString(",")}"
        )
      )

  private def asEnrichmentFailure(errorMessage: String): EnrichmentFailure =
    EnrichmentFailure(
      enrichment = None,
      FailureDetails.EnrichmentFailureMessage.Simple(errorMessage)
    )
}
