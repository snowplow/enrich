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
package com.snowplowanalytics.snowplow.enrich.common

import java.time.Instant

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.effect.Sync
import cats.implicits._

import org.joda.time.DateTime

import io.circe.syntax._

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, Payload, Processor}

import com.snowplowanalytics.snowplow.enrich.common.adapters.{AdapterRegistry, RawEvent}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{AtomicFields, EnrichmentManager, EnrichmentRegistry}
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils, OptionIor}

/** Expresses the end-to-end event pipeline supported by the Scala Common Enrich project. */
object EtlPipeline {

  /*
   * Feature flags available in the current version of Enrich
   * @param acceptInvalid Whether enriched events that are invalid against
   *                      atomic schema should be emitted as enriched events.
   *                      If not they will be emitted as bad rows
   */
  case class FeatureFlags(acceptInvalid: Boolean)

  /**
   * @param adapterRegistry Contains all of the events adapters
   * @param enrichmentRegistry Contains configuration for all enrichments to apply
   * @param client Our Iglu client, for schema lookups and validation
   * @param processor The ETL application (Spark/Beam/Stream enrich) and its version
   * @param etlTstamp The ETL timestamp
   * @param input The ValidatedMaybeCanonicalInput
   * @param featureFlags The feature flags available in the current version of Enrich
   * @param invalidCount Function to increment the count of invalid events
   * @param reportUnexpectedError Function to report an unexpected error thrown while enriching one event,
   *                              before that event is emitted as a `generic_error` bad row
   */
  def processEvents[F[_]: Sync](
    adapterRegistry: AdapterRegistry[F],
    enrichmentRegistry: EnrichmentRegistry[F],
    client: IgluCirceClient[F],
    processor: Processor,
    etlTstamp: DateTime,
    input: ValidatedNel[BadRow, CollectorPayload],
    featureFlags: FeatureFlags,
    invalidCount: F[Unit],
    reportUnexpectedError: Throwable => F[Unit],
    registryLookup: RegistryLookup[F],
    atomicFields: AtomicFields,
    emitFailed: Boolean,
    maxJsonDepth: Int
  ): F[List[OptionIor[BadRow, EnrichedEvent]]] =
    input match {
      case Validated.Valid(payload) =>
        adapterRegistry
          .toRawEvents(payload, client, processor, registryLookup, maxJsonDepth, Instant.ofEpochMilli(etlTstamp.getMillis))
          .flatMap {
            case Validated.Valid(rawEvents) =>
              rawEvents.toList.traverse { event =>
                // `defer` so that exceptions thrown while the `F` is being built are raised inside `F`
                // and reach the backstop, instead of losing the other events of the collector payload
                Sync[F]
                  .defer(
                    EnrichmentManager
                      .enrichEvent(
                        enrichmentRegistry,
                        client,
                        processor,
                        etlTstamp,
                        event,
                        featureFlags,
                        invalidCount,
                        registryLookup,
                        atomicFields,
                        emitFailed,
                        maxJsonDepth
                      )
                  )
                  .handleErrorWith(handleUnexpectedError(processor, event, reportUnexpectedError))
              }
            case Validated.Invalid(badRow) =>
              Sync[F].pure(List(OptionIor.Left(badRow)))
          }
      case Validated.Invalid(badRows) =>
        Sync[F].pure(badRows.toList.map(br => OptionIor.Left(br)))
    }

  /**
   * Backstop so that one event can never crash enrich or lose the other events of its collector payload.
   *
   * A `FatalEnrichError` is deliberately fatal and is re-raised. Anything else is reported and
   * turned into a single `generic_error` bad row holding the raw parameters of this event only.
   */
  private def handleUnexpectedError[F[_]: Sync](
    processor: Processor,
    event: RawEvent,
    reportUnexpectedError: Throwable => F[Unit]
  ): Throwable => F[OptionIor[BadRow, EnrichedEvent]] = {
    case fatal: FatalEnrichError =>
      Sync[F].raiseError(fatal)
    case t =>
      for {
        _ <- reportUnexpectedError(t)
        now <- Sync[F].realTimeInstant
        raw <- Sync[F].delay(RawEvent.toRawEvent(event).asJson.noSpaces)
      } yield OptionIor.Left(
        BadRow.GenericError(
          processor,
          Failure.GenericFailure(now, NonEmptyList.one(ConversionUtils.cleanStackTrace(t))),
          Payload.RawPayload(raw)
        )
      )
  }
}
