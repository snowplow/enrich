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
package com.snowplowanalytics.snowplow.enrich.common

import cats.Monad
import cats.data.{Validated, ValidatedNel}
import cats.effect.Clock
import cats.implicits._

import org.joda.time.DateTime

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}

import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{EnrichmentManager, EnrichmentRegistry}
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

/** Expresses the end-to-end event pipeline supported by the Scala Common Enrich project. */
object EtlPipeline {

  /*
   * Feature flags available in the current version of Enrich
   * @param acceptInvalid Whether enriched events that are invalid against
   *                      atomic schema should be emitted as enriched events.
   *                      If not they will be emitted as bad rows
   * @param legacyEnrichmentOrder Whether to use the incorrect enrichment order which was historically
   *                      used in early versions of enrich-pubsub and enrich-kinesis.
   */
  case class FeatureFlags(acceptInvalid: Boolean, legacyEnrichmentOrder: Boolean)

  /**
   * A helper method to take a ValidatedMaybeCanonicalInput and transform it into a List (possibly
   * empty) of ValidatedCanonicalOutputs.
   * We have to do some unboxing because enrichEvent expects a raw CanonicalInput as its argument,
   * not a MaybeCanonicalInput.
   * @param adapterRegistry Contains all of the events adapters
   * @param enrichmentRegistry Contains configuration for all enrichments to apply
   * @param client Our Iglu client, for schema lookups and validation
   * @param processor The ETL application (Spark/Beam/Stream enrich) and its version
   * @param etlTstamp The ETL timestamp
   * @param input The ValidatedMaybeCanonicalInput
   * @param featureFlags The feature flags available in the current version of Enrich
   * @param invalidCount Function to increment the count of invalid events
   * @return the ValidatedMaybeCanonicalOutput. Thanks to flatMap, will include any validation
   * errors contained within the ValidatedMaybeCanonicalInput
   */
  def processEvents[F[_]: Clock: Monad: RegistryLookup](
    adapterRegistry: AdapterRegistry[F],
    enrichmentRegistry: EnrichmentRegistry[F],
    client: IgluCirceClient[F],
    processor: Processor,
    etlTstamp: DateTime,
    input: ValidatedNel[BadRow, Option[CollectorPayload]],
    featureFlags: FeatureFlags,
    invalidCount: F[Unit]
  ): F[List[Validated[BadRow, EnrichedEvent]]] =
    input match {
      case Validated.Valid(Some(payload)) =>
        adapterRegistry
          .toRawEvents(payload, client, processor)
          .flatMap {
            case Validated.Valid(rawEvents) =>
              rawEvents.toList.traverse { event =>
                EnrichmentManager
                  .enrichEvent(
                    enrichmentRegistry,
                    client,
                    processor,
                    etlTstamp,
                    event,
                    featureFlags,
                    invalidCount
                  )
                  .toValidated
              }
            case Validated.Invalid(badRow) =>
              Monad[F].pure(List(badRow.invalid[EnrichedEvent]))
          }
      case Validated.Invalid(badRows) =>
        Monad[F].pure(badRows.map(_.invalid[EnrichedEvent])).map(_.toList)
      case Validated.Valid(None) =>
        Monad[F].pure(List.empty[Validated[BadRow, EnrichedEvent]])
    }
}
