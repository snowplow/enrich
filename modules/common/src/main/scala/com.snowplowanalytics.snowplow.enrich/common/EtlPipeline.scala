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

import cats.data.{Validated, ValidatedNel}
import cats.effect.Sync
import cats.implicits._

import org.joda.time.DateTime

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}

import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{AtomicFields, EnrichmentManager, EnrichmentRegistry}
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.OptionIor

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
                  .value
              }
            case Validated.Invalid(badRow) =>
              Sync[F].pure(List(OptionIor.Left(badRow)))
          }
      case Validated.Invalid(badRows) =>
        Sync[F].pure(badRows.toList.map(br => OptionIor.Left(br)))
    }
}
