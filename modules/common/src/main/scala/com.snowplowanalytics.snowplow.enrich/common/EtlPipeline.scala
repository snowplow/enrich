/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.common

import scala.concurrent.duration.FiniteDuration

import cats.Monad
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.implicits._

import cats.effect.{Clock, Concurrent, Sync, Timer}

import org.joda.time.DateTime

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows.{BadRow, FailureDetails, Processor}

import com.snowplowanalytics.snowplow.enrich.common.adapters.{AdapterRegistry, RawEvent}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{EnrichmentManager, EnrichmentRegistry, Timeouts}
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
   * @param timeouts Configured timeouts for the enriching step
   * @return the ValidatedMaybeCanonicalOutput. Thanks to flatMap, will include any validation
   * errors contained within the ValidatedMaybeCanonicalInput
   */
  def processEvents[F[_]: Clock: Concurrent: RegistryLookup: Timer](
    adapterRegistry: AdapterRegistry[F],
    enrichmentRegistry: EnrichmentRegistry[F],
    client: IgluCirceClient[F],
    processor: Processor,
    etlTstamp: DateTime,
    input: ValidatedNel[BadRow, Option[CollectorPayload]],
    featureFlags: FeatureFlags,
    invalidCount: F[Unit],
    timeouts: Timeouts
  ): F[List[Validated[BadRow, EnrichedEvent]]] =
    input match {
      case Validated.Valid(Some(payload)) =>
        adapterRegistry
          .toRawEvents(payload, client, processor)
          .flatMap {
            case Validated.Valid(rawEvents) =>
              rawEvents.toList.traverse { event =>
                Concurrent
                  .timeoutTo(
                    EnrichmentManager
                      .enrichEvent(
                        enrichmentRegistry,
                        client,
                        processor,
                        etlTstamp,
                        event,
                        featureFlags,
                        invalidCount,
                        timeouts
                      )
                      .value,
                    timeouts.enriching,
                    Sync[F].delay {
                      val br = createTimeoutBadRow(event, timeouts.enriching, processor)
                      Left(br): Either[BadRow, EnrichedEvent]
                    }
                  )
                  .map(_.toValidated)
              }
            case Validated.Invalid(badRow) =>
              Monad[F].pure(List(badRow.invalid[EnrichedEvent]))
          }
      case Validated.Invalid(badRows) =>
        Monad[F].pure(badRows.map(_.invalid[EnrichedEvent])).map(_.toList)
      case Validated.Valid(None) =>
        Monad[F].pure(List.empty[Validated[BadRow, EnrichedEvent]])
    }

  private def createTimeoutBadRow(
    event: RawEvent,
    timeout: FiniteDuration,
    processor: Processor
  ): BadRow = {
    val failure = FailureDetails.EnrichmentFailure(
      None,
      FailureDetails.EnrichmentFailureMessage.Simple(s"Enriching the event timed out ($timeout)")
    )
    EnrichmentManager.buildEnrichmentFailuresBadRow(
      NonEmptyList.one(failure),
      EnrichedEvent.toPartiallyEnrichedEvent(new EnrichedEvent),
      RawEvent.toRawEvent(event),
      processor
    )
  }
}
