/*
 * Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.fs2

import scala.concurrent.duration.FiniteDuration

import cats.Show
import cats.data.EitherT
import cats.implicits._

import cats.effect.{Async, Blocker, Clock, ConcurrentEffect, ContextShift, ExitCase, Resource, Sync, Timer}
import cats.effect.concurrent.{Ref, Semaphore}

import fs2.Stream

import _root_.io.circe.Json

import _root_.io.sentry.{Sentry, SentryClient}

import org.http4s.client.{Client => HttpClient}

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.iglu.client.{Client => IgluClient}
import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}

import com.snowplowanalytics.snowplow.badrows.Processor

import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.BlockerF

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.{ConfigFile, ParsedConfigs}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Concurrency, Telemetry => TelemetryConfig}
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.{Clients, Metrics}
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Clients.Client
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.experimental.Metadata

import scala.concurrent.ExecutionContext
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Input.Kinesis

/**
 * All allocated resources, configs and mutable variables necessary for running Enrich process
 * Also responsiblle for initial assets downloading (during `assetsState` initialisation)
 *
 * @param igluClient          Iglu Client
 * @param registryLookup      Iglu registry lookup
 * @param enrichments         enrichment registry with all clients and parsed configuration files
 *                            it's wrapped in mutable variable because all resources need to be
 *                            reinitialized after DB assets are updated via [[Assets]] stream
 * @param semaphore           its permit is shared between enriching the events and updating the assets
 * @param assetsState         a main entity from [[Assets]] stream, controlling when assets
 *                            have to be replaced with newer ones
 * @param httpClient          client used to perform HTTP requests
 * @param blocker             thread pool for blocking operations and enrichments themselves
 * @param source              stream of records containing the collector payloads
 * @param sinkGood            function that sinks enriched event
 * @param sinkPii             function that sinks pii event
 * @param sinkBad             function that sinks an event that failed validation or enrichment
 * @param checkpoint          function that checkpoints input stream records
 * @param getPayload          function that extracts the collector payload bytes from a record
 * @param sentry              optional sentry client
 * @param metrics             common counters
 * @param metadata            metadata aggregations
 * @param assetsUpdatePeriod  time after which enrich assets should be refresh
 * @param goodAttributes      fields from an enriched event to use as output message attributes
 * @param piiAttributes       fields from a PII event to use as output message attributes
 * @param telemetryConfig     configuration for telemetry
 * @param processor           identifies enrich asset in bad rows
 * @param streamsSettings     parameters used to configure the streams
 * @param region              region in the cloud where enrich runs
 * @param cloud               cloud where enrich runs (AWS or GCP)
 * @param acceptInvalid       Whether enriched event not valid against atomic schema should be
 *                            emitted. If false they will be emitted as bad rows.
 * @tparam A                  type emitted by the source (e.g. `ConsumerRecord` for PubSub).
 *                            getPayload must be defined for this type, as well as checkpointing
 */
final case class Environment[F[_], A](
  igluClient: IgluClient[F, Json],
  registryLookup: RegistryLookup[F],
  enrichments: Ref[F, Environment.Enrichments[F]],
  semaphore: Semaphore[F],
  assetsState: Assets.State[F],
  httpClient: HttpClient[F],
  blocker: Blocker,
  source: Stream[F, A],
  sinkGood: AttributedByteSink[F],
  sinkPii: Option[AttributedByteSink[F]],
  sinkBad: ByteSink[F],
  checkpoint: List[A] => F[Unit],
  getPayload: A => Array[Byte],
  sentry: Option[SentryClient],
  metrics: Metrics[F],
  metadata: Metadata[F],
  assetsUpdatePeriod: Option[FiniteDuration],
  goodAttributes: EnrichedEvent => Map[String, String],
  piiAttributes: EnrichedEvent => Map[String, String],
  telemetryConfig: TelemetryConfig,
  processor: Processor,
  streamsSettings: Environment.StreamsSettings,
  region: Option[String],
  cloud: Option[Telemetry.Cloud],
  acceptInvalid: Boolean
)

object Environment {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  /** Registry with all allocated clients (MaxMind, IAB etc) and their original configs */
  final case class Enrichments[F[_]](registry: EnrichmentRegistry[F], configs: List[EnrichmentConf]) {

    /** Initialize same enrichments, specified by configs (in case DB files updated) */
    def reinitialize(blocker: BlockerF[F])(implicit A: Async[F]): F[Enrichments[F]] =
      Enrichments.buildRegistry(configs, blocker).map(registry => Enrichments(registry, configs))
  }

  object Enrichments {
    def make[F[_]: Async: Clock](configs: List[EnrichmentConf], blocker: BlockerF[F]): Resource[F, Ref[F, Enrichments[F]]] =
      Resource.eval {
        for {
          registry <- buildRegistry[F](configs, blocker)
          ref <- Ref.of(Enrichments[F](registry, configs))
        } yield ref
      }

    def buildRegistry[F[_]: Async](configs: List[EnrichmentConf], blocker: BlockerF[F]) =
      EnrichmentRegistry.build[F](configs, blocker).value.flatMap {
        case Right(reg) => Async[F].pure(reg)
        case Left(error) => Async[F].raiseError[EnrichmentRegistry[F]](new RuntimeException(error))
      }
  }

  /** Initialize and allocate all necessary resources */
  def make[F[_]: ConcurrentEffect: ContextShift: Clock: Timer, A](
    blocker: Blocker,
    ec: ExecutionContext,
    parsedConfigs: ParsedConfigs,
    source: Stream[F, A],
    sinkGood: Resource[F, AttributedByteSink[F]],
    sinkPii: Option[Resource[F, AttributedByteSink[F]]],
    sinkBad: Resource[F, ByteSink[F]],
    clients: Resource[F, List[Client[F]]],
    checkpoint: List[A] => F[Unit],
    getPayload: A => Array[Byte],
    processor: Processor,
    maxRecordSize: Int,
    cloud: Option[Telemetry.Cloud],
    getRegion: => Option[String],
    acceptInvalid: Boolean
  ): Resource[F, Environment[F, A]] = {
    val file = parsedConfigs.configFile
    for {
      sentry <- mkSentry[F](file)
      good <- sinkGood
      bad <- sinkBad
      pii <- sinkPii.sequence
      http <- Clients.mkHttp(ec)
      clts <- clients.map(Clients.init(http, _))
      igluClient <- IgluClient.parseDefault[F](parsedConfigs.igluJson).resource
      metrics <- Resource.eval(Metrics.build[F](blocker, file.monitoring.metrics))
      metadata <- Resource.eval(metadataReporter[F](file, processor.artifact, http))
      assets = parsedConfigs.enrichmentConfigs.flatMap(_.filesToCache)
      sem <- Resource.eval(Semaphore(1L))
      assetsState <- Resource.eval(Assets.State.make[F](blocker, sem, clts, assets))
      enrichments <- Enrichments.make[F](parsedConfigs.enrichmentConfigs, BlockerF.ofBlocker(blocker))
    } yield Environment[F, A](
      igluClient,
      Http4sRegistryLookup(http),
      enrichments,
      sem,
      assetsState,
      http,
      blocker,
      source,
      good,
      pii,
      bad,
      checkpoint,
      getPayload,
      sentry,
      metrics,
      metadata,
      file.assetsUpdatePeriod,
      parsedConfigs.goodAttributes,
      parsedConfigs.piiAttributes,
      file.telemetry,
      processor,
      StreamsSettings(file.concurrency, maxRecordSize),
      getRegionFromConfig(file).orElse(getRegion),
      cloud,
      acceptInvalid
    )
  }

  private def mkSentry[F[_]: Sync](config: ConfigFile): Resource[F, Option[SentryClient]] =
    config.monitoring.sentry.map(_.dsn) match {
      case Some(dsn) =>
        Resource
          .makeCase(Sync[F].delay(Sentry.init(dsn.toString))) {
            case (sentry, ExitCase.Error(e)) =>
              Sync[F].delay(sentry.sendException(e)) >>
                Logger[F].info("Sentry report has been sent")
            case _ => Sync[F].unit
          }
          .map(Some(_))
      case None =>
        Resource.pure[F, Option[SentryClient]](none[SentryClient])
    }

  private def metadataReporter[F[_]: ConcurrentEffect: ContextShift: Timer](
    config: ConfigFile,
    appName: String,
    httpClient: HttpClient[F]
  ): F[Metadata[F]] =
    config.experimental
      .flatMap(_.metadata)
      .map(metadataConfig => Metadata.build[F](metadataConfig, Metadata.HttpMetadataReporter[F](metadataConfig, appName, httpClient)))
      .getOrElse(Metadata.noop[F].pure[F])

  private implicit class EitherTOps[F[_], E: Show, A](eitherT: EitherT[F, E, A]) {
    def resource(implicit F: Sync[F]): Resource[F, A] = {
      val action: F[A] = eitherT.value.flatMap {
        case Right(a) => Sync[F].pure(a)
        case Left(error) => Sync[F].raiseError(new RuntimeException(error.show)) // Safe since we already parsed it
      }
      Resource.eval[F, A](action)
    }
  }

  case class StreamsSettings(concurrency: Concurrency, maxRecordSize: Int)

  private def getRegionFromConfig(file: ConfigFile): Option[String] =
    file.input match {
      case k: Kinesis =>
        k.region
      case _ =>
        None
    }
}
