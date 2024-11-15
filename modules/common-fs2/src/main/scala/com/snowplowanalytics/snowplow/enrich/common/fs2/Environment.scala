/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2

import java.util.concurrent.TimeoutException

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import cats.Show
import cats.data.EitherT
import cats.implicits._

import cats.effect.kernel.{Async, Ref, Resource, Sync}
import cats.effect.kernel.Resource.ExitCase
import cats.effect.std.Semaphore

import fs2.Stream

import _root_.io.sentry.{Sentry, SentryClient}

import org.http4s.client.{Client => Http4sClient}
import org.http4s.Status

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}

import com.snowplowanalytics.snowplow.badrows.Processor

import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.RemoteAdapter
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{AtomicFields, EnrichmentRegistry}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.ApiRequestConf
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.{HttpClient, ShiftExecution}

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.{ConfigFile, ParsedConfigs}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Input.Kinesis
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{
  Cloud,
  Concurrency,
  FeatureFlags,
  RemoteAdapterConfigs,
  Telemetry => TelemetryConfig
}
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.{Clients, Metrics}
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Clients.Client
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.experimental.Metadata

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
 * @param blockingEC          thread pool for blocking operations (only used for IP lookup)
 * @param shifter             thread pool for blocking jdbc operations in the SqlEnrichment
 * @param source              stream of records containing the collector payloads
 * @param sinkGood            function that sinks enriched event
 * @param sinkPii             function that sinks pii event
 * @param sinkBad             function that sinks an event that failed validation or enrichment
 * @param sinkIncomplete      function that sinks incomplete events
 * @param checkpoint          function that checkpoints input stream records
 * @param getPayload          function that extracts the collector payload bytes from a record
 * @param sentry              optional sentry client
 * @param metrics             common counters
 * @param metadata            metadata aggregations
 * @param assetsUpdatePeriod  time after which enrich assets should be refresh
 * @param goodPartitionKey    field from an enriched event to use as output partition key
 * @param piiPartitionKey     field from a PII event to use as output partition key
 * @param goodAttributes      fields from an enriched event to use as output message attributes
 * @param piiAttributes       fields from a PII event to use as output message attributes
 * @param telemetryConfig     configuration for telemetry
 * @param processor           identifies enrich asset in bad rows
 * @param streamsSettings     parameters used to configure the streams
 * @param region              region in the cloud where enrich runs
 * @param cloud               cloud where enrich runs (AWS or GCP)
 * @param featureFlags        Feature flags for the current version of enrich, liable to change in future versions.
 * @tparam A                  type emitted by the source (e.g. `ConsumerRecord` for PubSub).
 *                            getPayload must be defined for this type, as well as checkpointing
 */
final case class Environment[F[_], A](
  igluClient: IgluCirceClient[F],
  registryLookup: RegistryLookup[F],
  enrichments: Ref[F, Environment.Enrichments[F]],
  semaphore: Semaphore[F],
  assetsState: Assets.State[F],
  httpClient: Http4sClient[F],
  blockingEC: ExecutionContext,
  shifter: ShiftExecution[F],
  source: Stream[F, A],
  adapterRegistry: AdapterRegistry[F],
  sinkGood: AttributedByteSink[F],
  sinkPii: Option[AttributedByteSink[F]],
  sinkBad: ByteSink[F],
  sinkIncomplete: Option[AttributedByteSink[F]],
  checkpoint: List[A] => F[Unit],
  getPayload: A => Array[Byte],
  sentry: Option[SentryClient],
  metrics: Metrics[F],
  metadata: Metadata[F],
  assetsUpdatePeriod: Option[FiniteDuration],
  goodPartitionKey: EnrichedEvent => String,
  piiPartitionKey: EnrichedEvent => String,
  goodAttributes: EnrichedEvent => Map[String, String],
  piiAttributes: EnrichedEvent => Map[String, String],
  telemetryConfig: TelemetryConfig,
  processor: Processor,
  streamsSettings: Environment.StreamsSettings,
  region: Option[String],
  cloud: Option[Cloud],
  featureFlags: FeatureFlags,
  atomicFields: AtomicFields,
  maxJsonDepth: Int
)

object Environment {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  /** Registry with all allocated clients (MaxMind, IAB etc) and their original configs */
  final case class Enrichments[F[_]: Async](
    registry: EnrichmentRegistry[F],
    configs: List[EnrichmentConf],
    httpApiEnrichment: HttpClient[F]
  ) {

    /** Initialize same enrichments, specified by configs (in case DB files updated) */
    def reinitialize(blockingEC: ExecutionContext, shifter: ShiftExecution[F]): F[Enrichments[F]] =
      Enrichments
        .buildRegistry(configs, blockingEC, shifter, httpApiEnrichment)
        .map(registry => Enrichments(registry, configs, httpApiEnrichment))
  }

  object Enrichments {
    def make[F[_]: Async](
      configs: List[EnrichmentConf],
      blockingEC: ExecutionContext,
      shifter: ShiftExecution[F]
    ): Resource[F, Ref[F, Enrichments[F]]] =
      for {
        // We don't want the HTTP client of API enrichment to be reinitialized each time the assets are refreshed
        httpClient <- configs.collectFirst { case api: ApiRequestConf => api.api.timeout } match {
                        case Some(timeout) =>
                          Clients.mkHttp(readTimeout = timeout.millis).map(HttpClient.fromHttp4sClient[F])
                        case None =>
                          Resource.pure[F, HttpClient[F]](HttpClient.noop[F])
                      }
        registry <- Resource.eval(buildRegistry[F](configs, blockingEC, shifter, httpClient))
        ref <- Resource.eval(Ref.of(Enrichments[F](registry, configs, httpClient)))
      } yield ref

    def buildRegistry[F[_]: Async](
      configs: List[EnrichmentConf],
      blockingEC: ExecutionContext,
      shifter: ShiftExecution[F],
      httpApiEnrichment: HttpClient[F]
    ) =
      EnrichmentRegistry.build[F](configs, shifter, httpApiEnrichment, blockingEC).value.flatMap {
        case Right(reg) => Async[F].pure(reg)
        case Left(error) => Async[F].raiseError[EnrichmentRegistry[F]](new RuntimeException(error))
      }
  }

  /** Initialize and allocate all necessary resources */
  def make[F[_]: Async, A](
    blockingEC: ExecutionContext,
    parsedConfigs: ParsedConfigs,
    source: Stream[F, A],
    sinkGood: Resource[F, AttributedByteSink[F]],
    sinkPii: Option[Resource[F, AttributedByteSink[F]]],
    sinkBad: Resource[F, ByteSink[F]],
    sinkIncomplete: Option[Resource[F, AttributedByteSink[F]]],
    clients: Resource[F, List[Client[F]]],
    checkpoint: List[A] => F[Unit],
    getPayload: A => Array[Byte],
    processor: Processor,
    maxRecordSize: Int,
    cloud: Option[Cloud],
    getRegion: => Option[String],
    featureFlags: FeatureFlags,
    atomicFields: AtomicFields
  ): Resource[F, Environment[F, A]] = {
    val file = parsedConfigs.configFile
    for {
      _ <- Resource.make(Logger[F].info("Running Enrich"))(_ => Logger[F].info("Enrich stopped"))
      sentry <- mkSentry[F](file)
      good <- sinkGood
      bad <- sinkBad
      pii <- sinkPii.sequence
      incomplete <- sinkIncomplete.sequence
      http4s <- Clients.mkHttp()
      clts <- clients.map(Clients.init(http4s, _))
      igluClient <- IgluCirceClient.parseDefault[F](parsedConfigs.igluJson, parsedConfigs.configFile.maxJsonDepth).resource
      remoteAdaptersEnabled = file.remoteAdapters.configs.nonEmpty
      metrics <- Resource.eval(Metrics.build[F](file.monitoring.metrics, remoteAdaptersEnabled, incomplete.isDefined))
      metadata <- metadataReporter[F](file, processor.artifact, http4s)
      assets = parsedConfigs.enrichmentConfigs.flatMap(_.filesToCache)
      remoteAdapters <- prepareRemoteAdapters[F](file.remoteAdapters, metrics)
      adapterRegistry = new AdapterRegistry(remoteAdapters, file.adaptersSchemas)
      sem <- Resource.eval(Semaphore(1L))
      assetsState <- Resource.eval(Assets.State.make[F](sem, clts, assets))
      shifter <- ShiftExecution.ofSingleThread[F]
      enrichments <- Enrichments.make[F](parsedConfigs.enrichmentConfigs, blockingEC, shifter)
    } yield Environment[F, A](
      igluClient,
      Http4sRegistryLookup(http4s),
      enrichments,
      sem,
      assetsState,
      http4s,
      blockingEC,
      shifter,
      source,
      adapterRegistry,
      good,
      pii,
      bad,
      incomplete,
      checkpoint,
      getPayload,
      sentry,
      metrics,
      metadata,
      file.assetsUpdatePeriod,
      parsedConfigs.goodPartitionKey,
      parsedConfigs.piiPartitionKey,
      parsedConfigs.goodAttributes,
      parsedConfigs.piiAttributes,
      file.telemetry,
      processor,
      StreamsSettings(file.concurrency, maxRecordSize),
      getRegionFromConfig(file).orElse(getRegion),
      cloud,
      featureFlags,
      atomicFields,
      parsedConfigs.configFile.maxJsonDepth
    )
  }

  private def mkSentry[F[_]: Sync](config: ConfigFile): Resource[F, Option[SentryClient]] =
    config.monitoring.sentry.map(_.dsn) match {
      case Some(dsn) =>
        Resource
          .makeCase(Sync[F].delay(Sentry.init(dsn.toString))) {
            case (sentry, ExitCase.Errored(e)) =>
              Sync[F].delay(sentry.sendException(e)) >>
                Logger[F].info("Sentry report has been sent")
            case _ => Sync[F].unit
          }
          .map(Some(_))
      case None =>
        Resource.pure[F, Option[SentryClient]](none[SentryClient])
    }

  private def metadataReporter[F[_]: Async](
    config: ConfigFile,
    appName: String,
    httpClient: Http4sClient[F]
  ): Resource[F, Metadata[F]] =
    config.experimental.flatMap(_.metadata) match {
      case Some(metadataConfig) =>
        for {
          reporter <- Metadata.HttpMetadataReporter.resource(metadataConfig, appName, httpClient)
          metadata <- Resource.eval(Metadata.build(metadataConfig, reporter))
        } yield metadata
      case None =>
        Resource.pure(Metadata.noop[F])
    }

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

  def prepareRemoteAdapters[F[_]: Async](
    remoteAdapters: RemoteAdapterConfigs,
    metrics: Metrics[F]
  ): Resource[F, Map[(String, String), RemoteAdapter[F]]] =
    remoteAdapters.configs match {
      case adapters if adapters.nonEmpty =>
        Clients
          .mkHttp(remoteAdapters.connectionTimeout, remoteAdapters.readTimeout, remoteAdapters.maxConnections)
          .map(enableRemoteAdapterMetrics(metrics))
          .map { client =>
            val http = HttpClient.fromHttp4sClient(client)
            adapters.map { config =>
              (config.vendor, config.version) -> RemoteAdapter[F](http, config.url)
            }.toMap
          }
      case _ =>
        Resource.pure[F, Map[(String, String), RemoteAdapter[F]]](Map.empty)
    }

  def enableRemoteAdapterMetrics[F[_]: Sync](metrics: Metrics[F])(inner: Http4sClient[F]): Http4sClient[F] =
    Http4sClient { req =>
      inner.run(req).attemptTap {
        case Left(_: TimeoutException) => Resource.eval(metrics.remoteAdaptersTimeoutCount)
        case Left(_) => Resource.eval(metrics.remoteAdaptersFailureCount)
        case Right(response) =>
          response.status.responseClass match {
            case Status.Successful => Resource.eval(metrics.remoteAdaptersSuccessCount)
            case _ => Resource.eval(metrics.remoteAdaptersFailureCount)
          }
      }
    }
}
