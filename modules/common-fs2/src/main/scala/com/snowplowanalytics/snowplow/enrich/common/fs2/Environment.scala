/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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

import cats.effect.{Async, Blocker, Clock, Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.effect.concurrent.Ref

import fs2.concurrent.SignallingRef
import fs2.{Pipe, Stream}

import _root_.io.circe.Json

import _root_.io.sentry.{Sentry, SentryClient}

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}

import com.snowplowanalytics.snowplow.badrows.Processor

import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.BlockerF

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.{ConfigFile, ParsedConfigs}
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.{Clients, Metrics}

import scala.concurrent.ExecutionContext

/**
 * All allocated resources, configs and mutable variables necessary for running Enrich process
 * Also responsiblle for initial assets downloading (during `assetsState` initialisation)
 *
 * @param igluClient          Iglu Client
 * @param registryLookup      Iglu registry lookup
 * @param enrichments         enrichment registry with all clients and parsed configuration files
 *                            it's wrapped in mutable variable because all resources need to be
 *                            reinitialized after DB assets are updated via [[Assets]] stream
 * @param pauseEnrich         a signalling reference that can pause a raw stream and enrichment,
 *                            should be used only by [[Assets]]
 * @param assetsState         a main entity from [[Assets]] stream, controlling when assets
 *                            have to be replaced with newer ones
 * @param blocker             thread pool for blocking operations and enrichments themselves
 * @param source              a stream of records
 * @param good                a sink for successfully enriched events
 * @param pii                 a sink for pii enriched events
 * @param bad                 a sink for events that failed validation or enrichment
 * @param checkpointer        pipe used to checkpoint the records
 * @param getPayload          function that extracts the collector payload bytes from a record
 * @param sentry              optional sentry client
 * @param metrics             common counters
 * @param assetsUpdatePeriod  time after which enrich assets should be refresh
 * @param goodAttributes      fields from an enriched event to use as output message attributes
 * @param piiAttributes       fields from a PII event to use as output message attributes
 * @param processor           identifies enrich asset in bad rows
 */
final case class Environment[F[_], A](
  igluClient: Client[F, Json],
  registryLookup: RegistryLookup[F],
  enrichments: Ref[F, Environment.Enrichments[F]],
  pauseEnrich: SignallingRef[F, Boolean],
  assetsState: Assets.State[F],
  blocker: Blocker,
  source: Stream[F, A],
  good: AttributedByteSink[F],
  pii: Option[AttributedByteSink[F]],
  bad: ByteSink[F],
  checkpointer: Pipe[F, A, Unit],
  getPayload: A => Array[Byte],
  sentry: Option[SentryClient],
  metrics: Metrics[F],
  assetsUpdatePeriod: Option[FiniteDuration],
  goodAttributes: EnrichedEvent => Map[String, String],
  piiAttributes: EnrichedEvent => Map[String, String],
  processor: Processor
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
    goodSink: Resource[F, AttributedByteSink[F]],
    piiSink: Option[Resource[F, AttributedByteSink[F]]],
    badSink:  Resource[F, ByteSink[F]],
    checkpointer: Pipe[F, A, Unit],
    getPayload: A => Array[Byte],
    processor: Processor
  ): Resource[F, Environment[F, A]] = {
      val file = parsedConfigs.configFile
      for {
        good <- goodSink
        bad <-badSink
        pii <- piiSink.sequence
        http <- Clients.mkHTTP(ec)
        client <- Client.parseDefault[F](parsedConfigs.igluJson).resource
        metrics <- Resource.eval(metricsReporter[F](blocker, file))
        assets = parsedConfigs.enrichmentConfigs.flatMap(_.filesToCache)
        pauseEnrich <- makePause[F]
        assets <- Assets.State.make[F](blocker, pauseEnrich, assets, http)
        enrichments <- Enrichments.make[F](parsedConfigs.enrichmentConfigs, BlockerF.ofBlocker(blocker))
        sentry <- file.monitoring.flatMap(_.sentry).map(_.dsn) match {
                    case Some(dsn) => Resource.eval[F, Option[SentryClient]](Sync[F].delay(Sentry.init(dsn.toString).some))
                    case None => Resource.pure[F, Option[SentryClient]](none[SentryClient])
                  }
        _ <- Resource.eval(pauseEnrich.set(false) *> Logger[F].info("Enrich environment initialized"))
      } yield Environment[F, A](
        client,
        Http4sRegistryLookup(http),
        enrichments,
        pauseEnrich,
        assets,
        blocker,
        source,
        good,
        pii,
        bad,
        checkpointer,
        getPayload,
        sentry,
        metrics,
        file.assetsUpdatePeriod,
        parsedConfigs.goodAttributes,
        parsedConfigs.piiAttributes,
        processor
      )
    }

  /**
   * Make sure `enrichPause` gets into paused state before destroying pipes
   * Initialised into `true` because enrich stream should not start until
   * [[Assets.State]] is constructed - it will download all assets
   */
  def makePause[F[_]: Concurrent]: Resource[F, SignallingRef[F, Boolean]] =
    Resource.make(SignallingRef(true))(_.set(true))

  private def metricsReporter[F[_]: ConcurrentEffect: ContextShift: Timer](blocker: Blocker, config: ConfigFile): F[Metrics[F]] =
    config.monitoring.flatMap(_.metrics).map(Metrics.build[F](blocker, _)).getOrElse(Metrics.noop[F].pure[F])

  private implicit class EitherTOps[F[_], E: Show, A](eitherT: EitherT[F, E, A]) {
    def resource(implicit F: Sync[F]): Resource[F, A] = {
      val action: F[A] = eitherT.value.flatMap {
        case Right(a) => Sync[F].pure(a)
        case Left(error) => Sync[F].raiseError(new RuntimeException(error.show)) // Safe since we already parsed it
      }
      Resource.eval[F, A](action)
    }
  }
}
