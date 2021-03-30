/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.fs2

import scala.concurrent.duration.FiniteDuration

import cats.Show
import cats.data.EitherT
import cats.implicits._

import cats.effect.{Async, Blocker, Clock, Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.effect.concurrent.Ref

import fs2.concurrent.SignallingRef

import _root_.io.circe.Json
import _root_.io.circe.syntax._

import _root_.io.sentry.{Sentry, SentryClient}
import _root_.io.chrisdavenport.log4cats.Logger
import _root_.io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.fs2.config.{CliConfig, ConfigFile}
import com.snowplowanalytics.snowplow.enrich.fs2.io.{FileSystem, Metrics, Sinks, Source}

/**
 * All allocated resources, configs and mutable variables necessary for running Enrich process
 * Also responsiblle for initial assets downloading (during `assetsState` initialisation)
 *
 * @param igluClient          Iglu Client
 * @param enrichments         enrichment registry with all clients and parsed configuration files
 *                            it's wrapped in mutable variable because all resources need to be
 *                            reinitialized after DB assets are updated via [[Assets]] stream
 * @param pauseEnrich         a signalling reference that can pause a raw stream and enrichment,
 *                            should be used only by [[Assets]]
 * @param assetsState         a main entity from [[Assets]] stream, controlling when assets
 *                            have to be replaced with newer ones
 * @param blocker             thread pool for blocking operations and enrichments themselves
 * @param source              a stream of raw collector payloads
 * @param good                a sink for successfully enriched events
 * @param pii                 a sink for pii enriched events
 * @param bad                 a sink for events that failed validation or enrichment
 * @param sentry              optional sentry client
 * @param metrics             common counters
 * @param assetsUpdatePeriod  time after which enrich assets should be refresh
 * @param metricsReportPeriod period after which metrics are updated
 */
final case class Environment[F[_]](
  igluClient: Client[F, Json],
  enrichments: Ref[F, Environment.Enrichments[F]],
  pauseEnrich: SignallingRef[F, Boolean],
  assetsState: Assets.State[F],
  blocker: Blocker,
  source: RawSource[F],
  good: ByteSink[F],
  pii: Option[ByteSink[F]],
  bad: ByteSink[F],
  sentry: Option[SentryClient],
  metrics: Metrics[F],
  assetsUpdatePeriod: Option[FiniteDuration],
  metricsReportPeriod: Option[FiniteDuration]
)

object Environment {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  type Parsed[F[_], A] = EitherT[F, String, A]

  type Allocated[F[_]] = Parsed[F, Resource[F, Environment[F]]]

  /** Registry with all allocated clients (MaxMind, IAB etc) and their original configs */
  final case class Enrichments[F[_]](registry: EnrichmentRegistry[F], configs: List[EnrichmentConf]) {

    /** Initialize same enrichments, specified by configs (in case DB files updated) */
    def reinitialize(implicit A: Async[F]): F[Enrichments[F]] =
      Enrichments.buildRegistry(configs).map(registry => Enrichments(registry, configs))
  }

  object Enrichments {
    def make[F[_]: Async: Clock](configs: List[EnrichmentConf]): Resource[F, Ref[F, Enrichments[F]]] =
      Resource.liftF {
        for {
          registry <- buildRegistry[F](configs)
          ref <- Ref.of(Enrichments[F](registry, configs))
        } yield ref
      }

    def buildRegistry[F[_]: Async](configs: List[EnrichmentConf]) =
      EnrichmentRegistry.build[F](configs).value.flatMap {
        case Right(reg) => Async[F].pure(reg)
        case Left(error) => Async[F].raiseError[EnrichmentRegistry[F]](new RuntimeException(error))
      }
  }

  /** Schema for all enrichments combined */
  val EnrichmentsKey: SchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow", "enrichments", "jsonschema", SchemaVer.Full(1, 0, 0))

  /** Initialize and allocate all necessary resources */
  def make[F[_]: ConcurrentEffect: ContextShift: Clock: Timer](config: CliConfig): Allocated[F] =
    parse[F](config).map { parsedConfigs =>
      val file = parsedConfigs.configFile
      for {
        client <- Client.parseDefault[F](parsedConfigs.igluJson).resource
        blocker <- Blocker[F]
        metrics <- Metrics.resource[F]
        rawSource = Source.read[F](blocker, file.auth, file.input)
        goodSink <- Sinks.sink[F](blocker, file.auth, file.good)
        piiSink <- file.pii.map(f => Sinks.sink[F](blocker, file.auth, f)).sequence
        badSink <- Sinks.sink[F](blocker, file.auth, file.bad)
        assets = parsedConfigs.enrichmentConfigs.flatMap(_.filesToCache)
        pauseEnrich <- makePause[F]
        assets <- Assets.State.make[F](blocker, pauseEnrich, assets)
        enrichments <- Enrichments.make[F](parsedConfigs.enrichmentConfigs)
        sentry <- file.sentry.map(_.dsn) match {
                    case Some(dsn) => Resource.liftF[F, Option[SentryClient]](Sync[F].delay(Sentry.init(dsn.toString).some))
                    case None => Resource.pure[F, Option[SentryClient]](none[SentryClient])
                  }
        _ <- Resource.liftF(pauseEnrich.set(false) *> Logger[F].info("Enrich environment initialized"))
      } yield Environment[F](client,
                             enrichments,
                             pauseEnrich,
                             assets,
                             blocker,
                             rawSource,
                             goodSink,
                             piiSink,
                             badSink,
                             sentry,
                             metrics,
                             file.assetsUpdatePeriod,
                             file.metricsReportPeriod
      )
    }

  /**
   * Make sure `enrichPause` gets into paused state before destroying pipes
   * Initialised into `true` because enrich stream should not start until
   * [[Assets.State]] is constructed - it will download all assets
   */
  def makePause[F[_]: Concurrent]: Resource[F, SignallingRef[F, Boolean]] =
    Resource.make(SignallingRef(true))(_.set(true))

  /** Decode base64-encoded configs, passed via CLI. Read files, validate and parse */
  def parse[F[_]: Async: Clock: ContextShift](config: CliConfig): Parsed[F, ParsedConfigs] =
    for {
      igluJson <- config.resolver.fold(b => EitherT.rightT[F, String](b.value), p => FileSystem.readJson[F](p))
      enrichmentJsons <- config.enrichments match {
                           case Left(base64) =>
                             EitherT.rightT[F, String](base64.value)
                           case Right(path) =>
                             FileSystem
                               .readJsonDir[F](path)
                               .map(jsons => Json.arr(jsons: _*))
                               .map(json => SelfDescribingData(EnrichmentsKey, json).asJson)
                         }
      configFile <- ConfigFile.parse[F](config.config)
      client <- Client.parseDefault[F](igluJson).leftMap(x => show"Cannot decode Iglu Client. $x")
      _ <- EitherT.liftF(
             Logger[F].info(show"Parsed Iglu Client with following registries: ${client.resolver.repos.map(_.config.name).mkString(", ")}")
           )
      configs <- EitherT(EnrichmentRegistry.parse[F](enrichmentJsons, client, false).map(_.toEither)).leftMap { x =>
                   show"Cannot decode enrichments ${x.mkString_(", ")}"
                 }
      _ <- EitherT.liftF(Logger[F].info(show"Parsed following enrichments: ${configs.map(_.schemaKey.name).mkString(", ")}"))
    } yield ParsedConfigs(igluJson, configs, configFile)

  private[fs2] final case class ParsedConfigs(
    igluJson: Json,
    enrichmentConfigs: List[EnrichmentConf],
    configFile: ConfigFile
  )

  private implicit class EitherTOps[F[_], E: Show, A](eitherT: EitherT[F, E, A]) {
    def resource(implicit F: Sync[F]): Resource[F, A] = {
      val action: F[A] = eitherT.value.flatMap {
        case Right(a) => Sync[F].pure(a)
        case Left(error) => Sync[F].raiseError(new RuntimeException(error.show)) // Safe since we already parsed it
      }
      Resource.liftF[F, A](action)
    }
  }
}
