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

import cats.Show
import cats.data.EitherT
import cats.implicits._

import cats.effect.{Async, Blocker, Clock, Concurrent, ContextShift, Resource, Sync}

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
import com.snowplowanalytics.snowplow.enrich.fs2.io.{FileSystem, Sinks, Source}

/**
 * All allocated resources, configs and mutable variables necessary for running Enrich process
 * @param config original HOCON configuration
 * @param resolver Iglu Client
 * @param enrichments enrichment registry with all its clients and parsed configuration files
 * @param stop a signalling reference that can pause a raw stream and enrichment,
 *             used for [[AssetsRefresh]]
 * @param blocker thread pool for blocking operations and enrichments itself
 * @param source a stream of raw collector payloads
 * @param good a sink for successfully enriched events
 * @param bad a sink for events that failed validation or enrichment
 * @param sentry optional sentry client
 */
case class Environment[F[_]](
  config: ConfigFile,
  resolver: Client[F, Json],
  enrichments: Environment.Enrichments[F],
  stop: SignallingRef[F, Boolean],
  blocker: Blocker,
  source: RawSource[F],
  good: GoodSink[F],
  bad: BadSink[F],
  sentry: Option[SentryClient]
)

object Environment {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  type Parsed[F[_], A] = EitherT[F, String, A]

  type Allocated[F[_]] = Parsed[F, Resource[F, Environment[F]]]

  /** Registry with all allocated clients (MaxMind, IAB etc) and their original configs */
  case class Enrichments[F[_]](registry: EnrichmentRegistry[F], configs: List[EnrichmentConf])

  /** Schema for all enrichments combined */
  val EnrichmentsKey: SchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow", "enrichments", "jsonschema", SchemaVer.Full(1, 0, 0))

  /** Initialize and allocate all necessary resources */
  def init[F[_]: Concurrent: ContextShift: Clock](config: CliConfig): Allocated[F] =
    parse[F](config).map { parsedConfigs =>
      for {
        client <- Client.parseDefault[F](parsedConfigs.igluJson).resource
        registry <- EnrichmentRegistry.build[F](parsedConfigs.enrichmentConfigs).resource
        blocker <- Blocker[F]
        rawSource = Source.read[F](blocker, parsedConfigs.configFile.auth, parsedConfigs.configFile.input)
        goodSink <- Sinks.goodSink[F](parsedConfigs.configFile.auth, parsedConfigs.configFile.good)
        badSink <- Sinks.badSink[F](parsedConfigs.configFile.auth, parsedConfigs.configFile.bad)
        stop <- Resource.liftF(SignallingRef(true))
        enrichments = Enrichments(registry, parsedConfigs.enrichmentConfigs)
        sentry <- parsedConfigs.configFile.sentryDsn match {
                    case Some(dsn) => Resource.liftF[F, Option[SentryClient]](Sync[F].delay(Sentry.init(dsn.toString).some))
                    case None => Resource.pure[F, Option[SentryClient]](none[SentryClient])
                  }
      } yield Environment[F](parsedConfigs.configFile, client, enrichments, stop, blocker, rawSource, goodSink, badSink, sentry)
    }

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
      _ <- EitherT.liftF(Logger[F].info(show"Parsed Iglu Client with following registries: ${client.resolver.repos.map(_.config.name)}"))
      configs <- EitherT(EnrichmentRegistry.parse[F](enrichmentJsons, client, false).map(_.toEither)).leftMap { x =>
                   show"Cannot decode enrichments ${x.mkString_(", ")}"
                 }
      _ <- EitherT.liftF(Logger[F].info(show"Parsed following enrichments: ${configs.map(_.schemaKey.toSchemaUri)}"))
    } yield ParsedConfigs(igluJson, configs, configFile)

  private[fs2] case class ParsedConfigs(
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
