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
package com.snowplowanalytics.snowplow.enrich.common.fs2.config

import scala.concurrent.duration.FiniteDuration

import cats.implicits._
import cats.data.EitherT

import cats.effect.kernel.Sync

import _root_.io.circe.Decoder
import _root_.io.circe.config.syntax._
import _root_.io.circe.generic.extras.semiauto.deriveConfiguredDecoder

import com.typesafe.config.{ConfigFactory, Config => TSConfig}

import com.snowplowanalytics.snowplow.enrich.common.adapters.AdaptersSchemas
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io._

/**
 * Parsed HOCON configuration file
 *
 * @param input input (PubSub, Kinesis etc)
 * @param output wraps good bad and pii outputs (PubSub, Kinesis, FS etc)
 * @param assetsUpdatePeriod time after which assets should be updated, in minutes
 * @param monitoring configuration for sentry and metrics
 * @param telemetry configuration for telemetry
 * @param featureFlags to activate/deactivate enrich features
 * @param experimental configuration for experimental features
 * @param adaptersSchemas configuration for adapters
 */
final case class ConfigFile(
  input: Input,
  output: Outputs,
  concurrency: Concurrency,
  assetsUpdatePeriod: Option[FiniteDuration],
  remoteAdapters: RemoteAdapterConfigs,
  monitoring: Monitoring,
  telemetry: Telemetry,
  featureFlags: FeatureFlags,
  experimental: Option[Experimental],
  adaptersSchemas: AdaptersSchemas,
  blobStorage: BlobStorageClients,
  license: License,
  validation: Validation,
  maxJsonDepth: Int
)

object ConfigFile {
  import AdaptersSchemasDecoders._

  implicit val configFileDecoder: Decoder[ConfigFile] =
    deriveConfiguredDecoder[ConfigFile].emap {
      case c: ConfigFile if c.assetsUpdatePeriod.exists(_.length <= 0L) =>
        "assetsUpdatePeriod in config file cannot be less than 0".asLeft // TODO: use newtype
      case c: ConfigFile =>
        val Outputs(good, pii, bad, incomplete) = c.output
        val piiCleaned = pii match {
          case Some(ki: Output.Kinesis) if ki.streamName.isEmpty => None
          case Some(p: Output.PubSub) if p.topic.isEmpty => None
          case Some(ka: Output.Kafka) if ka.topicName.isEmpty => None
          case _ => pii
        }
        val incompleteCleaned = incomplete match {
          case Some(ki: Output.Kinesis) if ki.streamName.isEmpty => None
          case Some(p: Output.PubSub) if p.topic.isEmpty => None
          case Some(ka: Output.Kafka) if ka.topicName.isEmpty => None
          case _ => incomplete
        }
        c.copy(output = Outputs(good, piiCleaned, bad, incompleteCleaned)).asRight
    }

  /* Defines where to look for default values if they are not in the provided file
   *
   * Command line parameters have the highest priority.
   * Then the provided file.
   * Then the application.conf file.
   * Then the reference.conf file.
   */
  private def configFileFallbacks(in: TSConfig): TSConfig = {
    val defaultConf = ConfigFactory.load("reference.conf")
    ConfigFactory.load(in.withFallback(ConfigFactory.load().withFallback(defaultConf)))
  }

  def parse[F[_]: Sync](raw: EncodedHoconOrPath): EitherT[F, String, ConfigFile] =
    ParsedConfigs.parseEncodedOrPath(raw, configFileFallbacks)

}
