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

import java.nio.file.{InvalidPathException, Path => JPath, Paths}
import java.time.Instant
import java.net.URI
import java.util.UUID

import cats.syntax.either._

import scala.concurrent.duration.{Duration, FiniteDuration}
import _root_.io.circe.{Decoder, DecodingFailure, HCursor}
import _root_.io.circe.generic.extras.semiauto._
import _root_.io.circe.config.syntax._

import org.http4s.{ParseFailure, Uri}

import fs2.io.file.Path

import com.snowplowanalytics.snowplow.enrich.common.EtlPipeline.{FeatureFlags => CommonFeatureFlags}
import com.snowplowanalytics.snowplow.enrich.common.adapters._
import com.snowplowanalytics.snowplow.enrich.common.enrichments.AtomicFields

object io {

  val putRecordsMaxRecords = 500

  implicit val jPathDecoder: Decoder[JPath] =
    Decoder[String].emap { s =>
      Either.catchOnly[InvalidPathException](Paths.get(s)).leftMap(_.getMessage)
    }

  implicit val pathDecoder: Decoder[Path] =
    Decoder[String].emap { s =>
      Either.catchNonFatal(Path(s)).leftMap(_.getMessage)
    }

  implicit val javaUriDecoder: Decoder[URI] =
    Decoder[String].emap { s =>
      Either.catchOnly[IllegalArgumentException](URI.create(s)).leftMap(err => s"error while parsing URI $s: ${err.getMessage}")
    }

  implicit val http4sUriDecoder: Decoder[Uri] =
    Decoder[String].emap(s => Either.catchOnly[ParseFailure](Uri.unsafeFromString(s)).leftMap(_.toString))

  case class BackoffPolicy(
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    maxRetries: Option[Int]
  )
  object BackoffPolicy {
    implicit def backoffPolicyDecoder: Decoder[BackoffPolicy] =
      deriveConfiguredDecoder[BackoffPolicy]
  }

  sealed trait RetryCheckpointing {
    val checkpointBackoff: BackoffPolicy
  }

  sealed trait Input

  object Input {

    case class Kafka private (
      topicName: String,
      bootstrapServers: String,
      consumerConf: Map[String, String]
    ) extends Input

    case class Nsq private (
      topic: String,
      channel: String,
      lookupHost: String,
      lookupPort: Int,
      maxBufferQueueSize: Int,
      checkpointBackoff: BackoffPolicy
    ) extends Input
        with RetryCheckpointing

    case class PubSub private (
      subscription: String,
      parallelPullCount: Int,
      maxQueueSize: Int,
      maxRequestBytes: Int,
      maxAckExtensionPeriod: FiniteDuration,
      gcpUserAgent: GcpUserAgent
    ) extends Input {
      val (project, name) =
        subscription.split("/").toList match {
          case List("projects", project, "subscriptions", name) =>
            (project, name)
          case _ =>
            throw new IllegalArgumentException(s"Subscription format $subscription invalid")
        }
    }
    case class FileSystem(dir: Path) extends Input
    case class Kinesis private (
      appName: String,
      streamName: String,
      region: Option[String],
      initialPosition: Kinesis.InitPosition,
      retrievalMode: Kinesis.Retrieval,
      bufferSize: Int,
      checkpointBackoff: BackoffPolicy,
      customEndpoint: Option[URI],
      dynamodbCustomEndpoint: Option[URI],
      cloudwatchCustomEndpoint: Option[URI]
    ) extends Input
        with RetryCheckpointing

    object Kinesis {
      sealed trait InitPosition
      object InitPosition {
        case object Latest extends InitPosition
        case object TrimHorizon extends InitPosition
        case class AtTimestamp(timestamp: Instant) extends InitPosition

        case class InitPositionRaw(`type`: String, timestamp: Option[Instant])
        implicit val initPositionRawDecoder: Decoder[InitPositionRaw] = deriveConfiguredDecoder[InitPositionRaw]

        implicit val initPositionDecoder: Decoder[InitPosition] =
          Decoder.instance { cur =>
            for {
              rawParsed <- cur.as[InitPositionRaw].map(raw => raw.copy(`type` = raw.`type`.toUpperCase))
              initPosition <- rawParsed match {
                                case InitPositionRaw("TRIM_HORIZON", _) =>
                                  TrimHorizon.asRight
                                case InitPositionRaw("LATEST", _) =>
                                  Latest.asRight
                                case InitPositionRaw("AT_TIMESTAMP", Some(timestamp)) =>
                                  AtTimestamp(timestamp).asRight
                                case other =>
                                  DecodingFailure(
                                    s"Initial position $other is not supported. Possible types are TRIM_HORIZON, LATEST and AT_TIMESTAMP (must provide timestamp field)",
                                    cur.history
                                  ).asLeft
                              }
            } yield initPosition
          }
      }

      sealed trait Retrieval
      object Retrieval {
        case class Polling(maxRecords: Int) extends Retrieval
        case object FanOut extends Retrieval

        case class RetrievalRaw(`type`: String, maxRecords: Option[Int])
        implicit val retrievalRawDecoder: Decoder[RetrievalRaw] = deriveConfiguredDecoder[RetrievalRaw]

        implicit val retrievalDecoder: Decoder[Retrieval] =
          Decoder.instance { cur =>
            for {
              rawParsed <- cur.as[RetrievalRaw].map(raw => raw.copy(`type` = raw.`type`.toUpperCase))
              retrieval <- rawParsed match {
                             case RetrievalRaw("POLLING", Some(maxRecords)) =>
                               Polling(maxRecords).asRight
                             case RetrievalRaw("FANOUT", _) =>
                               FanOut.asRight
                             case other =>
                               DecodingFailure(
                                 s"Retrieval mode $other is not supported. Possible types are FanOut and Polling (must provide maxRecords field)",
                                 cur.history
                               ).asLeft
                           }
            } yield retrieval
          }
      }

      implicit val kinesisDecoder: Decoder[Kinesis] = deriveConfiguredDecoder[Kinesis]
    }

    implicit val inputDecoder: Decoder[Input] =
      deriveConfiguredDecoder[Input]
        .emap {
          case s @ PubSub(sub, _, _, _, _, _) =>
            sub.split("/").toList match {
              case List("projects", _, "subscriptions", _) =>
                s.asRight
              case _ =>
                s"Subscription must conform projects/project-name/subscriptions/subscription-name format, $s given".asLeft
            }
          case Kafka(topicName, bootstrapServers, _) if topicName.isEmpty ^ bootstrapServers.isEmpty =>
            "Both topicName and bootstrapServers have to be set".asLeft
          case other => other.asRight
        }
        .emap {
          case PubSub(_, p, _, _, _, _) if p <= 0 =>
            "PubSub parallelPullCount must be > 0".asLeft
          case PubSub(_, _, m, _, _, _) if m <= 0 =>
            "PubSub maxQueueSize must be > 0".asLeft
          case PubSub(_, _, _, m, _, _) if m <= 0 =>
            "PubSub maxRequestBytes must be > 0".asLeft
          case PubSub(_, _, _, _, m, _) if m < Duration.Zero =>
            "PubSub maxAckExtensionPeriod must be >= 0".asLeft
          case other =>
            other.asRight
        }
  }

  case class Outputs(
    good: Output,
    pii: Option[Output],
    bad: Output,
    incomplete: Option[Output]
  )
  object Outputs {
    implicit val outputsDecoder: Decoder[Outputs] = deriveConfiguredDecoder[Outputs]
  }

  sealed trait Output

  object Output {

    case class Kafka private (
      topicName: String,
      bootstrapServers: String,
      partitionKey: String,
      headers: Set[String],
      producerConf: Map[String, String]
    ) extends Output

    case class Nsq private (
      topic: String,
      nsqdHost: String,
      nsqdPort: Int,
      backoffPolicy: BackoffPolicy
    ) extends Output

    case class PubSub private (
      topic: String,
      attributes: Option[Set[String]],
      delayThreshold: FiniteDuration,
      maxBatchSize: Long,
      maxBatchBytes: Long,
      gcpUserAgent: GcpUserAgent
    ) extends Output {
      val (project, name) =
        topic.split("/").toList match {
          case _ if topic.isEmpty =>
            ("", "")
          case List("projects", project, "topics", name) =>
            (project, name)
          case _ =>
            throw new IllegalArgumentException(s"Topic format $topic invalid")
        }
    }
    case class FileSystem(file: JPath, maxBytes: Option[Long]) extends Output
    case class Kinesis(
      streamName: String,
      region: Option[String],
      partitionKey: Option[String],
      backoffPolicy: BackoffPolicy,
      throttledBackoffPolicy: BackoffPolicy,
      recordLimit: Int,
      byteLimit: Int,
      customEndpoint: Option[URI]
    ) extends Output

    implicit val outputDecoder: Decoder[Output] =
      deriveConfiguredDecoder[Output]
        .emap {
          case Kafka(topicName, bootstrapServers, _, _, _) if topicName.isEmpty ^ bootstrapServers.isEmpty =>
            "Both topicName and bootstrapServers have to be set".asLeft
          case s @ PubSub(top, _, _, _, _, _) if top.nonEmpty =>
            top.split("/").toList match {
              case List("projects", _, "topics", _) =>
                s.asRight
              case _ =>
                s"Topic must conform projects/project-name/topics/topic-name format, $top given".asLeft
            }
          case k: Kinesis if k.streamName.isEmpty && k.region.nonEmpty =>
            "streamName needs to be set".asLeft
          case other => other.asRight
        }
        .emap {
          case Kafka(_, _, pk, _, _) if pk.nonEmpty && !ParsedConfigs.isValidPartitionKey(pk) =>
            s"Kafka partition key [$pk] is invalid".asLeft
          case ka: Kafka if ka.headers.nonEmpty =>
            val invalidAttrs = ParsedConfigs.filterInvalidAttributes(ka.headers)
            if (invalidAttrs.nonEmpty) s"Kafka headers [${invalidAttrs.mkString(",")}] are invalid".asLeft
            else ka.asRight
          case p: PubSub if p.delayThreshold < Duration.Zero =>
            "PubSub delay threshold cannot be less than 0".asLeft
          case p: PubSub if p.maxBatchSize < 0 =>
            "PubSub max batch size cannot be less than 0".asLeft
          case p: PubSub if p.maxBatchBytes < 0 =>
            "PubSub max batch bytes cannot be less than 0".asLeft
          case k: Kinesis if k.recordLimit > putRecordsMaxRecords =>
            s"recordLimit can't be > $putRecordsMaxRecords".asLeft
          case other =>
            other.asRight
        }
  }

  final case class Concurrency(enrich: Int, sink: Int)

  object Concurrency {
    implicit val concurrencyDecoder: Decoder[Concurrency] =
      deriveConfiguredDecoder[Concurrency]
  }

  final case class Http(client: Http.Client)
  object Http {
    case class Client(
      maxConnectionsPerServer: Int
    )
    object Client {
      implicit val clientDecoder: Decoder[Client] =
        deriveConfiguredDecoder[Client]
    }
    implicit val httpDecoder: Decoder[Http] =
      deriveConfiguredDecoder[Http]
  }

  final case class MetricsReporters(
    statsd: Option[MetricsReporters.StatsD],
    stdout: Option[MetricsReporters.Stdout],
    cloudwatch: Boolean
  )

  object MetricsReporters {
    final case class Stdout(period: FiniteDuration, prefix: Option[String])
    final case class StatsD(
      hostname: String,
      port: Int,
      tags: Map[String, String],
      period: FiniteDuration,
      prefix: Option[String]
    )

    implicit val stdoutDecoder: Decoder[Stdout] =
      deriveConfiguredDecoder[Stdout].emap { stdout =>
        if (stdout.period < Duration.Zero)
          "metrics report period in config file cannot be less than 0".asLeft
        else
          stdout.asRight
      }

    implicit val statsDecoder: Decoder[StatsD] =
      deriveConfiguredDecoder[StatsD].emap { statsd =>
        if (statsd.period < Duration.Zero)
          "metrics report period in config file cannot be less than 0".asLeft
        else
          statsd.asRight
      }

    implicit val metricsReportersDecoder: Decoder[MetricsReporters] =
      deriveConfiguredDecoder[MetricsReporters]

    def normalizeMetric(prefix: Option[String], metric: String): String =
      s"${prefix.getOrElse(DefaultPrefix).stripSuffix(".")}.$metric".stripPrefix(".")

    val DefaultPrefix = "snowplow.enrich"
  }

  case class Monitoring(sentry: Option[Sentry], metrics: MetricsReporters)

  object Monitoring {
    implicit val monitoringDecoder: Decoder[Monitoring] =
      deriveConfiguredDecoder[Monitoring]
  }

  case class RemoteAdapterConfig(
    vendor: String,
    version: String,
    url: String
  )

  object RemoteAdapterConfig {
    implicit val remoteAdapterConfigDecoder: Decoder[RemoteAdapterConfig] =
      deriveConfiguredDecoder[RemoteAdapterConfig]
  }

  case class RemoteAdapterConfigs(
    connectionTimeout: FiniteDuration,
    readTimeout: FiniteDuration,
    maxConnections: Int,
    configs: List[RemoteAdapterConfig]
  )

  object RemoteAdapterConfigs {
    implicit val remoteAdapterConfigsDecoder: Decoder[RemoteAdapterConfigs] =
      deriveConfiguredDecoder[RemoteAdapterConfigs]
  }

  case class Telemetry(
    disable: Boolean,
    interval: FiniteDuration,
    method: String,
    collectorUri: String,
    collectorPort: Int,
    secure: Boolean,
    userProvidedId: Option[String],
    autoGeneratedId: Option[String],
    instanceId: Option[String],
    moduleName: Option[String],
    moduleVersion: Option[String]
  )

  object Telemetry {
    implicit val telemetryDecoder: Decoder[Telemetry] =
      deriveConfiguredDecoder[Telemetry]
  }

  case class Metadata(
    endpoint: Uri,
    interval: FiniteDuration,
    organizationId: UUID,
    pipelineId: UUID,
    maxBodySize: Int
  )
  object Metadata {
    // The app should define a default max body size.
    // This can't be put inside the usual application.conf because metadata reporting is optional.
    val defaultMaxBodySize = 149000

    implicit val metadataDecoder: Decoder[Metadata] = new Decoder[Metadata] {
      def apply(c: HCursor): Decoder.Result[Metadata] =
        for {
          endpoint <- c.downField("endpoint").as[Uri]
          interval <- c.downField("interval").as[FiniteDuration]
          organizationId <- c.downField("organizationId").as[UUID]
          pipelineId <- c.downField("pipelineId").as[UUID]
          maxBodySize <- c.downField("maxBodySize").as[Int].recover {
                           case failure if failure.reason == DecodingFailure.Reason.MissingField =>
                             defaultMaxBodySize
                         }
        } yield Metadata(endpoint, interval, organizationId, pipelineId, maxBodySize)
    }
  }

  case class Experimental(metadata: Option[Metadata])
  object Experimental {
    implicit val experimentalDecoder: Decoder[Experimental] =
      deriveConfiguredDecoder[Experimental]
  }

  case class FeatureFlags(
    acceptInvalid: Boolean,
    legacyEnrichmentOrder: Boolean,
    tryBase64Decoding: Boolean
  )

  object FeatureFlags {
    implicit val featureFlagsDecoder: Decoder[FeatureFlags] =
      deriveConfiguredDecoder[FeatureFlags]

    // Currently the FS2 feature flags exactly match the common feature flags, but it might not always be like this.
    def toCommon(ff: FeatureFlags): CommonFeatureFlags =
      CommonFeatureFlags(
        acceptInvalid = ff.acceptInvalid,
        legacyEnrichmentOrder = ff.legacyEnrichmentOrder
      )
  }

  case class GcpUserAgent(productName: String)

  object GcpUserAgent {
    implicit val gcpUserAgentDecoder: Decoder[GcpUserAgent] =
      deriveConfiguredDecoder[GcpUserAgent]
  }

  case class BlobStorageClients(
    gcs: Boolean,
    s3: Boolean,
    azureStorage: Option[BlobStorageClients.AzureStorage]
  )
  object BlobStorageClients {

    case class AzureStorage(accounts: List[AzureStorage.Account])
    object AzureStorage {

      final case class Account(name: String, auth: Option[Account.Auth])
      object Account {

        sealed trait Auth
        object Auth {
          final case object DefaultCredentialsChain extends Auth
          final case class SasToken(value: String) extends Auth
        }
      }
    }

    implicit val sasTokenDecoder: Decoder[AzureStorage.Account.Auth.SasToken] =
      deriveConfiguredDecoder

    implicit val accountAuthDecoder: Decoder[AzureStorage.Account.Auth] =
      Decoder.instance { cursor =>
        val typeCur = cursor.downField("type")
        typeCur.as[String].map(_.toLowerCase) match {
          case Right("default") =>
            Right(AzureStorage.Account.Auth.DefaultCredentialsChain)
          case Right("sas") =>
            cursor.as[AzureStorage.Account.Auth.SasToken]
          case Right(other) =>
            Left(
              DecodingFailure(s"Storage account authentication type '$other' is not supported yet. Supported types: 'default', 'sas'",
                              typeCur.history
              )
            )
          case Left(other) =>
            Left(other)
        }
      }

    implicit val storageAccountDecoder: Decoder[AzureStorage.Account] =
      deriveConfiguredDecoder
    implicit val azureStorageDecoder: Decoder[AzureStorage] =
      deriveConfiguredDecoder[AzureStorage]
    implicit val blobStorageClientDecoder: Decoder[BlobStorageClients] =
      deriveConfiguredDecoder[BlobStorageClients]
  }

  object AdaptersSchemasDecoders {
    implicit val adaptersSchemasDecoder: Decoder[AdaptersSchemas] =
      deriveConfiguredDecoder[AdaptersSchemas]
    implicit val callrailSchemasDecoder: Decoder[CallrailSchemas] =
      deriveConfiguredDecoder[CallrailSchemas]
    implicit val cloudfrontAccessLogSchemasDecoder: Decoder[CloudfrontAccessLogSchemas] =
      deriveConfiguredDecoder[CloudfrontAccessLogSchemas]
    implicit val googleAnalyticsSchemasDecoder: Decoder[GoogleAnalyticsSchemas] =
      deriveConfiguredDecoder[GoogleAnalyticsSchemas]
    implicit val hubspotSchemasDecoder: Decoder[HubspotSchemas] =
      deriveConfiguredDecoder[HubspotSchemas]
    implicit val mailchimpSchemasDecoder: Decoder[MailchimpSchemas] =
      deriveConfiguredDecoder[MailchimpSchemas]
    implicit val mailgunSchemasDecoder: Decoder[MailgunSchemas] =
      deriveConfiguredDecoder[MailgunSchemas]
    implicit val mandrillSchemasDecoder: Decoder[MandrillSchemas] =
      deriveConfiguredDecoder[MandrillSchemas]
    implicit val marketoSchemasDecoder: Decoder[MarketoSchemas] =
      deriveConfiguredDecoder[MarketoSchemas]
    implicit val olarkSchemasDecoder: Decoder[OlarkSchemas] =
      deriveConfiguredDecoder[OlarkSchemas]
    implicit val pagerdutySchemasDecoder: Decoder[PagerdutySchemas] =
      deriveConfiguredDecoder[PagerdutySchemas]
    implicit val pingdomSchemasDecoder: Decoder[PingdomSchemas] =
      deriveConfiguredDecoder[PingdomSchemas]
    implicit val sendgridSchemasDecoder: Decoder[SendgridSchemas] =
      deriveConfiguredDecoder[SendgridSchemas]
    implicit val statusgatorSchemasDecoder: Decoder[StatusGatorSchemas] =
      deriveConfiguredDecoder[StatusGatorSchemas]
    implicit val unbounceSchemasDecoder: Decoder[UnbounceSchemas] =
      deriveConfiguredDecoder[UnbounceSchemas]
    implicit val urbanAirshipSchemasDecoder: Decoder[UrbanAirshipSchemas] =
      deriveConfiguredDecoder[UrbanAirshipSchemas]
    implicit val veroSchemasDecoder: Decoder[VeroSchemas] =
      deriveConfiguredDecoder[VeroSchemas]
  }

  sealed trait Cloud

  object Cloud {
    case object Aws extends Cloud
    case object Gcp extends Cloud
    case object Azure extends Cloud
  }

  case class License(
    accept: Boolean
  )

  object License {
    implicit val licenseDecoder: Decoder[License] = {
      val truthy = Set("true", "yes", "on", "1")
      Decoder
        .forProduct1("accept")((s: String) => License(truthy(s.toLowerCase())))
        .or(Decoder.forProduct1("accept")((b: Boolean) => License(b)))
    }
  }

  case class Validation(atomicFieldsLimits: AtomicFields)

  implicit val validationDecoder: Decoder[Validation] =
    deriveConfiguredDecoder

  implicit val atomicFieldsDecoder: Decoder[AtomicFields] = Decoder[Map[String, Int]].emap { fieldsLimits =>
    val configuredFields = fieldsLimits.keys.toList
    val supportedFields = AtomicFields.supportedFields.map(_.name)
    val unsupportedFields = configuredFields.diff(supportedFields)

    if (unsupportedFields.nonEmpty)
      Left(s"""
        |Configured atomic fields: ${unsupportedFields.mkString("[", ",", "]")} are not supported.
        |Supported fields: ${supportedFields.mkString("[", ",", "]")}""".stripMargin)
    else
      Right(AtomicFields.from(fieldsLimits))
  }
}
