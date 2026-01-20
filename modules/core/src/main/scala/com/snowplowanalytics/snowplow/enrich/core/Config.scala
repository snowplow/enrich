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
package com.snowplowanalytics.snowplow.enrich.core

import java.util.UUID
import java.nio.file.{Files => NioFiles, Path => NioPath}
import java.lang.reflect.{Field => JavaField}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import com.typesafe.config.ConfigFactory

import cats.Id
import cats.data.EitherT
import cats.implicits._

import cats.effect.kernel.{Async, Sync}

import fs2.io.file.Files
import fs2.io.file.{Path => Fs2Path}

import io.circe.{Decoder, Json}
import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.config.syntax._
import io.circe.syntax._

import com.comcast.ip4s.Port

import org.http4s.{ParseFailure, Uri}

import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, Metrics => CommonMetrics, Retrying, Telemetry}
import com.snowplowanalytics.snowplow.runtime.HealthProbe.decoders._

import com.snowplowanalytics.snowplow.enrich.common.adapters._
import com.snowplowanalytics.snowplow.enrich.common.enrichments.AtomicFields
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonPath

case class Config[+Factory, +Source, +Sink, +BlobClients](
  license: AcceptedLicense,
  input: Source,
  output: Config.Output[Sink],
  streams: Factory,
  cpuParallelismFraction: BigDecimal,
  sinkParallelismFraction: BigDecimal,
  monitoring: Config.Monitoring,
  assetsUpdatePeriod: FiniteDuration,
  jsAllowedJavaClasses: Set[String],
  validation: Config.Validation,
  telemetry: Telemetry.Config,
  metadata: Option[Config.Metadata],
  identity: Option[Config.Identity],
  blobClients: BlobClients,
  adaptersSchemas: AdaptersSchemas,
  decompression: Config.Decompression,
  http: Config.Http,
  iglu: Config.Iglu
)

object Config {

  case class Full[+Factory, +Source, +Sink, +BlobClients](
    main: Config[Factory, Source, Sink, BlobClients],
    iglu: ResolverConfig,
    enrichments: Json
  )

  case class SinkMetadata(
    maxRecordSize: Int,
    partitionKey: Option[JavaField],
    attributes: List[JavaField]
  )

  case class SinkWithMetadata[+Sink](
    sink: Sink,
    maxRecordSize: Int,
    partitionKey: Option[JavaField],
    attributes: List[JavaField]
  )

  case class Output[+Sink](
    good: SinkWithMetadata[Sink],
    failed: Option[SinkWithMetadata[Sink]],
    bad: SinkWithMetadata[Sink]
  )

  case class Metrics(
    statsd: Option[CommonMetrics.StatsdConfig]
  )

  case class SentryM[M[_]](
    dsn: M[String],
    tags: Map[String, String]
  )

  type Sentry = SentryM[Id]

  case class HealthProbe(port: Port, unhealthyLatency: FiniteDuration)

  case class Monitoring(
    metrics: Metrics,
    sentry: Option[Sentry],
    healthProbe: HealthProbe
  )

  case class Validation(
    acceptInvalid: Boolean,
    atomicFieldsLimits: AtomicFields,
    maxJsonDepth: Int,
    exitOnJsCompileError: Boolean
  )

  case class MetadataM[M[_]](
    endpoint: M[Uri],
    organizationId: M[UUID],
    pipelineId: M[UUID],
    interval: FiniteDuration,
    maxBodySize: Int
  )

  type Metadata = MetadataM[Id]

  case class IdentityM[M[_]](
    endpoint: M[Uri],
    username: M[String],
    password: M[String],
    concurrencyFactor: BigDecimal,
    retries: Retrying.Config.ForTransient,
    circuitBreaker: Identity.CircuitBreakerConfig,
    identifiers: M[List[Identity.Identifier]],
    filters: Option[Identity.Filtering.Filters]
  )

  object Identity {
    case class CircuitBreakerConfig(
      maxConsecutiveFailures: Int,
      failureRateThreshold: Double,
      failureRateWindow: FiniteDuration,
      minRequestsForRateCheck: Int,
      initialBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      backoffMultiplier: Double
    )

    case class Identifier(
      name: String,
      field: Identifier.Field
    )

    object Identifier {
      sealed trait Field
      case class Atomic(name: JavaField) extends Field
      case class Event(
        vendor: String,
        name: String,
        majorVersion: Int,
        path: JsonPath
      ) extends Field
      case class Entity(
        vendor: String,
        name: String,
        majorVersion: Int,
        index: Option[Int],
        path: JsonPath
      ) extends Field
    }

    object Filtering {
      case class Filters(
        logic: Logic,
        rules: List[Rule]
      )

      sealed trait Logic
      object Logic {
        case object All extends Logic
        case object Any extends Logic
      }

      case class Rule(
        field: Identifier.Field,
        operator: Operator,
        values: List[String]
      )

      sealed trait Operator
      object Operator {
        case object In extends Operator
        case object NotIn extends Operator
      }
    }
  }

  type Identity = IdentityM[Id]

  case class Http(
    client: HttpClient
  )

  case class HttpClient(
    requestTimeout: FiniteDuration
  )

  case class Iglu(
    maxRetry: Int,
    maxWait: FiniteDuration
  )

  /**
   * Configures behaviour of the parser when decompressing
   *
   *  @param maxBytesInBatch: A cutoff used when incrementally adding events to a batch. The batch
   *    is emitted immediately when this cutoff size is reached. This config parameter is needed to
   *    protect the app's memory.  Bear in mind a 1MB compressed message could become HUGE after
   *    decompression.
   *  @param maxBytesSinglePayload: Each individual message should not exceed this size, after
   *    decompression.
   */
  case class Decompression(maxBytesInBatch: Int, maxBytesSinglePayload: Int)

  implicit def decoder[
    Factory: Decoder,
    Source: Decoder,
    Sink: Decoder: OptionalDecoder,
    BlobClients: Decoder
  ]: Decoder[Config[Factory, Source, Sink, BlobClients]] = {
    implicit val http4sUriDecoder: Decoder[Uri] =
      Decoder[String].emap(s => Either.catchOnly[ParseFailure](Uri.unsafeFromString(s)).leftMap(_.toString))
    implicit val configuration = Configuration.default
      .withDiscriminator("type")
      .copy(transformConstructorNames = _.toLowerCase)
    implicit val licenseDecoder =
      AcceptedLicense.decoder(AcceptedLicense.DocumentationLink("https://docs.snowplow.io/limited-use-license-1.1/"))
    implicit val fieldDecoder: Decoder[JavaField] = Decoder[String].emap { name =>
      EnrichedEvent.atomicFieldsByName.get(name) match {
        case Some(field) => Right(field)
        case None => Left(s"$name is not a field of EnrichedEvent")
      }
    }
    implicit val sinkWithMetadataDecoder = for {
      sink <- Decoder[Sink]
      metadata <- deriveConfiguredDecoder[SinkMetadata]
    } yield SinkWithMetadata(sink, metadata.maxRecordSize, metadata.partitionKey, metadata.attributes)
    implicit val optionalSinkWithMetadataDecoder = for {
      sink <- Decoder[Option[Sink]]
      metadata <- deriveConfiguredDecoder[SinkMetadata]
    } yield sink.map(SinkWithMetadata(_, metadata.maxRecordSize, metadata.partitionKey, metadata.attributes))
    implicit val outputDecoder = deriveConfiguredDecoder[Output[Sink]]
    implicit val sentryDecoder = deriveConfiguredDecoder[SentryM[Option]]
      .map[Option[Sentry]] {
        case SentryM(Some(dsn), tags) =>
          Some(SentryM[Id](dsn, tags))
        case SentryM(None, _) =>
          None
      }
    implicit val metricsDecoder = deriveConfiguredDecoder[Metrics]
    implicit val healthProbeDecoder = deriveConfiguredDecoder[HealthProbe]
    implicit val monitoringDecoder = deriveConfiguredDecoder[Monitoring]
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
    implicit val validationDecoder: Decoder[Validation] =
      deriveConfiguredDecoder

    implicit val metadataDecoder = deriveConfiguredDecoder[MetadataM[Option]]
      .emap[Option[Metadata]] {
        case MetadataM(Some(endpoint), Some(organizationId), Some(pipelineId), interval, bodySize) =>
          Right(Some(MetadataM[Id](endpoint, organizationId, pipelineId, interval, bodySize)))
        case MetadataM(None, None, None, _, _) =>
          Right(None)
        case _ =>
          Left("endpoint, organizationId and pipelineId should all be defined to enable metadata reporting")
      }

    implicit val circuitBreakerConfigDecoder: Decoder[Identity.CircuitBreakerConfig] =
      deriveConfiguredDecoder[Identity.CircuitBreakerConfig].emap { config =>
        val errors = List(
          if (config.failureRateThreshold < 0.0 || config.failureRateThreshold > 1.0)
            Some("failureRateThreshold must be between 0.0 and 1.0")
          else None,
          if (config.maxConsecutiveFailures <= 0)
            Some("maxConsecutiveFailures must be positive")
          else None,
          if (config.minRequestsForRateCheck <= 0)
            Some("minRequestsForRateCheck must be positive")
          else None,
          if (config.backoffMultiplier <= 1.0)
            Some("backoffMultiplier must be greater than 1.0")
          else None,
          if (config.initialBackoff.toMillis <= 0)
            Some("initialBackoff must be positive")
          else None,
          if (config.maxBackoff.toMillis <= 0)
            Some("maxBackoff must be positive")
          else None,
          if (config.failureRateWindow.toMillis <= 0)
            Some("failureRateWindow must be positive")
          else None,
          if (config.maxBackoff < config.initialBackoff)
            Some("maxBackoff must be greater than or equal to initialBackoff")
          else None
        ).flatten

        if (errors.isEmpty) Right(config)
        else Left(s"Invalid circuit breaker configuration: ${errors.mkString(", ")}")
      }
    implicit val identityAtomicDecoder = deriveConfiguredDecoder[Identity.Identifier.Atomic]
    implicit val identityEventDecoder = deriveConfiguredDecoder[Identity.Identifier.Event]
    implicit val identityEntityDecoder = deriveConfiguredDecoder[Identity.Identifier.Entity]
    implicit val identityFieldDecoder = deriveConfiguredDecoder[Identity.Identifier.Field]
    implicit val filterLogicDecoder: Decoder[Identity.Filtering.Logic] = Decoder[String].emap {
      case "all" => Right(Identity.Filtering.Logic.All)
      case "any" => Right(Identity.Filtering.Logic.Any)
      case other => Left(s"Unknown filter logic: $other. Expected 'all' or 'any'")
    }
    implicit val filterOperatorDecoder: Decoder[Identity.Filtering.Operator] = Decoder[String].emap {
      case "in" => Right(Identity.Filtering.Operator.In)
      case "nin" => Right(Identity.Filtering.Operator.NotIn)
      case other => Left(s"Unknown filter operator: $other. Expected 'in' or 'nin'")
    }
    implicit val filterRuleDecoder: Decoder[Identity.Filtering.Rule] = deriveConfiguredDecoder[Identity.Filtering.Rule]
    implicit val filtersDecoder: Decoder[Identity.Filtering.Filters] = deriveConfiguredDecoder[Identity.Filtering.Filters]
    implicit val identifierDecoder: Decoder[Identity.Identifier] = deriveConfiguredDecoder[Identity.Identifier]

    implicit val identityDecoder = deriveConfiguredDecoder[IdentityM[Option]]
      .emap[Option[Identity]] {
        case i @ IdentityM(Some(endpoint), Some(username), Some(password), _, _, _, Some(identifiers), _) if identifiers.nonEmpty =>
          Right(Some(IdentityM[Id](endpoint, username, password, i.concurrencyFactor, i.retries, i.circuitBreaker, identifiers, i.filters)))
        case _ =>
          Right(None)
      }

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
    implicit val adaptersSchemasDecoder: Decoder[AdaptersSchemas] =
      deriveConfiguredDecoder[AdaptersSchemas]
    implicit val decompressionDecoder: Decoder[Decompression] =
      deriveConfiguredDecoder[Decompression]
    implicit val httpClientDecoder: Decoder[HttpClient] =
      deriveConfiguredDecoder[HttpClient]
    implicit val httpDecoder: Decoder[Http] =
      deriveConfiguredDecoder[Http]
    implicit val igluDecoder: Decoder[Iglu] =
      deriveConfiguredDecoder[Iglu]

    deriveConfiguredDecoder[Config[Factory, Source, Sink, BlobClients]]
  }

  /** Create the JSON that holds all the enrichments configs */
  def mkEnrichmentsJson[F[_]: Async](dir: NioPath): EitherT[F, String, Json] =
    for {
      paths <- Files
                 .forAsync[F]
                 .list(Fs2Path.fromNioPath(dir))
                 .compile
                 .toList
                 .attemptT
                 .leftMap(e => s"Can't list enrichments config files in ${dir.toAbsolutePath.toString}: $e")
                 .map(_.filter(_.toString.endsWith(".json")))
      jsons <- paths.traverse { path =>
                 readJsonFile(path.toNioPath)
                   .leftMap(error => s"Problem while parsing config file $path: $error")
               }
      enrichmentsJson = SelfDescribingData(
                          SchemaKey("com.snowplowanalytics.snowplow", "enrichments", "jsonschema", SchemaVer.Full(1, 0, 0)),
                          Json.arr(jsons: _*)
                        ).asJson
    } yield enrichmentsJson

  private def readJsonFile[F[_]: Sync](path: NioPath): EitherT[F, String, Json] =
    for {
      str <- EitherT(Sync[F].blocking {
               Either
                 .catchNonFatal(NioFiles.readAllLines(path).asScala.mkString("\n"))
                 .leftMap(e => s"Error reading ${path.toAbsolutePath} file from filesystem: ${e.getMessage}")
             })
      config <- EitherT.fromEither[F](Either.catchNonFatal(ConfigFactory.parseString(str)).leftMap(_.getMessage))
      resolved <- EitherT.fromEither[F](Either.catchNonFatal(config.resolve()).leftMap(_.getMessage))
      json <- EitherT.fromEither[F](resolved.as[Json].leftMap(_.show))
    } yield json
}
