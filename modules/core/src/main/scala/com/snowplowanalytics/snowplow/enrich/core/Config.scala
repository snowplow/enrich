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
import java.lang.reflect.Field

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

import io.gatling.jsonpath.JsonPath

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
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonPath.compileQuery

case class Config[+Factory, +Source, +Sink, +BlobClients](
  license: AcceptedLicense,
  input: Source,
  output: Config.Output[Sink],
  streams: Factory,
  cpuParallelismFraction: BigDecimal,
  sinkParallelismFraction: BigDecimal,
  monitoring: Config.Monitoring,
  assetsUpdatePeriod: FiniteDuration,
  validation: Config.Validation,
  telemetry: Telemetry.Config,
  metadata: Option[Config.Metadata],
  identity: Option[Config.Identity],
  blobClients: BlobClients,
  adaptersSchemas: AdaptersSchemas,
  decompression: Config.Decompression
)

object Config {

  case class Full[+Factory, +Source, +Sink, +BlobClients](
    main: Config[Factory, Source, Sink, BlobClients],
    iglu: ResolverConfig,
    enrichments: Json
  )

  case class SinkMetadata(
    maxRecordSize: Int,
    partitionKey: Option[Field],
    attributes: List[Field]
  )

  case class SinkWithMetadata[+Sink](
    sink: Sink,
    maxRecordSize: Int,
    partitionKey: Option[Field],
    attributes: List[Field]
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
    customIdentifiers: Option[CustomIdentifiers]
  )

  type Identity = IdentityM[Id]

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

  case class CustomIdentifiers(
    identifiers: List[Identifier],
    filters: Option[Filter]
  )

  case class Identifier(
    name: String,
    field: IdentifierField,
    priority: Int,
    unique: Boolean
  )

  sealed trait IdentifierField

  object IdentifierField {
    // Raw types for config deserialization (contain path as String)
    case class EventRaw(
      vendor: String,
      name: String,
      major_version: Int,
      path: String
    )

    case class EntityRaw(
      vendor: String,
      name: String,
      major_version: Int,
      index: Option[Int],
      path: String
    )

    // Compiled types for runtime (contain compiled JsonPath)
    case class Atomic(
      name: Field
    ) extends IdentifierField

    case class Event(
      vendor: String,
      name: String,
      major_version: Int,
      path: JsonPath
    ) extends IdentifierField {
      // Override equals/hashCode to exclude JsonPath (which doesn't implement equals)
      // Only compare the structural fields
      // Used in unit tests
      override def equals(obj: Any): Boolean =
        obj match {
          case that: Event =>
            this.vendor == that.vendor &&
              this.name == that.name &&
              this.major_version == that.major_version
          case _ => false
        }
      override def hashCode(): Int = (vendor, name, major_version).##
    }

    object Event {
      def fromRaw(raw: EventRaw): Either[String, Event] =
        compileQuery(raw.path).map(compiled => Event(raw.vendor, raw.name, raw.major_version, compiled))
    }

    case class Entity(
      vendor: String,
      name: String,
      major_version: Int,
      index: Option[Int],
      path: JsonPath
    ) extends IdentifierField {
      // Override equals/hashCode to exclude JsonPath (which doesn't implement equals)
      // Only compare the structural fields
      // Used in unit tests
      override def equals(obj: Any): Boolean =
        obj match {
          case that: Entity =>
            this.vendor == that.vendor &&
              this.name == that.name &&
              this.major_version == that.major_version &&
              this.index == that.index
          case _ => false
        }
      override def hashCode(): Int = (vendor, name, major_version, index).##
    }

    object Entity {
      def fromRaw(raw: EntityRaw): Either[String, Entity] =
        compileQuery(raw.path).map(compiled => Entity(raw.vendor, raw.name, raw.major_version, raw.index, compiled))
    }
  }

  sealed trait FilterLogic
  object FilterLogic {
    case object All extends FilterLogic
    case object Any extends FilterLogic
  }

  sealed trait FilterOperator
  object FilterOperator {
    case object In extends FilterOperator
    case object NotIn extends FilterOperator
  }

  case class Filter(
    logic: FilterLogic,
    rules: List[FilterRule]
  )

  case class FilterRule(
    field: IdentifierField,
    operator: FilterOperator,
    values: List[String]
  )

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
    implicit val fieldDecoder: Decoder[Field] = Decoder[String].emap { name =>
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

    implicit val atomicDecoder: Decoder[IdentifierField.Atomic] =
      Decoder[Field].at("name").map(IdentifierField.Atomic(_))
    implicit val eventRawDecoder: Decoder[IdentifierField.EventRaw] =
      deriveConfiguredDecoder[IdentifierField.EventRaw]
    implicit val eventDecoder: Decoder[IdentifierField.Event] =
      eventRawDecoder.emap { raw =>
        IdentifierField.Event.fromRaw(raw).leftMap(err => s"Invalid JSONPath '${raw.path}': $err")
      }
    implicit val entityRawDecoder: Decoder[IdentifierField.EntityRaw] =
      deriveConfiguredDecoder[IdentifierField.EntityRaw]
    implicit val entityDecoder: Decoder[IdentifierField.Entity] =
      entityRawDecoder.emap { raw =>
        IdentifierField.Entity.fromRaw(raw).leftMap(err => s"Invalid JSONPath '${raw.path}': $err")
      }

    implicit val identifierFieldDecoder: Decoder[IdentifierField] =
      deriveConfiguredDecoder[IdentifierField]
    implicit val filterLogicDecoder: Decoder[FilterLogic] = Decoder[String].emap {
      case "all" => Right(FilterLogic.All)
      case "any" => Right(FilterLogic.Any)
      case other => Left(s"Unknown filter logic: $other. Expected 'all' or 'any'")
    }
    implicit val filterOperatorDecoder: Decoder[FilterOperator] = Decoder[String].emap {
      case "in" => Right(FilterOperator.In)
      case "nin" => Right(FilterOperator.NotIn)
      case other => Left(s"Unknown filter operator: $other. Expected 'in' or 'nin'")
    }
    implicit val filterRuleDecoder: Decoder[FilterRule] = deriveConfiguredDecoder[FilterRule]
    implicit val filterDecoder: Decoder[Filter] = deriveConfiguredDecoder[Filter]
    implicit val identifierDecoder: Decoder[Identifier] = deriveConfiguredDecoder[Identifier]
    implicit val customIdentifiersDecoder: Decoder[CustomIdentifiers] =
      deriveConfiguredDecoder[CustomIdentifiers].emap { config =>
        // Validate duplicate identifier names
        val names = config.identifiers.map(_.name)
        val duplicates = names.diff(names.distinct).distinct
        Either.cond(
          duplicates.isEmpty,
          config,
          s"Duplicate identifier names: ${duplicates.mkString(", ")}"
        )
      }

    implicit val identityDecoder = deriveConfiguredDecoder[IdentityM[Option]]
      .emap[Option[Identity]] {
        case i @ IdentityM(Some(endpoint), Some(username), Some(password), _, _, _) =>
          Right(Some(IdentityM[Id](endpoint, username, password, i.concurrencyFactor, i.retries, i.customIdentifiers)))
        case IdentityM(None, None, None, _, _, _) =>
          Right(None)
        case _ =>
          Left("endpoint, username and password must all be defined to enable the Identity context")
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
