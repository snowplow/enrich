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
package com.snowplowanalytics.snowplow.enrich.streams.common

import java.util.UUID

import scala.concurrent.duration.FiniteDuration

import cats.Id
import cats.syntax.either._

import io.circe.Decoder
import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.config.syntax._

import com.comcast.ip4s.Port

import org.http4s.{ParseFailure, Uri}

import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig

import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, Metrics => CommonMetrics, Telemetry}
import com.snowplowanalytics.snowplow.runtime.HttpClient.{Config => HttpClientConfig}
import com.snowplowanalytics.snowplow.runtime.HealthProbe.decoders._

import com.snowplowanalytics.snowplow.enrich.common.adapters._
import com.snowplowanalytics.snowplow.enrich.common.enrichments.AtomicFields

case class Config[+Source, +Sink](
  license: AcceptedLicense,
  input: Source,
  output: Config.Output[Sink],
  cpuParallelismFraction: BigDecimal,
  monitoring: Config.Monitoring,
  assetsUpdatePeriod: FiniteDuration,
  http: Config.Http,
  validation: Config.Validation,
  telemetry: Telemetry.Config,
  metadata: Option[Config.Metadata],
  adaptersSchemas: AdaptersSchemas
)

object Config {

  case class WithIglu[+Source, +Sink](main: Config[Source, Sink], iglu: ResolverConfig)

  case class SinkMetadata(
    maxRecordSize: Int,
    partitionKey: Option[String],
    attributes: Option[Set[String]]
  )

  case class SinkWithMetadata[+Sink](
    sink: Sink,
    maxRecordSize: Int,
    partitionKey: Option[String],
    attributes: Option[Set[String]]
  )

  case class Output[+Sink](
    enriched: SinkWithMetadata[Sink],
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

  case class Http(client: HttpClientConfig)

  case class Validation(
    acceptInvalid: Boolean,
    atomicFieldsLimits: AtomicFields,
    maxJsonDepth: Int
  )

  case class MetadataM[M[_]](
    endpoint: M[Uri],
    organizationId: M[UUID],
    pipelineId: M[UUID],
    interval: FiniteDuration,
    maxBodySize: Int
  )

  type Metadata = MetadataM[Id]

  implicit val http4sUriDecoder: Decoder[Uri] =
    Decoder[String].emap(s => Either.catchOnly[ParseFailure](Uri.unsafeFromString(s)).leftMap(_.toString))

  implicit def decoder[Source: Decoder, Sink: Decoder: OptionalDecoder]: Decoder[Config[Source, Sink]] = {
    implicit val configuration = Configuration.default.withDiscriminator("type")
    implicit val licenseDecoder =
      AcceptedLicense.decoder(AcceptedLicense.DocumentationLink("https://docs.snowplow.io/limited-use-license-1.1/"))
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
    implicit val httpDecoder = deriveConfiguredDecoder[Http]
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

    deriveConfiguredDecoder[Config[Source, Sink]]
  }
}
