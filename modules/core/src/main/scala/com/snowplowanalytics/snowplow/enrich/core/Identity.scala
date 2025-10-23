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

import cats.effect.{Async, Sync}
import cats.implicits._
import io.circe.{Decoder, Json}
import io.circe.literal._
import io.circe.generic.semiauto._
import org.http4s.client.{Client => Http4sClient}
import org.http4s.{BasicCredentials, EntityDecoder, EntityEncoder, Headers, MediaType, Method, Request}
import org.http4s.headers.{Authorization, `Content-Type`}
import org.http4s.circe._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry.syntax.all._
import retry.RetryPolicies

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonPath.CirceExtractor

object Identity {

  trait Api[F[_]] {
    def concurrency: Int
    def addContexts(events: List[EnrichedEvent]): F[Unit]
  }

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  case class ExtractedIdentifier(
    name: String,
    value: String,
    priority: Int,
    unique: Boolean
  )

  def build[F[_]: Async](config: Config.Identity, httpClient: Http4sClient[F]): Api[F] = {
    val retryPolicy = RetryPolicies.fullJitter[F](config.retries.delay).join(RetryPolicies.limitRetries(config.retries.attempts - 1))
    new Api[F] {
      private def post(inputs: List[BatchIdentifiers]): F[List[BatchIdentity]] = {
        val request = Request[F](
          method = Method.POST,
          uri = config.endpoint / "identities" / "batch",
          headers = Headers(
            Authorization(BasicCredentials(config.username, config.password))
          )
        ).withEntity(inputs)
        httpClient
          .expect[List[BatchIdentity]](request)
          .retryingOnAllErrors(
            policy = retryPolicy,
            onError = (exception, retryDetails) =>
              Logger[F]
                .error(exception)(s"Error calling Identity API (${retryDetails.retriesSoFar} retries)")
          )
      }
      def concurrency: Int =
        (Runtime.getRuntime.availableProcessors * config.concurrencyFactor)
          .setScale(0, BigDecimal.RoundingMode.UP)
          .toInt
      def addContexts(events: List[EnrichedEvent]): F[Unit] =
        for {
          req <- getIdentifiers(events, config.customIdentifiers)
          res <- if (req.nonEmpty) post(req) else Sync[F].pure(List.empty[BatchIdentity])
          _ <- Sync[F].delay(mergeInIdentifiers(events, res))
        } yield ()
    }
  }

  private def extractCustomIdentifiers(
    event: EnrichedEvent,
    config: Config.CustomIdentifiers
  ): List[ExtractedIdentifier] =
    config.identifiers.flatMap { identifierConfig =>
      getFieldValue(event, identifierConfig.field).map { value =>
        ExtractedIdentifier(
          identifierConfig.name,
          value,
          identifierConfig.priority,
          identifierConfig.unique
        )
      }
    }

  private def shouldProcessEvent[F[_]: Sync](
    event: EnrichedEvent,
    config: Config.CustomIdentifiers
  ): F[Boolean] =
    config.filters match {
      case None => true.pure[F]
      case Some(filterConfig) =>
        filterConfig.rules.traverse(shouldKeep[F](event, _)).map { results =>
          filterConfig.logic match {
            case Config.FilterLogic.All => results.forall(identity)
            case Config.FilterLogic.Any => results.exists(identity)
          }
        }
    }

  private def getFieldValue(
    event: EnrichedEvent,
    field: Config.IdentifierField
  ): Option[String] =
    field match {
      case Config.IdentifierField.Atomic(field) =>
        Option(field.get(event)).map(_.toString)
      case ef: Config.IdentifierField.Event =>
        val criterion = SchemaCriterion(ef.vendor, ef.name, "jsonschema", ef.major_version)
        event.unstruct_event
          .filter(data => criterion.matches(data.schema))
          .flatMap(data => ef.path.circeQuery(data.data).headOption.flatMap(_.asString))
      case ef: Config.IdentifierField.Entity =>
        val criterion = SchemaCriterion(ef.vendor, ef.name, "jsonschema", ef.major_version)
        event.contexts
          .filter(entity => criterion.matches(entity.schema))
          .lift(ef.index.getOrElse(0))
          .flatMap(entity => ef.path.circeQuery(entity.data).headOption.flatMap(_.asString))
    }

  private def shouldKeep[F[_]: Sync](event: EnrichedEvent, rule: Config.FilterRule): F[Boolean] =
    getFieldValue(event, rule.field) match {
      case Some(value) =>
        val result = rule.operator match {
          case Config.FilterOperator.In => rule.values.contains(value)
          case Config.FilterOperator.NotIn => !rule.values.contains(value)
        }
        result.pure[F]
      case None =>
        Logger[F]
          .debug(
            s"Filter evaluation failed for event ${event.event_id}: field ${fieldDescription(rule.field)} not found. Event will be filtered out."
          )
          .as(false) // Missing values don't match any rule
    }

  private def fieldDescription(field: Config.IdentifierField): String =
    field match {
      case Config.IdentifierField.Atomic(field) => s"atomic:${field.getName}"
      case ef: Config.IdentifierField.Event => s"event:${ef.vendor}/${ef.name}/${ef.major_version}"
      case ef: Config.IdentifierField.Entity =>
        s"entity:${ef.vendor}/${ef.name}/${ef.major_version}[${ef.index.getOrElse(0)}]"
    }

  private def buildBatchIdentifier(
    event: EnrichedEvent,
    customConfig: Option[Config.CustomIdentifiers]
  ): Option[BatchIdentifiers] = {
    val customIdentifiers = customConfig.flatMap { config =>
      val extracted = extractCustomIdentifiers(event, config)
      if (extracted.nonEmpty)
        Some(extracted.map { id =>
          id.name -> CustomIdentifier(id.value, id.priority, id.unique)
        }.toMap)
      else
        None
    }

    val identifiers = Identifiers(
      domain_userid = Option(event.domain_userid).filter(_.nonEmpty),
      network_userid = Option(event.network_userid).filter(_.nonEmpty),
      user_id = Option(event.user_id).filter(_.nonEmpty),
      custom = customIdentifiers
    )

    if (identifiers.hasAnyIdentifier)
      Some(BatchIdentifiers(event.event_id, identifiers))
    else
      None
  }

  private def getIdentifiers[F[_]: Sync](
    events: List[EnrichedEvent],
    customConfig: Option[Config.CustomIdentifiers]
  ): F[List[BatchIdentifiers]] =
    customConfig match {
      case Some(config) =>
        events
          .filterA(shouldProcessEvent[F](_, config))
          .map(_.flatMap(buildBatchIdentifier(_, customConfig)))
      case None =>
        Sync[F].pure(events.flatMap(buildBatchIdentifier(_, None)))
    }

  private val identitySchemaKey: SchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "identity", "jsonschema", SchemaVer.Full(1, 0, 0))

  private def mergeInIdentifiers(events: List[EnrichedEvent], response: List[BatchIdentity]): Unit = {
    val byEventId = response.map { batchIdentity =>
      val context = json"""{
          "snowplowId": ${batchIdentity.snowplowId},
          "createdAt": ${batchIdentity.createdAt},
          "merged": ${batchIdentity.merged}
        }"""
      val sdj = SelfDescribingData(identitySchemaKey, context)
      batchIdentity.eventId -> sdj
    }.toMap
    events.foreach { e =>
      byEventId.get(e.event_id).foreach { sdj =>
        e.derived_contexts = sdj :: e.derived_contexts
      }
    }
  }

  case class BatchIdentifiers(eventId: String, identifiers: Identifiers)

  case class Identifiers(
    domain_userid: Option[String],
    network_userid: Option[String],
    user_id: Option[String],
    custom: Option[Map[String, CustomIdentifier]]
  ) {
    def hasAnyIdentifier: Boolean =
      domain_userid.isDefined || network_userid.isDefined || user_id.isDefined || custom.exists(_.nonEmpty)
  }

  case class CustomIdentifier(
    value: String,
    priority: Int,
    unique: Boolean
  )

  case class BatchIdentity(
    merged: List[Json],
    createdAt: String,
    eventId: String,
    snowplowId: String
  )

  private val identityMediaType = new MediaType(
    mainType = "application",
    subType = "x.snowplow.v1+json",
    compressible = MediaType.Compressible,
    binary = MediaType.Binary
  )

  private implicit def entityDecoder[F[_]: Async]: EntityDecoder[F, List[BatchIdentity]] = {
    implicit val batchIdentityDecoder: Decoder[BatchIdentity] = deriveDecoder[BatchIdentity]
    jsonOfWithMedia[F, List[BatchIdentity]](identityMediaType)
  }

  private implicit def entityEncoder[F[_]]: EntityEncoder[F, List[BatchIdentifiers]] = {
    implicit val customIdentifierEncoder: io.circe.Encoder[CustomIdentifier] = deriveEncoder[CustomIdentifier]
    implicit val identifiersEncoder: io.circe.Encoder[Identifiers] = deriveEncoder[Identifiers].mapJson(_.deepDropNullValues)
    implicit val batchIdentifiersEncoder: io.circe.Encoder[BatchIdentifiers] = deriveEncoder[BatchIdentifiers]
    jsonEncoderOf[F, List[BatchIdentifiers]]
      .withContentType(`Content-Type`(identityMediaType))
  }

}
