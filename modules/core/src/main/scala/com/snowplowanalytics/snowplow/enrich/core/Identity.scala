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
import io.circe.Decoder
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

object Identity {

  trait Api[F[_]] {
    def concurrency: Int
    def addIdentityContexts(events: List[EnrichedEvent]): F[Unit]
  }

  case class EventIdentifiers(eventId: String, identifiers: Map[String, String])

  // Corresponds to BatchIdentity in identity API
  case class Identity(
    createdAt: String,
    eventId: String,
    snowplowId: String
  )

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def build[F[_]: Async](config: Config.Identity, httpClient: Http4sClient[F]): Api[F] = {
    val retryPolicy = RetryPolicies.fullJitter[F](config.retries.delay).join(RetryPolicies.limitRetries(config.retries.attempts - 1))

    new Api[F] {
      private def post(events: List[EventIdentifiers]): F[List[Identity]] = {
        val request = Request[F](
          method = Method.POST,
          uri = config.endpoint / "identities" / "batch",
          headers = Headers(
            Authorization(BasicCredentials(config.username, config.password))
          )
        ).withEntity(events)

        httpClient
          .expect[List[Identity]](request)
          .retryingOnAllErrors(
            policy = retryPolicy,
            onError = (exception, retryDetails) =>
              Logger[F]
                .error(exception)(s"Error calling Identity API (${retryDetails.retriesSoFar} retries)")
          )
      }

      override def concurrency: Int =
        (Runtime.getRuntime.availableProcessors * config.concurrencyFactor)
          .setScale(0, BigDecimal.RoundingMode.UP)
          .toInt

      override def addIdentityContexts(events: List[EnrichedEvent]): F[Unit] =
        for {
          filtered <- Sync[F].delay(filter(events, config.filters))
          eventsIdentifiers <- Sync[F].delay(filtered.flatMap(extractEventIdentifiers(_, config.identifiers)))
          _ <- if (eventsIdentifiers.nonEmpty)
                 post(eventsIdentifiers).flatMap(identities => Sync[F].delay(merge(events, identities)))
               else Sync[F].unit
        } yield ()
    }
  }

  private def filter(
    events: List[EnrichedEvent],
    maybeFilters: Option[Config.Identity.Filtering.Filters]
  ): List[EnrichedEvent] =
    maybeFilters.fold(events)(filters => events.filter(shouldKeepEvent(_, filters)))

  private def shouldKeepEvent(
    event: EnrichedEvent,
    filters: Config.Identity.Filtering.Filters
  ): Boolean = {
    val matches = filters.rules.map(eventMatchesRule(event, _))
    filters.logic match {
      case Config.Identity.Filtering.Logic.All => matches.forall(_ === true)
      case Config.Identity.Filtering.Logic.Any => matches.exists(_ === true)
    }
  }

  private def eventMatchesRule(event: EnrichedEvent, rule: Config.Identity.Filtering.Rule): Boolean =
    getFieldValue(event, rule.field) match {
      case Some(value) =>
        rule.operator match {
          case Config.Identity.Filtering.Operator.In => rule.values.contains(value)
          case Config.Identity.Filtering.Operator.NotIn => !rule.values.contains(value)
        }
      case None =>
        false // Missing values don't match any rule
    }

  private def getFieldValue(
    event: EnrichedEvent,
    field: Config.Identity.Identifier.Field
  ): Option[String] =
    field match {
      case Config.Identity.Identifier.Atomic(field) =>
        Option(field.get(event)).map(_.toString)
      case Config.Identity.Identifier.Event(vendor, name, major, path) =>
        val criterion = SchemaCriterion(vendor, name, "jsonschema", major)
        event.unstruct_event
          .filter(data => criterion.matches(data.schema))
          .flatMap(data => path.circeQuery(data.data).headOption.flatMap(_.asString))
      case Config.Identity.Identifier.Entity(vendor, name, major, index, path) =>
        val criterion = SchemaCriterion(vendor, name, "jsonschema", major)
        event.contexts
          .filter(entity => criterion.matches(entity.schema))
          .lift(index.getOrElse(0))
          .flatMap(entity => path.circeQuery(entity.data).headOption.flatMap(_.asString))
    }

  private def extractEventIdentifiers(
    event: EnrichedEvent,
    fields: List[Config.Identity.Identifier]
  ): Option[EventIdentifiers] = {
    val values = fields.flatMap { identifier =>
      getFieldValue(event, identifier.field).map(identifier.name -> _)
    }.toMap

    if (values.nonEmpty)
      Some(
        EventIdentifiers(
          event.event_id,
          values
        )
      )
    else
      None
  }

  private def merge(events: List[EnrichedEvent], identities: List[Identity]): Unit = {
    val byEventId = identities.map { identity =>
      val context = json"""{
          "snowplowId": ${identity.snowplowId},
          "createdAt": ${identity.createdAt}
        }"""
      val sdj = SelfDescribingData(identitySchemaKey, context)
      identity.eventId -> sdj
    }.toMap

    events.foreach { e =>
      byEventId.get(e.event_id).foreach { sdj =>
        e.derived_contexts = sdj :: e.derived_contexts
      }
    }
  }

  private val identitySchemaKey: SchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "identity", "jsonschema", SchemaVer.Full(1, 0, 0))

  private val identityMediaType = new MediaType(
    mainType = "application",
    subType = "x.snowplow.v1+json",
    compressible = MediaType.Compressible,
    binary = MediaType.Binary
  )

  private implicit def eventsIdentifiersEncoder[F[_]]: EntityEncoder[F, List[EventIdentifiers]] = {
    implicit val eventIdentifiersEncoder: io.circe.Encoder[EventIdentifiers] = deriveEncoder[EventIdentifiers]
    jsonEncoderOf[F, List[EventIdentifiers]]
      .withContentType(`Content-Type`(identityMediaType))
  }

  private implicit def entitiesDecoder[F[_]: Async]: EntityDecoder[F, List[Identity]] = {
    implicit val identityDecoder: Decoder[Identity] = deriveDecoder[Identity]
    jsonOfWithMedia[F, List[Identity]](identityMediaType)
  }
}
