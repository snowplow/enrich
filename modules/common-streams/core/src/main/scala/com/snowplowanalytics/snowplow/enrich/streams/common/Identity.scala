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

import cats.effect.{Async, Sync}
import cats.implicits._
import io.circe.Json
import io.circe.literal._
import io.circe.generic.semiauto._
import io.circe.literal._
import org.http4s.client.{Client => Http4sClient}
import org.http4s.{BasicCredentials, EntityDecoder, EntityEncoder, Headers, MediaType, Method, Request}
import org.http4s.headers.{Authorization, `Content-Type`}
import org.http4s.circe._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry.syntax.all._
import retry.RetryPolicies

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

object Identity {

  trait Api[F[_]] {
    def post(inputs: List[BatchIdentifiers]): F[List[BatchIdentity]]
    def concurrency: Int
  }

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def build[F[_]: Async](config: Config.Identity, httpClient: Http4sClient[F]): Api[F] = {
    val retryPolicy = RetryPolicies.fullJitter[F](config.retries.delay).join(RetryPolicies.limitRetries(config.retries.attempts - 1))
    new Api[F] {
      def post(inputs: List[BatchIdentifiers]): F[List[BatchIdentity]] = {
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
      def concurrency: Int = config.concurrency
    }
  }

  def addContexts[F[_]: Sync](api: Api[F], events: List[EnrichedEvent]): F[Unit] =
    for {
      req <- Sync[F].delay(buildRequestBody(events))
      res <- api.post(req)
      _ <- Sync[F].delay(mergeInIdentifiers(events, res))
    } yield ()

  private def buildRequestBody(events: List[EnrichedEvent]): List[BatchIdentifiers] =
    events.map { e =>
      val identifiers = Identifiers(
        domain_userid = Option(e.domain_userid),
        network_userid = Option(e.network_userid),
        user_id = Option(e.user_id)
      )
      BatchIdentifiers(e.event_id, identifiers)
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
    user_id: Option[String]
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
    implicit val batchIdentityDeoder = deriveDecoder[BatchIdentity]
    jsonOfWithMedia[F, List[BatchIdentity]](identityMediaType)
  }

  private implicit def entityEncoder[F[_]]: EntityEncoder[F, List[BatchIdentifiers]] = {
    implicit val identifiersEncoder = deriveEncoder[Identifiers].mapJson(_.deepDropNullValues)
    implicit val batchIdentifiersEncoder = deriveEncoder[BatchIdentifiers]
    jsonEncoderOf[F, List[BatchIdentifiers]]
      .withContentType(`Content-Type`(identityMediaType))
  }

}
