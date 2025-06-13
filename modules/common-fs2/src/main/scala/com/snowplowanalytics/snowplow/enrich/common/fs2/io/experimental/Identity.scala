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
package com.snowplowanalytics.snowplow.enrich.common.fs2.io.experimental

import cats.effect.{Async, Sync}
import cats.implicits._
import fs2.{Pipe, Stream}
import org.http4s.client.{Client => HttpClient}
import org.http4s.{BasicCredentials, EntityDecoder, EntityEncoder, Headers, MediaType, Method, Request}
import org.http4s.headers.{Authorization, `Content-Type`}
import org.http4s.circe._
import io.circe.Json
import io.circe.generic.semiauto._
import io.circe.literal._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry.syntax.all._
import retry.RetryPolicy

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.OptionIor
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Identity => IdentityConfig}
import com.snowplowanalytics.snowplow.enrich.common.fs2.Enrich
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Retries

object Identity {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def pipeIfConfigured[F[_]: Async, A](
    config: Option[IdentityConfig],
    httpClient: HttpClient[F]
  ): Pipe[F, List[Enrich.Result[A]], List[Enrich.Result[A]]] =
    config match {
      case Some(c) => pipe(c, httpClient)
      case None => identity
    }

  def pipe[F[_]: Async, A](config: IdentityConfig, httpClient: HttpClient[F]): Pipe[F, List[Enrich.Result[A]], List[Enrich.Result[A]]] = {
    in =>
      val retryPolicy = Retries.fullJitter[F](config.backoffPolicy)
      Stream.eval(Logger[F].info(show"Identity enrichment enabled with endpoint ${config.endpoint}")).drain ++
        in.parEvalMap(config.concurrency) { results =>
          val enrichedEvents = results.view.flatMap(_.enriched).foldLeft(List.empty[EnrichedEvent]) {
            case (acc, OptionIor.Right(e)) => e :: acc
            case (acc, OptionIor.Both(_, e)) => e :: acc
            case (acc, _) => acc
          }
          if (enrichedEvents.nonEmpty)
            runHttpTxn(config, retryPolicy, httpClient, enrichedEvents)
              .flatMap { response =>
                Sync[F]
                  .delay {
                    mergeInIdentifiers(results, response)
                  }
                  .as(results)
              }
          else
            results.pure[F]
        }
  }

  private val identitySchemaKey: SchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "identity", "jsonschema", SchemaVer.Full(1, 0, 0))

  private def runHttpTxn[F[_]: Async](
    config: IdentityConfig,
    retryPolicy: RetryPolicy[F],
    httpClient: HttpClient[F],
    batch: List[EnrichedEvent]
  ): F[List[BatchIdentity]] = {
    val requestBody = batch.map { e =>
      val identifiers = Identifiers(
        domain_userid = Option(e.domain_userid),
        network_userid = Option(e.network_userid),
        user_id = Option(e.user_id)
      )
      BatchIdentifiers(e.event_id, identifiers)
    }
    val request = Request[F](
      method = Method.POST,
      uri = config.endpoint / "identities" / "batch",
      headers = Headers(
        Authorization(BasicCredentials(config.username, config.password))
      )
    ).withEntity(requestBody)
    httpClient
      .expect[List[BatchIdentity]](request)
      .retryingOnAllErrors(
        policy = retryPolicy,
        onError = (exception, retryDetails) =>
          Logger[F]
            .error(exception)(s"Error calling Identity API (${retryDetails.retriesSoFar} retries)")
      )
  }

  private def mergeInIdentifiers[A](inputs: List[Enrich.Result[A]], response: List[BatchIdentity]): Unit = {
    val byEventId = response.map { batchIdentity =>
      val context = json"""{
          "snowplowId": ${batchIdentity.snowplowId},
          "createdAt": ${batchIdentity.createdAt},
          "merged": ${batchIdentity.merged}
        }"""
      val sdj = SelfDescribingData(identitySchemaKey, context)
      batchIdentity.eventId -> sdj
    }.toMap
    inputs.foreach { input =>
      input.enriched.foreach {
        case OptionIor.Right(e) =>
          byEventId.get(e.event_id).foreach { sdj =>
            e.derived_contexts = sdj :: e.derived_contexts
          }
        case OptionIor.Both(_, e) =>
          byEventId.get(e.event_id).foreach { sdj =>
            e.derived_contexts = sdj :: e.derived_contexts
          }
        case _ =>
          ()
      }
    }
  }

  private case class BatchIdentifiers(eventId: String, identifiers: Identifiers)

  private case class Identifiers(
    domain_userid: Option[String],
    network_userid: Option[String],
    user_id: Option[String]
  )

  private case class BatchIdentity(
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
