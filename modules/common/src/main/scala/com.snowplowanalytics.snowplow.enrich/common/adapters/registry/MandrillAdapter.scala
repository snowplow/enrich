/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry

import cats.Monad
import cats.data.ValidatedNel
import cats.effect.Clock
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._
import io.circe._

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils, JsonUtils}
import com.snowplowanalytics.snowplow.enrich.common.adapters._
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter.Adapted

/**
 * Transforms a collector payload which conforms to a known version of the Mandrill Tracking webhook
 * into raw events.
 */
case class MandrillAdapter(schemas: MandrillSchemas) extends Adapter {
  // Tracker version for an Mandrill Tracking webhook
  private val TrackerVersion = "com.mandrill-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private[registry] val EventSchemaMap = Map(
    "hard_bounce" -> schemas.messageBouncedSchemaKey,
    "click" -> schemas.messageClickedSchemaKey,
    "deferral" -> schemas.messageDelayedSchemaKey,
    "spam" -> schemas.messageMarkedAsSpamSchemaKey,
    "open" -> schemas.messageOpenedSchemaKey,
    "reject" -> schemas.messageRejectedSchemaKey,
    "send" -> schemas.messageSentSchemaKey,
    "soft_bounce" -> schemas.messageSoftBouncedSchemaKey,
    "unsub" -> schemas.recipientUnsubscribedSchemaKey,
    "delivered" -> schemas.messageDeliveredSchemaKey
  )

  /**
   * Converts a CollectorPayload instance into raw events.
   * A Mandrill Tracking payload contains many events in the body of the payload, stored within a
   * HTTP encoded string.
   * We expect the event parameter of these events to be 1 of 9 options otherwise we have an
   * unsupported event type.
   * @param payload The CollectorPayload containing one or more raw events as collected by a
   * Snowplow collector
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad: Clock](
    payload: CollectorPayload,
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F],
    maxJsonDepth: Int
  ): F[Adapted] =
    (payload.body, payload.contentType) match {
      case (None, _) =>
        val failure = FailureDetails.AdapterFailure.InputData(
          "body",
          none,
          "empty body: no events to process"
        )
        Monad[F].pure(failure.invalidNel)
      case (_, None) =>
        val msg = s"no content type: expected $ContentType"
        Monad[F].pure(
          FailureDetails.AdapterFailure.InputData("contentType", none, msg).invalidNel
        )
      case (_, Some(ct)) if ct != ContentType =>
        val msg = s"expected $ContentType"
        Monad[F].pure(
          FailureDetails.AdapterFailure
            .InputData("contentType", ct.some, msg)
            .invalidNel
        )
      case (Some(body), _) =>
        payloadBodyToEvents(body, maxJsonDepth) match {
          case Left(str) => Monad[F].pure(str.invalidNel)
          case Right(list) =>
            val _ = client
            // Create our list of Validated RawEvents
            val rawEventsList: List[ValidatedNel[FailureDetails.AdapterFailure, RawEvent]] =
              for {
                (event, index) <- list.zipWithIndex
              } yield {
                val eventOpt = event.hcursor.get[String]("event").toOption
                for {
                  schema <- lookupSchema(eventOpt, index, EventSchemaMap).toValidatedNel
                } yield {
                  val formattedEvent =
                    cleanupJsonEventValues(event, eventOpt.map(("event", _)), List("ts"))
                  val qsParams = toMap(payload.querystring)
                  RawEvent(
                    api = payload.api,
                    parameters = toUnstructEventParams(
                      TrackerVersion,
                      qsParams,
                      schema,
                      formattedEvent,
                      "srv"
                    ),
                    contentType = payload.contentType,
                    source = payload.source,
                    context = payload.context
                  )
                }
              }
            // Processes the List for Failures and Successes and returns ValidatedRawEvents
            Monad[F].pure(rawEventsListProcessor(rawEventsList))
        }
    }

  /**
   * Returns a list of events from the payload body of a Mandrill Event. Each event will be
   * formatted as an individual JSON.
   * NOTE: The payload.body string must adhere to UTF-8 encoding standards.
   * @param rawEventString The encoded string from the Mandrill payload body
   * @return a list of single events formatted as JSONs or a Failure String
   */
  private[registry] def payloadBodyToEvents(rawEventString: String, maxJsonDepth: Int): Either[FailureDetails.AdapterFailure, List[Json]] =
    for {
      bodyMap <- ConversionUtils
                   .parseUrlEncodedForm(rawEventString)
                   .map(_.collect { case (k, Some(v)) => (k, v) })
                   .leftMap(e => FailureDetails.AdapterFailure.InputData("body", rawEventString.some, e))
      res <- bodyMap match {
               case map if map.size != 1 =>
                 val msg = s"body should have size 1: actual size ${map.size}"
                 FailureDetails.AdapterFailure
                   .InputData("body", rawEventString.some, msg)
                   .asLeft
               case map =>
                 map.get("mandrill_events") match {
                   case None =>
                     val msg = "no `mandrill_events` parameter provided"
                     FailureDetails.AdapterFailure
                       .InputData("body", rawEventString.some, msg)
                       .asLeft
                   case Some("") =>
                     val msg = "`mandrill_events` field is empty"
                     FailureDetails.AdapterFailure
                       .InputData("body", rawEventString.some, msg)
                       .asLeft
                   case Some(dStr) =>
                     JsonUtils
                       .extractJson(dStr, maxJsonDepth)
                       .leftMap(e =>
                         FailureDetails.AdapterFailure
                           .NotJson("mandril_events", dStr.some, e)
                       )
                       .flatMap { json =>
                         json.asArray match {
                           case Some(array) => array.toList.asRight
                           case _ =>
                             val msg = "not a json array"
                             FailureDetails.AdapterFailure
                               .InputData("mandrill_events", dStr.some, msg)
                               .asLeft
                         }
                       }
                 }
             }
    } yield res

}
