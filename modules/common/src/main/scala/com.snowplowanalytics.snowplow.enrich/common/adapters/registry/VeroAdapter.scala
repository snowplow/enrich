/*
 * Copyright (c) 2018-present Snowplow Analytics Ltd.
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
import cats.effect.Clock
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._

import io.circe._

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonUtils
import com.snowplowanalytics.snowplow.enrich.common.adapters._
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter.Adapted

/** Transforms a collector payload which fits the Vero webhook into raw events. */
case class VeroAdapter(schemas: VeroSchemas) extends Adapter {
  // Tracker version for an Vero webhook
  private val TrackerVersion = "com.getvero-v1"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "bounced" -> schemas.bouncedSchemaKey,
    "clicked" -> schemas.clickedSchemaKey,
    "delivered" -> schemas.deliveredSchemaKey,
    "opened" -> schemas.openedSchemaKey,
    "sent" -> schemas.sentSchemaKey,
    "unsubscribed" -> schemas.unsubscribedSchemaKey,
    "user_created" -> schemas.createdSchemaKey,
    "user_updated" -> schemas.updatedSchemaKey
  )

  /**
   * Converts a CollectorPayload instance into raw events. A Vero API payload only contains a single
   * event. We expect the type parameter to match the supported events, otherwise we have an
   * unsupported event type.
   * @param payload The CollectorPayload containing one or more raw events
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
      case (Some(body), _) =>
        val _ = client
        val event = payloadBodyToEvent(body, payload, maxJsonDepth)
        Monad[F].pure(rawEventsListProcessor(List(event.toValidatedNel)))
    }

  /**
   * Converts a payload into a single validated event. Expects a valid json returns failure if one
   * is not present
   * @param json Payload body that is sent by Vero
   * @param payload The details of the payload
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  private def payloadBodyToEvent(
    json: String,
    payload: CollectorPayload,
    maxJsonDepth: Int
  ): Either[FailureDetails.AdapterFailure, RawEvent] =
    for {
      parsed <- JsonUtils
                  .extractJson(json, maxJsonDepth)
                  .leftMap(e => FailureDetails.AdapterFailure.NotJson("body", json.some, e))
      eventType <- parsed.hcursor
                     .get[String]("type")
                     .leftMap(e =>
                       FailureDetails.AdapterFailure
                         .InputData("type", none, s"could not extract 'type': ${e.getMessage}")
                     )
      formattedEvent = cleanupJsonEventValues(
                         parsed,
                         ("type", eventType).some,
                         List(s"${eventType}_at", "triggered_at")
                       )
      reformattedEvent = reformatParameters(formattedEvent)
      schema <- lookupSchema(eventType.some, EventSchemaMap)
      params = toUnstructEventParams(
                 TrackerVersion,
                 toMap(payload.querystring),
                 schema,
                 reformattedEvent,
                 "srv"
               )
      rawEvent = RawEvent(
                   api = payload.api,
                   parameters = params,
                   contentType = payload.contentType,
                   source = payload.source,
                   context = payload.context
                 )
    } yield rawEvent

  /**
   * Returns an updated Vero event JSON where the "_tags" field is renamed to "tags"
   * @param json The event JSON which we need to update values for
   * @return the updated JSON with updated fields and values
   */
  def reformatParameters(json: Json): Json = {
    val oldTagsKey = "_tags"
    val tagsKey = "tags"
    json.mapObject { obj =>
      val updatedObj = obj.toMap.map {
        case (k, v) if k == oldTagsKey => (tagsKey, v)
        case (k, v) if v.isObject => (k, reformatParameters(v))
        case (k, v) => (k, v)
      }
      JsonObject(updatedObj.toList: _*)
    }
  }
}
