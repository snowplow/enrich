/*
 * Copyright (c) 2018-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
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
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.{JsonUtils => JU}
import com.snowplowanalytics.snowplow.enrich.common.adapters._
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter.Adapted

/**
 * Transforms a collector payload which conforms to a known version of the Marketo webhook into raw
 * events.
 */
case class MarketoAdapter(schemas: MarketoSchemas) extends Adapter {
  // Tracker version for an Marketo webhook
  private val TrackerVersion = "com.marketo-v1"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "event" -> schemas.eventSchemaKey
  )

  // Datetime format used by Marketo
  private val MarketoDateTimeFormat =
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC)

  // Fields containing data which need to be reformatted
  private val DateFields = List(
    "acquisition_date",
    "created_at",
    "email_suspended_at",
    "last_referred_enrollment",
    "last_referred_visit",
    "updated_at",
    "datetime",
    "last_interesting_moment_date"
  )

  /**
   * Converts a CollectorPayload instance into raw events.
   * Marketo event contains no "type" field and since there's only 1 schema the function
   * lookupschema takes the eventType parameter as "event".
   * We expect the type parameter to match the supported events, else
   * we have an unsupported event type.
   * @param payload The CollectorPayload containing one or more raw events
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad: RegistryLookup: Clock](
    payload: CollectorPayload,
    client: IgluCirceClient[F]
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
        val event = payloadBodyToEvent(body, payload)
        Monad[F].pure(rawEventsListProcessor(List(event)))
    }

  /**
   * Returns a validated JSON payload event. Converts all date-time values to a valid format.
   * The payload will be validated against marketo "event" schema.
   * @param json The JSON payload sent by Marketo
   * @param payload Rest of the payload details
   * @return a validated JSON payload on Success, or a NEL
   */
  private def payloadBodyToEvent(json: String, payload: CollectorPayload): ValidatedNel[FailureDetails.AdapterFailure, RawEvent] =
    (for {
      parsed <- JU
                  .extractJson(json)
                  .leftMap(e => FailureDetails.AdapterFailure.NotJson("body", json.some, e))

      parsedConverted <- if (parsed.isObject) reformatParameters(parsed).asRight
                         else
                           FailureDetails.AdapterFailure
                             .InputData("body", json.some, "not a json object")
                             .asLeft

      // The payload doesn't contain a "type" field so we're constraining the eventType to be of
      // type "event"
      eventType = Some("event")
      schema <- lookupSchema(eventType, EventSchemaMap)
      params = toUnstructEventParams(
                 TrackerVersion,
                 toMap(payload.querystring),
                 schema,
                 parsedConverted,
                 "srv"
               )
      rawEvent = RawEvent(
                   api = payload.api,
                   parameters = params,
                   contentType = payload.contentType,
                   source = payload.source,
                   context = payload.context
                 )
    } yield rawEvent).toValidatedNel

  private[registry] def reformatParameters(json: Json): Json =
    json.mapObject { obj =>
      val updatedObj = obj.toMap.map {
        case (k, v) if DateFields.contains(k) =>
          (
            k,
            v.mapString { s =>
              Either
                .catchNonFatal(JU.toJsonSchemaDateTime(s, MarketoDateTimeFormat))
                .getOrElse(s)
            }
          )
        case (k, v) if v.isObject => (k, reformatParameters(v))
        case (k, v) => (k, v)
      }
      JsonObject(updatedObj.toList: _*)
    }
}
