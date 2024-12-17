/*
 * Copyright (c) 2015-present Snowplow Analytics Ltd.
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
import cats.syntax.apply._
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._
import io.circe.DecodingFailure
import org.joda.time.{DateTime, DateTimeZone}

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonUtils
import com.snowplowanalytics.snowplow.enrich.common.adapters._
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter.Adapted

/**
 * Transforms a collector payload which conforms to a known version of the UrbanAirship Connect API
 * into raw events.
 */
case class UrbanAirshipAdapter(schemas: UrbanAirshipSchemas) extends Adapter {
  // Tracker version for an UrbanAirship Connect API
  private val TrackerVersion = "com.urbanairship.connect-v1"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "CLOSE" -> schemas.closeSchemaKey,
    "CUSTOM" -> schemas.customSchemaKey,
    "FIRST_OPEN" -> schemas.firstOpenSchemaKey,
    "IN_APP_MESSAGE_DISPLAY" -> schemas.inAppMessageDisplaySchemaKey,
    "IN_APP_MESSAGE_EXPIRATION" -> schemas.inAppMessageExpirationSchemaKey,
    "IN_APP_MESSAGE_RESOLUTION" -> schemas.inAppMessageResolutionSchemaKey,
    "LOCATION" -> schemas.locationSchemaKey,
    "OPEN" -> schemas.openSchemaKey,
    "PUSH_BODY" -> schemas.pushBodySchemaKey,
    "REGION" -> schemas.regionSchemaKey,
    "RICH_DELETE" -> schemas.richDeleteSchemaKey,
    "RICH_DELIVERY" -> schemas.richDeliverySchemaKey,
    "RICH_HEAD" -> schemas.richHeadSchemaKey,
    "SEND" -> schemas.sendSchemaKey,
    "TAG_CHANGE" -> schemas.tagChangeSchemaKey,
    "UNINSTALL" -> schemas.uninstallSchemaKey
  )

  /**
   * Converts a CollectorPayload instance into raw events. A UrbanAirship connect API payload only
   * contains a single event. We expect the name parameter to match the supported events, else
   * we have an unsupported event type.
   * @param payload The CollectorPayload containing one or more raw events
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad: Clock](
    payload: CollectorPayload,
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F]
  ): F[Adapted] =
    (payload.body, payload.contentType) match {
      case (None, _) =>
        val failure = FailureDetails.AdapterFailure.InputData(
          "body",
          none,
          "empty body: no events to process"
        )
        Monad[F].pure(failure.invalidNel)
      case (_, Some(ct)) =>
        val msg = s"expected no content type"
        Monad[F].pure(
          FailureDetails.AdapterFailure
            .InputData("contentType", ct.some, msg)
            .invalidNel
        )
      case (Some(body), _) =>
        val _ = client
        val event = payloadBodyToEvent(body, payload)
        Monad[F].pure(rawEventsListProcessor(List(event)))
    }

  /**
   * Converts payload into a single validated event. Expects a valid json, returns failure if one is
   * not present.
   * @param bodyJson json payload as a string
   * @param payload other payload details
   * @return a validated event - a success is the RawEvent, failures will contain the reasons
   */
  private def payloadBodyToEvent(bodyJson: String, payload: CollectorPayload): ValidatedNel[FailureDetails.AdapterFailure, RawEvent] = {
    def toTtmFormat(jsonTimestamp: String) =
      "%d".format(new DateTime(jsonTimestamp).getMillis)

    def err(field: String, e: DecodingFailure): FailureDetails.AdapterFailure.InputData =
      FailureDetails.AdapterFailure.InputData(
        field,
        none,
        s"could not extract '$field': ${e.getMessage}"
      )

    JsonUtils.extractJson(bodyJson) match {
      case Right(json) =>
        val cursor = json.hcursor
        val eventType = cursor.get[String]("type").toOption
        val trueTs = cursor.get[String]("occurred").leftMap(err("occurred", _)).toValidatedNel
        val eid = cursor.get[String]("id").leftMap(err("id", _)).toValidatedNel
        val collectorTs = cursor
          .get[String]("processed")
          .leftMap(err("processed", _))
          .toValidatedNel
        (
          trueTs,
          eid,
          collectorTs,
          lookupSchema(eventType, EventSchemaMap).toValidatedNel
        ).mapN { (tts, id, cts, schema) =>
          RawEvent(
            api = payload.api,
            parameters = toUnstructEventParams(
              TrackerVersion,
              toMap(payload.querystring) ++ Map("ttm" -> Option(toTtmFormat(tts)), "eid" -> Option(id)),
              schema,
              json,
              "srv"
            ),
            contentType = payload.contentType,
            source = payload.source,
            context = payload.context.copy(timestamp = Some(new DateTime(cts, DateTimeZone.UTC)))
          )
        }
      case Left(e) =>
        FailureDetails.AdapterFailure.NotJson("body", bodyJson.some, e).invalidNel
    }
  }
}
