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
import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.Clock
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._

import io.circe._
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils, JsonUtils}
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter.Adapted

/**
 * Transforms a collector payload which either:
 * 1. Provides a set of kv pairs on a GET querystring with a &schema={iglu schema uri} parameter.
 * 2. Provides a &schema={iglu schema uri} parameter on a POST querystring and a set of kv pairs in
 * the body.
 *    - Formatted as JSON
 *    - Formatted as a Form Body
 */
object IgluAdapter extends Adapter {
  // Tracker version for an Iglu-compatible webhook
  private val TrackerVersion = "com.snowplowanalytics.iglu-v1"

  // Create a simple formatter function
  private val IgluFormatter: FormatterFunc = buildFormatter() // For defaults

  private val contentTypes = (
    "application/json",
    "application/json; charset=utf-8",
    "application/x-www-form-urlencoded"
  )
  private val contentTypesStr = contentTypes.productIterator.mkString(", ")

  /**
   * Converts a CollectorPayload instance into raw events. Currently we only support a single event
   * Iglu-compatible self-describing event passed in on the querystring.
   * @param payload The CollectorPaylod containing one or more raw events as collected by a Snowplow
   * collector
   * @param client The Iglu client used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  override def toRawEvents[F[_]: Monad: Clock](
    payload: CollectorPayload,
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F],
    maxJsonDepth: Int
  ): F[Adapted] = {
    val _ = client
    val params = toMap(payload.querystring)
    (params.get("schema").flatten, payload.body, payload.contentType) match {
      case (_, Some(_), None) =>
        val msg = s"expected one of $contentTypesStr"
        Monad[F].pure(
          FailureDetails.AdapterFailure.InputData("contentType", none, msg).invalidNel
        )
      case (None, Some(body), Some(contentType)) =>
        Monad[F].pure(payloadSdJsonToEvent(payload, body, contentType, params, maxJsonDepth))
      case (Some(schemaUri), _, _) => // Ignore body
        Monad[F].pure(payloadToEventWithSchema(payload, schemaUri, params, maxJsonDepth))
      case (None, None, _) =>
        val nel = NonEmptyList.of(
          FailureDetails.AdapterFailure
            .InputData("schema", none, "empty `schema` field"),
          FailureDetails.AdapterFailure.InputData("body", none, "empty body")
        )
        Monad[F].pure(nel.invalid)
    }
  }

  // --- SelfDescribingJson Payloads

  /**
   * Processes a potential SelfDescribingJson into a validated raw-event.
   * @param payload The CollectorPaylod containing one or more raw events
   * @param body The extracted body string
   * @param contentType The extracted contentType string
   * @param params The raw map of params from the querystring.
   */
  private[registry] def payloadSdJsonToEvent(
    payload: CollectorPayload,
    body: String,
    contentType: String,
    params: Map[String, Option[String]],
    maxJsonDepth: Int
  ): ValidatedNel[FailureDetails.AdapterFailure, NonEmptyList[RawEvent]] =
    contentType match {
      case contentTypes._1 => sdJsonBodyToEvent(payload, body, params, maxJsonDepth)
      case contentTypes._2 => sdJsonBodyToEvent(payload, body, params, maxJsonDepth)
      case _ =>
        val msg = s"expected one of ${List(contentTypes._1, contentTypes._2).mkString(", ")}"
        FailureDetails.AdapterFailure
          .InputData("contentType", contentType.some, msg)
          .invalidNel
    }

  /**
   * Processes a potential SelfDescribingJson into a validated raw-event.
   * @param payload The CollectorPaylod containing one or more raw events
   * @param body The extracted body string
   * @param params The raw map of params from the querystring.
   */
  private[registry] def sdJsonBodyToEvent(
    payload: CollectorPayload,
    body: String,
    params: Map[String, Option[String]],
    maxJsonDepth: Int
  ): ValidatedNel[FailureDetails.AdapterFailure, NonEmptyList[RawEvent]] =
    JsonUtils.extractJson(body, maxJsonDepth) match {
      case Right(parsed) =>
        SelfDescribingData.parse(parsed) match {
          case Left(parseError) =>
            FailureDetails.AdapterFailure.NotIglu(parsed, parseError).invalidNel
          case Right(sd) =>
            NonEmptyList
              .one(
                RawEvent(
                  api = payload.api,
                  parameters = toUnstructEventParams(
                    TrackerVersion,
                    params,
                    sd.schema,
                    sd.data,
                    "app"
                  ),
                  contentType = payload.contentType,
                  source = payload.source,
                  context = payload.context
                )
              )
              .valid
        }
      case Left(e) =>
        FailureDetails.AdapterFailure.NotJson("body", Option(body), e).invalidNel
    }

  // --- Payloads with the Schema in the Query-String

  /**
   * Processes a payload that has the schema field in the query-string.
   * @param payload The CollectorPaylod containing one or more raw events
   * @param schemaUri The schema-uri found (potentially invalid)
   * @param params The raw map of params from the querystring.
   */
  private[registry] def payloadToEventWithSchema(
    payload: CollectorPayload,
    schemaUri: String,
    params: Map[String, Option[String]],
    maxJsonDepth: Int
  ): ValidatedNel[FailureDetails.AdapterFailure, NonEmptyList[RawEvent]] =
    SchemaKey.fromUri(schemaUri) match {
      case Left(parseError) =>
        FailureDetails.AdapterFailure
          .InputData("schema", schemaUri.some, parseError.code)
          .invalidNel
      case Right(key) =>
        (payload.body, payload.contentType) match {
          case (None, _) =>
            NonEmptyList
              .one(
                RawEvent(
                  api = payload.api,
                  parameters = toUnstructEventParams(
                    TrackerVersion,
                    params - "schema",
                    key,
                    IgluFormatter,
                    "app"
                  ),
                  contentType = payload.contentType,
                  source = payload.source,
                  context = payload.context
                )
              )
              .valid
          case (Some(body), Some(contentType)) =>
            contentType match {
              case contentTypes._1 => jsonBodyToEvent(payload, body, key, params, maxJsonDepth)
              case contentTypes._2 => jsonBodyToEvent(payload, body, key, params, maxJsonDepth)
              case contentTypes._3 => formBodyToEvent(payload, body, key, params)
              case _ =>
                val msg = s"expected one of $contentTypesStr"
                FailureDetails.AdapterFailure
                  .InputData("contentType", contentType.some, msg)
                  .invalidNel
            }
          case (_, None) =>
            val msg = s"expected one of $contentTypesStr"
            FailureDetails.AdapterFailure
              .InputData("contentType", none, msg)
              .invalidNel
        }
    }

  /**
   * Converts a json payload into a single validated event
   * @param body json payload as POST'd by a webhook
   * @param payload the rest of the payload details
   * @param schemaUri the schemaUri for the event
   * @param params The query string parameters
   * @return a single validated event
   */
  private[registry] def jsonBodyToEvent(
    payload: CollectorPayload,
    body: String,
    schemaUri: SchemaKey,
    params: Map[String, Option[String]],
    maxJsonDepth: Int
  ): ValidatedNel[FailureDetails.AdapterFailure, NonEmptyList[RawEvent]] = {
    def buildRawEvent(e: Json): RawEvent =
      RawEvent(
        api = payload.api,
        parameters = toUnstructEventParams(TrackerVersion, params - "schema", schemaUri, e, "app"),
        contentType = payload.contentType,
        source = payload.source,
        context = payload.context
      )

    JsonUtils.extractJson(body, maxJsonDepth) match {
      case Right(parsed) =>
        parsed.asArray match {
          case Some(array) =>
            array.toList match {
              case h :: t => NonEmptyList.of(buildRawEvent(h), t.map(buildRawEvent): _*).valid
              case _ =>
                FailureDetails.AdapterFailure
                  .InputData("body", body.some, "empty array of events")
                  .invalidNel
            }
          case _ =>
            if (parsed.asObject.fold(true)(_.isEmpty))
              FailureDetails.AdapterFailure
                .InputData("body", body.some, "has no key-value pairs")
                .invalidNel
            else
              NonEmptyList.one(buildRawEvent(parsed)).valid
        }
      case Left(e) =>
        FailureDetails.AdapterFailure.NotJson("body", Option(body), e).invalidNel
    }
  }

  /**
   * Converts a form body payload into a single validated event
   * @param body the form body from the payload as POST'd by a webhook
   * @param payload the rest of the payload details
   * @param schemaUri the schemaUri for the event
   * @param params The query string parameters
   * @return a single validated event
   */
  private[registry] def formBodyToEvent(
    payload: CollectorPayload,
    body: String,
    schemaUri: SchemaKey,
    params: Map[String, Option[String]]
  ): ValidatedNel[FailureDetails.AdapterFailure, NonEmptyList[RawEvent]] =
    (for {
      bodyMap <- ConversionUtils
                   .parseUrlEncodedForm(body)
                   .leftMap(e => FailureDetails.AdapterFailure.InputData("body", body.some, e))
      event = bodyMap.asJson
      rawEvent = NonEmptyList
                   .one(
                     RawEvent(
                       api = payload.api,
                       parameters = toUnstructEventParams(TrackerVersion, params - "schema", schemaUri, event, "srv"),
                       contentType = payload.contentType,
                       source = payload.source,
                       context = payload.context
                     )
                   )
    } yield rawEvent).toValidatedNel
}
