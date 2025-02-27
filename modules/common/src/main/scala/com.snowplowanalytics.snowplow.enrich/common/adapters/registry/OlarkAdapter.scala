/*
 * Copyright (c) 2016-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.JavaConverters._
import scala.util.{Try, Success => TS, Failure => TF}

import cats.Monad
import cats.data.NonEmptyList
import cats.instances.either._
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._

import cats.effect.Clock

import io.circe._
import io.circe.optics.JsonPath._

import org.apache.http.client.utils.URLEncodedUtils
import org.joda.time.DateTime

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.{JsonUtils => JU}
import com.snowplowanalytics.snowplow.enrich.common.adapters._
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter.Adapted

/**
 * Transforms a collector payload which conforms to a known version of the Olark Tracking webhook
 * into raw events.
 */
case class OlarkAdapter(schemas: OlarkSchemas) extends Adapter {
  // Tracker version for an Olark Tracking webhook
  private val TrackerVersion = "com.olark-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "transcript" -> schemas.transcriptSchemaKey,
    "offline_message" -> schemas.offlineMessageSchemaKey
  )

  /**
   * Converts a CollectorPayload instance into raw events. An Olark Tracking payload contains one
   * single event in the body of the payload, stored within a HTTP encoded string.
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
      case (Some(body), _) if body.isEmpty =>
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
          FailureDetails.AdapterFailure.InputData("contentType", none, msg).invalidNel
        )
      case (Some(body), _) =>
        val _ = client
        val qsParams = toMap(payload.querystring)
        Try {
          toMap(
            URLEncodedUtils.parse(URI.create("http://localhost/?" + body), UTF_8).asScala.toList
          )
            .collect { case (k, Some(v)) => (k, v) }
        } match {
          case TF(e) =>
            val msg = s"could not parse body: ${JU.stripInstanceEtc(e.getMessage).orNull}"
            Monad[F].pure(
              FailureDetails.AdapterFailure.InputData(body, body.some, msg).invalidNel
            )
          case TS(bodyMap) =>
            Monad[F].pure(
              (for {
                event <- payloadBodyToEvent(bodyMap, maxJsonDepth)
                eventType = event.hcursor.get[Json]("operators").toOption match {
                              case Some(_) => "transcript"
                              case _ => "offline_message"
                            }
                schema <- lookupSchema(eventType.some, EventSchemaMap)
                transformedEvent <- transformTimestamps(event)
              } yield NonEmptyList.one(
                RawEvent(
                  api = payload.api,
                  parameters = toUnstructEventParams(
                    TrackerVersion,
                    qsParams,
                    schema,
                    camelize(transformedEvent),
                    "srv"
                  ),
                  contentType = payload.contentType,
                  source = payload.source,
                  context = payload.context
                )
              )).toValidatedNel
            )
        }
    }

  /**
   * Converts all olark timestamps in a parsed transcript or offline_message json object to iso8601
   * strings
   * @param json a parsed event
   * @return JObject the event with timstamps replaced
   */
  private def transformTimestamps(json: Json): Either[FailureDetails.AdapterFailure, Json] = {
    def toMsec(oTs: String): Either[FailureDetails.AdapterFailure, Long] =
      for {
        formatted <- oTs.split('.') match {
                       case Array(sec) => s"${sec}000".asRight
                       case Array(sec, msec) => s"${sec}${msec.take(3).padTo(3, '0')}".asRight
                       case _ =>
                         FailureDetails.AdapterFailure
                           .InputData("timestamp", oTs.some, "unexpected timestamp format")
                           .asLeft
                     }
        long <- Either
                  .catchNonFatal(formatted.toLong)
                  .leftMap { _ =>
                    FailureDetails.AdapterFailure.InputData(
                      "timestamp",
                      formatted.some,
                      "cannot be converted to Double"
                    )
                  }
      } yield long

    type EitherAF[A] = Either[FailureDetails.AdapterFailure, A]
    root.items.each.timestamp.string.modifyF[EitherAF] { v =>
      toMsec(v).map(long => JsonSchemaDateTimeFormat.print(new DateTime(long)))
    }(json)
  }

  /**
   * Converts a querystring payload into an event
   * @param bodyMap The converted map from the querystring
   */
  private def payloadBodyToEvent(bodyMap: Map[String, String], maxJsonDepth: Int): Either[FailureDetails.AdapterFailure, Json] =
    bodyMap.get("data") match {
      case None | Some("") =>
        FailureDetails.AdapterFailure
          .InputData("data", none, "missing 'data' field")
          .asLeft
      case Some(json) =>
        JU.extractJson(json, maxJsonDepth)
          .leftMap(e => FailureDetails.AdapterFailure.NotJson("data", json.some, e))
    }
}
