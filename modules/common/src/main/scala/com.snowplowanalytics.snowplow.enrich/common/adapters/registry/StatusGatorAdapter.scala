/*
 * Copyright (c) 2016-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
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
import cats.effect.Clock
import cats.syntax.option._
import cats.syntax.validated._
import io.circe.syntax._
import org.apache.http.client.utils.URLEncodedUtils

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.{JsonUtils => JU}
import com.snowplowanalytics.snowplow.enrich.common.adapters._
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter.Adapted

/**
 * Transforms a collector payload which conforms to a known version of the StatusGator Tracking
 * webhook into raw events.
 */
case class StatusGatorAdapter(schemas: StatusGatorSchemas) extends Adapter {
  // Tracker version for an StatusGator Tracking webhook
  private val TrackerVersion = "com.statusgator-v1"

  // Expected content type for a request body
  private val ContentType = "application/x-www-form-urlencoded"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchema = schemas.statusChangeSchemaKey

  /**
   * Converts a CollectorPayload instance into raw events. A StatusGator Tracking payload contains
   * one single event in the body of the payload, stored within a HTTP encoded string.
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
          FailureDetails.AdapterFailure
            .InputData("contentType", ct.some, msg)
            .invalidNel
        )
      case (Some(body), _) =>
        val _ = client
        val qsParams = toMap(payload.querystring)
        Try {
          toMap(
            URLEncodedUtils.parse(URI.create("http://localhost/?" + body), UTF_8).asScala.toList
          )
        } match {
          case TF(e) =>
            val msg = s"could not parse body: ${JU.stripInstanceEtc(e.getMessage).orNull}"
            Monad[F].pure(
              FailureDetails.AdapterFailure
                .InputData("body", body.some, msg)
                .invalidNel
            )
          case TS(bodyMap) =>
            Monad[F].pure(
              NonEmptyList
                .one(
                  RawEvent(
                    api = payload.api,
                    parameters = toUnstructEventParams(
                      TrackerVersion,
                      qsParams,
                      EventSchema,
                      camelize(bodyMap.asJson),
                      "srv"
                    ),
                    contentType = payload.contentType,
                    source = payload.source,
                    context = payload.context
                  )
                )
                .valid
            )
        }
    }
}
