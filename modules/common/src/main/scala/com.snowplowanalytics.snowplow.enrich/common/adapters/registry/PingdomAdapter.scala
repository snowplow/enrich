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
import cats.data.NonEmptyList
import cats.effect.Clock
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._
import io.circe._
import org.apache.http.NameValuePair

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonUtils
import com.snowplowanalytics.snowplow.enrich.common.adapters._
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter.Adapted

/**
 * Transforms a collector payload which conforms to a known version of the Pingdom Tracking webhook
 * into raw events.
 */
case class PingdomAdapter(schemas: PingdomSchemas) extends Adapter {
  // Tracker version for an Pingdom Tracking webhook
  private val TrackerVersion = "com.pingdom-v1"

  // Regex for extracting data from querystring values which we
  // believe are incorrectly handled Python unicode strings.
  private val PingdomValueRegex = """\(u'([^']+)',\)""".r

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map(
    "assign" -> schemas.incidentAssignSchemaKey,
    "notify_user" -> schemas.incidentNotifyUserSchemaKey,
    "notify_of_close" -> schemas.incidentNotifyOfCloseSchemaKey
  )

  /**
   * Converts a CollectorPayload instance into raw events. A Pingdom Tracking payload only contains
   * a single event. We expect the name parameter to be one of two types otherwise we have an
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
    payload.querystring match {
      case Nil =>
        val msg = "empty querystring: no events to process"
        val failure =
          FailureDetails.AdapterFailure.InputData("querystring", none, msg)
        Monad[F].pure(failure.invalidNel)
      case qs =>
        reformatMapParams(qs) match {
          case Left(f) => Monad[F].pure(f.invalid)
          case Right(s) =>
            s.get("message") match {
              case Some(Some(event)) =>
                Monad[F].pure((for {
                  parsedEvent <- JsonUtils
                                   .extractJson(event, maxJsonDepth)
                                   .leftMap(e =>
                                     FailureDetails.AdapterFailure
                                       .NotJson("message", event.some, e)
                                   )
                  schema <- {
                    val eventOpt = parsedEvent.hcursor.downField("action").as[String].toOption
                    lookupSchema(eventOpt, EventSchemaMap)
                  }
                } yield {
                  val _ = client
                  val formattedEvent = reformatParameters(parsedEvent)
                  val qsParams = s - "message"
                  NonEmptyList.one(
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
                  )
                }).toValidatedNel)
              case _ =>
                val msg = "no `message` parameter provided"
                val formattedQS = s.map { case (k, v) => s"$k=${v.getOrElse("null")}" }.mkString("&")
                val failure = FailureDetails.AdapterFailure.InputData(
                  "querystring",
                  formattedQS.some,
                  msg
                )
                Monad[F].pure(failure.invalidNel)
            }
        }
    }

  /**
   * As Pingdom wraps each value in the querystring within: (u'[content]',) we need to check that
   * these wrappers have been removed by the Collector before we can use them.
   * example: p -> (u'app',) becomes p -> app
   * The expected behavior is that every value will have been cleaned by the Collector; however if
   * this is not the case we will not be able to process values.
   * @param params A list of name-value pairs from the querystring of the Pingdom payload
   * @return a Map of name-value pairs which has been validated as all passing the regex extraction
   * or return a NonEmptyList of Failures if any could not pass the regex.
   */
  private[registry] def reformatMapParams(
    params: List[NameValuePair]
  ): Either[NonEmptyList[FailureDetails.AdapterFailure], Map[String, Option[String]]] = {
    val formatted = params.map { nvp =>
      (nvp.getName, Option(nvp.getValue)) match {
        case (k, Some(PingdomValueRegex(v))) =>
          FailureDetails.AdapterFailure
            .InputData(k, v.some, s"should not pass regex $PingdomValueRegex")
            .asLeft
        case other => other.asRight
      }
    }

    val successes: List[(String, Option[String])] = formatted.collect { case Right(s) => s }
    val failures: List[FailureDetails.AdapterFailure] = formatted.collect { case Left(f) => f }

    (successes, failures) match {
      case (s :: ss, Nil) => (s :: ss).toMap.asRight // No Failures collected.
      case (_, f :: fs) => NonEmptyList.of(f, fs: _*).asLeft // Some Failures, return only those.
      case (Nil, Nil) =>
        val msg = "empty querystring: nothing to process"
        NonEmptyList
          .one(FailureDetails.AdapterFailure.InputData("querystring", none, msg))
          .asLeft
    }
  }

  /**
   * Returns an updated Pingdom Event JSON where the "action" field has been removed
   * @param json The event JSON which we need to update values for
   * @return the updated JSON without the "action" field included
   */
  private[registry] def reformatParameters(json: Json): Json =
    json.hcursor.downField("action").delete.top.getOrElse(json)
}
