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
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry.snowplow

import cats.Monad
import cats.data.NonEmptyList
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.validated._

import cats.effect.Clock

import io.circe._
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter.Adapted
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.{JsonUtils => JU, ConversionUtils => CU}

/**
 * The Redirect Adapter is essentially a pre-processor for
 * Snowplow Tracker Protocol v2 above (although it doesn't
 * use the TP2 code above directly).
 * The &u= parameter used for a redirect is converted into
 * a URI Redirect entity and then either stored as an
 * unstructured event, added to an existing contexts array
 * or used to initialize a new contexts array.
 */
object RedirectAdapter extends Adapter {

  // Tracker version for an Iglu-compatible webhook
  private val TrackerVersion = "r-tp2"

  // Our default tracker platform
  private val TrackerPlatform = "web"

  // Schema for a URI redirect. Could end up being an event or a context
  // depending on what else is in the payload
  val UriRedirect =
    SchemaKey(
      "com.snowplowanalytics.snowplow",
      "uri_redirect",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )

  /**
   * Converts a CollectorPayload instance into raw events. Assumes we have a GET querystring with
   * a u parameter for the URI redirect and other parameters per the Snowplow Tracker Protocol.
   * @param payload The CollectorPaylod containing one or more raw events as collected by a
   * Snowplow collector
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
    val originalParams = toMap(payload.querystring)
    if (originalParams.isEmpty) {
      val msg = "empty querystring: not a valid URI redirect"
      val failure = FailureDetails.TrackerProtocolViolation.InputData(
        "querystring",
        none,
        msg
      )
      Monad[F].pure(failure.invalidNel)
    } else
      originalParams.get("u") match {
        case Some(Some(u)) =>
          val json = buildUriRedirect(u)
          val newParams: Either[FailureDetails.TrackerProtocolViolation, Map[String, Option[String]]] =
            (if (originalParams.contains("e")) {
               // Already have an event so add the URI redirect as a context (more fiddly)
               def newCo = Map("co" -> toContext(json).noSpaces)
               (originalParams.get("cx"), originalParams.get("co")) match {
                 case (None, None) => newCo.asRight
                 case (None, Some(Some(co))) if co == "" => newCo.asRight
                 case (None, Some(Some(co))) => addToExistingCo(json, co, maxJsonDepth).map(str => Map("co" -> str))
                 case (Some(Some(cx)), _) => addToExistingCx(json, cx, maxJsonDepth).map(str => Map("cx" -> str))
               }
             } else
               // Add URI redirect as an unstructured event
               Map("e" -> "ue", "ue_pr" -> toUnstructEvent(json).noSpaces).asRight)
              .map(_.map { case (k, v) => (k, Option(v)) })

          val fixedParams = Map(
            "tv" -> Some(TrackerVersion),
            "p" -> originalParams.getOrElse("p", Some(TrackerPlatform)) // Required field
          )

          Monad[F].pure((for {
            np <- newParams
            ev = NonEmptyList.one(
                   RawEvent(
                     api = payload.api,
                     parameters = (originalParams - "u") ++ np ++ fixedParams,
                     contentType = payload.contentType,
                     source = payload.source,
                     context = payload.context
                   )
                 )
          } yield ev).leftMap(e => NonEmptyList.one(e)).toValidated)
        case _ =>
          val msg = "missing `u` parameter: not a valid URI redirect"
          val qs = originalParams.map(t => s"${t._1}=${t._2.getOrElse("null")}").mkString("&")
          val failure =
            FailureDetails.TrackerProtocolViolation.InputData(
              "querystring",
              qs.some,
              msg
            )
          Monad[F].pure(failure.invalidNel)
      }
  }

  /**
   * Builds a self-describing JSON representing a URI redirect entity.
   * @param uri The URI we are redirecting to
   * @return a URI redirect as a self-describing JValue
   */
  private def buildUriRedirect(uri: String): SelfDescribingData[Json] =
    SelfDescribingData(UriRedirect, Json.obj("uri" := uri))

  /**
   * Adds a context to an existing non-Base64-encoded self-describing contexts stringified JSON.
   * Does the minimal amount of validation required to ensure the context can be safely added, or
   * returns a Failure.
   * @param newContext The context to add to the existing list of contexts
   * @param existing The existing contexts as a non-Base64-encoded stringified JSON
   * @return an updated non-Base64-encoded self-describing contexts stringified JSON
   */
  private def addToExistingCo(
    newContext: SelfDescribingData[Json],
    existing: String,
    maxJsonDepth: Int
  ): Either[FailureDetails.TrackerProtocolViolation, String] =
    for {
      json <- JU
                .extractJson(existing, maxJsonDepth) // co|cx
                .leftMap(e =>
                  FailureDetails.TrackerProtocolViolation
                    .NotJson("co|cx", existing.some, e)
                )
      merged = json.hcursor
                 .downField("data")
                 .withFocus(_.mapArray(newContext.asJson +: _))
                 .top
                 .getOrElse(json)
    } yield merged.noSpaces

  /**
   * Adds a context to an existing Base64-encoded self-describing contexts stringified JSON.
   * Does the minimal amount of validation required to ensure the context can be safely added, or
   * returns a Failure.
   * @param newContext The context to add to the existing list of contexts
   * @param existing The existing contexts as a non-Base64-encoded stringified JSON
   * @return an updated non-Base64-encoded self-describing contexts stringified JSON
   */
  private def addToExistingCx(
    newContext: SelfDescribingData[Json],
    existing: String,
    maxJsonDepth: Int
  ): Either[FailureDetails.TrackerProtocolViolation, String] =
    for {
      decoded <- CU
                   .decodeBase64Url(existing) // cx
                   .leftMap(e =>
                     FailureDetails.TrackerProtocolViolation
                       .InputData("cx", existing.some, e)
                   )
      added <- addToExistingCo(newContext, decoded, maxJsonDepth)
      recoded = CU.encodeBase64Url(added)
    } yield recoded

}
