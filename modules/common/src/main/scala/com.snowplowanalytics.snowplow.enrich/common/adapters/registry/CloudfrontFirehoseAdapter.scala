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

import java.nio.charset.StandardCharsets
import java.util.Base64

import cats.Monad
import cats.data.NonEmptyList
import cats.syntax.either._
import cats.syntax.option._
import cats.syntax.traverse._
import cats.effect.Clock

import io.circe._

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils, JsonUtils}
import com.snowplowanalytics.snowplow.enrich.common.adapters._
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.Adapter.Adapted

/** Transforms CloudFront access logs sent through Firehose with JSON format into page view events. */
case class CloudfrontFirehoseAdapter() extends Adapter {

  private val ContentType = "application/json"

  override def toRawEvents[F[_]: Monad: Clock](
    payload: CollectorPayload,
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F],
    maxJsonDepth: Int
  ): F[Adapted] = {
    val _ = (client, registryLookup) // to remove warnings
    val result = for {
      bodyStr <- payload.bodyAsString
                   .toRight(
                     FailureDetails.AdapterFailure
                       .InputData("body", None, "empty body")
                   )
      _ <- payload.contentType match {
             case Some(`ContentType`) => ().asRight
             case other =>
               FailureDetails.AdapterFailure
                 .InputData("Content-Type", other, s"expected $ContentType")
                 .asLeft
           }
      json <- JsonUtils
                .extractJson(bodyStr, maxJsonDepth)
                .leftMap(e =>
                  FailureDetails.AdapterFailure
                    .InputData("body", bodyStr.some, e)
                )
      recordsJson <- json.hcursor
                       .downField("records")
                       .focus
                       .toRight(
                         FailureDetails.AdapterFailure
                           .InputData("body", bodyStr.some, "missing 'records' field")
                       )
      records <- recordsJson
                   .as[List[Json]]
                   .leftMap(e =>
                     FailureDetails.AdapterFailure
                       .InputData("records", recordsJson.noSpaces.some, s"'records' is not a JSON array: ${e.getMessage}")
                   )
      logEntries <- decodeRecords(records, maxJsonDepth)
      events <- toEvents(logEntries, payload, bodyStr)
    } yield events

    Monad[F].pure(result.toValidated.leftMap(NonEmptyList.one))
  }

  // Each record contains a `data` field that contains a JSON that is base64-encoded
  private def decodeRecords(
    records: List[Json],
    maxJsonDepth: Int
  ): Either[FailureDetails.AdapterFailure, List[Json]] =
    records.traverse { record =>
      for {
        data <- record.hcursor
                  .downField("data")
                  .as[String]
                  .leftMap(e =>
                    FailureDetails.AdapterFailure
                      .InputData("records[].data", record.noSpaces.some, s"missing or non-string 'data' field: ${e.getMessage}")
                  )
        bytes <- Either
                   .catchNonFatal(Base64.getDecoder.decode(data))
                   .leftMap(e =>
                     FailureDetails.AdapterFailure
                       .InputData("records[].data", data.some, s"invalid base64: ${e.getMessage}")
                   )
        json <- JsonUtils
                  .extractJson(new String(bytes, StandardCharsets.UTF_8), maxJsonDepth)
                  .leftMap(e =>
                    FailureDetails.AdapterFailure
                      .InputData("records[].data", data.some, e)
                  )
      } yield json
    }

  private def toEvents(
    logEntries: List[Json],
    payload: CollectorPayload,
    bodyStr: String
  ): Either[FailureDetails.AdapterFailure, NonEmptyList[RawEvent]] = {
    // `aid` lives on the collector URL (e.g. /com.amazon.aws.cloudfront/firehose?aid=cf-test)
    // and is the same for every record in the batch, so we lift it once.
    // Defaults to "cloudfront" when no `aid` is provided.
    val aid = toMap(payload.querystring).get("aid").flatten.getOrElse("cloudfront")
    logEntries.map(entry => toEvent(entry, payload, aid)) match {
      case head :: tail => NonEmptyList(head, tail).asRight
      case Nil =>
        FailureDetails.AdapterFailure
          .InputData("body", bodyStr.some, "no log entries found")
          .asLeft
    }
  }

  private def toEvent(
    entry: Json,
    payload: CollectorPayload,
    aid: String
  ): RawEvent = {
    val c = entry.hcursor

    def field(name: String): Option[String] =
      c.downField(name).as[String].toOption.filter(_ != "-")

    def longField(name: String): Option[Long] =
      field(name).flatMap(s => Either.catchNonFatal(s.toLong).toOption)

    val timestampMs = longField("timestamp(ms)")
    val csReferer = field("cs(Referer)")
    val csUserAgent = field("cs(User-Agent)")
    val xHostHeader = field("x-host-header")
    val csUriStem = field("cs-uri-stem")
    val csUriQuery = field("cs-uri-query")
    val cIp = field("c-ip")

    val pageUrl = xHostHeader.map { host =>
      val stem = csUriStem.getOrElse("/")
      val query = csUriQuery.map(q => s"?$q").getOrElse("")
      s"https://$host$stem$query"
    }

    val userAgent = csUserAgent.map(ConversionUtils.singleEncodePcts)

    val parameters: Map[String, Option[String]] = Map(
      "tv" -> Some(s"${payload.api.vendor}/${payload.api.version}"),
      "e" -> Some("pv"),
      "p" -> Some("srv"),
      "aid" -> Some(aid)
    ) ++
      pageUrl.map(u => "url" -> u.some) ++
      csReferer.map(r => "refr" -> decodeField(r).some) ++
      timestampMs.map(ms => "stm" -> ms.toString.some)

    RawEvent(
      api = payload.api,
      parameters = parameters,
      contentType = payload.contentType,
      source = payload.source,
      context = CollectorPayload.Context(payload.context.timestamp, cIp, userAgent, None, Nil, None)
    )
  }

  private def decodeField(field: String): String =
    ConversionUtils.doubleDecode(field).getOrElse(field)
}
