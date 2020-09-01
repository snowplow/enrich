/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.fs2

import java.time.Instant
import java.util.UUID

import cats.Applicative
import cats.data.Validated
import cats.effect.IO

import _root_.io.circe.syntax._

import org.apache.http.message.BasicNameValuePair

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.fs2.SpecHelpers.staticIoClock

import org.specs2.mutable.Specification

class EnrichSpec extends Specification {

  "enrichWith" should {
    "enrich a minimal page_view CollectorPayload event without any enrichments enabled" in {
      val result = for {
        igluClient <- SpecHelpers.igluClient
        registry = SpecHelpers.enrichmentReg
        result <- Enrich.enrichWith(registry, igluClient, None)(EnrichSpec.payload[IO])
      } yield result.data.map(event => event.map(Enrich.encodeEvent))

      val Expected = Event
        .minimal(
          EnrichSpec.eventId,
          Instant.ofEpochMilli(0L),
          "ssc-0.0.0-test",
          "fs2-enrich-1.3.1-common-1.3.1"
        )
        .copy(
          etl_tstamp = Some(Instant.ofEpochMilli(SpecHelpers.StaticTime)),
          user_ipaddress = Some("127.10.1.3"),
          event = Some("page_view"),
          event_vendor = Some("com.snowplowanalytics.snowplow"),
          event_name = Some("page_view"),
          event_format = Some("jsonschema"),
          event_version = Some("1-0-0"),
          derived_tstamp = Some(Instant.ofEpochMilli(0L))
        )

      result.unsafeRunSync() must be like {
        case List(Validated.Valid(tsv)) =>
          Event.parse(tsv) match {
            case Validated.Valid(Expected) => ok
            case Validated.Valid(event) => ko(s"Event is valid, but event:\n ${event.asJson}\ndoesn't match expected:\n${Expected.asJson}")
            case Validated.Invalid(error) => ko(s"Analytics SDK cannot parse the output. $error")
          }
        case _ => ko("Expected one valid event")
      }
    }
  }
}

object EnrichSpec {
  val eventId = UUID.fromString("deadbeef-dead-beef-dead-beefdead")

  val api = CollectorPayload.Api("com.snowplowanalytics.snowplow", "tp2")
  val source = CollectorPayload.Source("ssc-0.0.0-test", "UTF-8", Some("collector.snplow.net"))
  val context = CollectorPayload.Context(None, Some("127.10.1.3"), None, None, List(), None)
  val querystring = List(
    new BasicNameValuePair("e", "pv"),
    new BasicNameValuePair("eid", eventId.toString)
  )
  val colllectorPayload = CollectorPayload(api, querystring, None, None, source, context)
  def payload[F[_]: Applicative] = Payload(colllectorPayload.toRaw, Applicative[F].unit)
}
