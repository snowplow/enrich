/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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

import scala.concurrent.duration._

import cats.Applicative
import cats.data.Validated
import cats.implicits._

import cats.effect.IO

import fs2.Stream

import _root_.io.circe.literal._

import org.apache.http.NameValuePair
import org.apache.http.message.BasicNameValuePair

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{Processor, BadRow, Payload => BadRowPayload}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.IpLookupsEnrichment
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.ConversionUtils

import com.snowplowanalytics.snowplow.enrich.fs2.EnrichSpec.{Expected, minimalEvent, normalizeResult}
import com.snowplowanalytics.snowplow.enrich.fs2.test._

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsIO

import org.specs2.scalacheck.Parameters

class EnrichSpec extends Specification with CatsIO with ScalaCheck {

  sequential

  "enrichWith" should {
    "enrich a minimal page_view CollectorPayload event without any enrichments enabled" in {
      val expected = minimalEvent
        .copy(
          etl_tstamp = Some(Instant.ofEpochMilli(SpecHelpers.StaticTime)),
          user_ipaddress = Some("175.16.199.0"),
          event = Some("page_view"),
          event_vendor = Some("com.snowplowanalytics.snowplow"),
          event_name = Some("page_view"),
          event_format = Some("jsonschema"),
          event_version = Some("1-0-0"),
          derived_tstamp = Some(Instant.ofEpochMilli(0L))
        )

      Enrich
        .enrichWith(TestEnvironment.enrichmentReg.pure[IO], TestEnvironment.igluClient, None, _ => IO.unit)(
          EnrichSpec.payload[IO]
        )
        .map(normalizeResult)
        .map {
          case List(Validated.Valid(event)) => event must beEqualTo(expected)
          case other => ko(s"Expected one valid event, got $other")
        }
    }

    "enrich a randomly generated page view event" in {
      implicit val cpGen = PayloadGen.getPageViewArbitrary
      prop { (collectorPayload: CollectorPayload) =>
        val payload = Payload(collectorPayload.toRaw, IO.unit)
        Enrich
          .enrichWith(TestEnvironment.enrichmentReg.pure[IO], TestEnvironment.igluClient, None, _ => IO.unit)(payload)
          .map(normalizeResult)
          .map {
            case List(Validated.Valid(e)) => e.event must beSome("page_view")
            case other => ko(s"Expected one valid event, got $other")
          }
      }.setParameters(Parameters(maxSize = 20, minTestsOk = 25))
    }
  }

  "enrich" should {
    "update metrics with raw, good and bad counters" in {
      val input = Stream.emits(List(Payload(Array.empty[Byte], IO.unit), EnrichSpec.payload[IO]))
      TestEnvironment.make(input).use { test =>
        // The sleep for 100 millis should not be necessary; but it seems to prevent
        // unexplained test failures in CI.
        val finalise = IO.sleep(100.millis) *> test.finalise()

        val enrichStream = Enrich.run[IO](test.env).onFinalize(finalise)
        val rows = test.bad.dequeue
          .either(test.good.dequeue)
          .concurrently(enrichStream)
        for {
          payloads <- rows.compile.toList
          counter <- test.counter.get
        } yield {
          counter.raw must_== 2L
          counter.good must_== 1L
          counter.bad must_== 1L
          payloads must be like {
            case List(Left(_), Right(_)) => ok
            case List(Right(_), Left(_)) => ok
            case other => ko(s"Expected one bad and one good row, got $other")
          }
        }
      }
    }

    "enrich event using refreshing MaxMind DB" in {
      // 4 enrichments can update assets: MaxMind, IAB, referer-parser, ua-parser
      val input = Stream(EnrichSpec.payload[IO]) ++ Stream.sleep_(2.seconds) ++ Stream(EnrichSpec.payload[IO])
      val ipLookupsConf = IpLookupsEnrichment
        .parse(
          json"""{
                    "name": "ip_lookups",
                    "vendor": "com.snowplowanalytics.snowplow",
                    "enabled": true,
                    "parameters": {
                      "geo": {
                        "database": "GeoIP2-City.mmdb",
                        "uri": "http://localhost:8080/maxmind"
                      }
                    }
                  }""",
          SchemaKey(
            "com.snowplowanalytics.snowplow",
            "ip_lookups",
            "jsonschema",
            SchemaVer.Full(2, 0, 0)
          ),
          false // Unlike in other tests we actually download it
        )
        .getOrElse(throw new RuntimeException("Invalid test configuration"))

      val one = Expected
        .copy(
          geo_country = Some("CN"),
          geo_region = Some("22"),
          geo_city = Some("Changchun"),
          geo_latitude = Some(43.88),
          geo_longitude = Some(125.3228),
          geo_region_name = Some("Jilin Sheng"),
          geo_timezone = Some("Asia/Harbin")
        )
      val two = one.copy(geo_city = Some("Baishan"))
      // Third one is Fuyu

      val assetsServer = HttpServer.resource(6.seconds)
      (assetsServer *> TestEnvironment.make(input, List(ipLookupsConf))).use { test =>
        test
          .run(_.copy(assetsUpdatePeriod = Some(1800.millis)))
          .map { events =>
            events must containTheSameElementsAs(List(Output.Good(one), Output.Good(two)))
          }
      }
    }
  }
}

object EnrichSpec {
  val eventId: UUID = UUID.fromString("deadbeef-dead-beef-dead-beefdead")

  val api: CollectorPayload.Api =
    CollectorPayload.Api("com.snowplowanalytics.snowplow", "tp2")
  val source: CollectorPayload.Source =
    CollectorPayload.Source("ssc-0.0.0-test", "UTF-8", Some("collector.snplow.net"))
  val context: CollectorPayload.Context = CollectorPayload.Context(None, Some("175.16.199.0"), None, None, List(), None)
  val querystring: List[NameValuePair] = List(
    new BasicNameValuePair("e", "pv"),
    new BasicNameValuePair("eid", eventId.toString)
  )
  val colllectorPayload: CollectorPayload = CollectorPayload(api, querystring, None, None, source, context)
  def payload[F[_]: Applicative]: Payload[F, Array[Byte]] =
    Payload(colllectorPayload.toRaw, Applicative[F].unit)

  def normalize(payload: String): Validated[BadRow, Event] =
    Event
      .parse(payload)
      .map(_.copy(etl_tstamp = Some(Instant.ofEpochMilli(SpecHelpers.StaticTime)))) match {
      case Validated.Valid(event) =>
        Validated.Valid(event)
      case Validated.Invalid(error) =>
        val rawPayload = BadRowPayload.RawPayload(payload)
        val badRow = BadRow.LoaderParsingError(Processor("fs2-enrich-test-suite", "x"), error, rawPayload)
        Validated.Invalid(badRow)
    }

  def normalizeResult(payload: Result[IO]): List[Validated[BadRow, Event]] =
    payload.data.map {
      case Validated.Valid(a) => normalize(ConversionUtils.tabSeparatedEnrichedEvent(a))
      case Validated.Invalid(e) => e.invalid
    }

  val minimalEvent = Event
    .minimal(
      EnrichSpec.eventId,
      Instant.ofEpochMilli(0L),
      "ssc-0.0.0-test",
      s"fs2-enrich-${generated.BuildInfo.version}-common-${generated.BuildInfo.version}"
    )

  val Expected = minimalEvent
    .copy(
      etl_tstamp = Some(Instant.ofEpochMilli(SpecHelpers.StaticTime)),
      user_ipaddress = Some("175.16.199.0"),
      event = Some("page_view"),
      event_vendor = Some("com.snowplowanalytics.snowplow"),
      event_name = Some("page_view"),
      event_format = Some("jsonschema"),
      event_version = Some("1-0-0"),
      derived_tstamp = Some(Instant.ofEpochMilli(0L))
    )
}
