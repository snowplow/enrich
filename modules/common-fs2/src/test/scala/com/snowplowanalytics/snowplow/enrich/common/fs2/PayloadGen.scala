/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2

import java.util.Base64

import cats.effect.IO
import cats.effect.kernel.Ref

import cats.effect.testing.specs2.CatsEffect

import _root_.io.circe.literal._

import fs2.Stream
import fs2.io.file.{Files, Path}

import org.apache.http.message.BasicNameValuePair

import org.joda.time.{DateTimeZone, LocalDate}

import org.scalacheck.{Arbitrary, Gen}

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

object PayloadGen extends CatsEffect {

  val api: CollectorPayload.Api =
    CollectorPayload.Api("com.snowplowanalytics.snowplow", "tp2")
  val source: CollectorPayload.Source =
    CollectorPayload.Source("ssc-0.0.0-test", "UTF-8", Some("collector.snplow.net"))

  val userAgentGen: Gen[String] = for {
    os <- Gen.oneOf("Windows NT 10.0; Win64; x64",
                    "Windows NT 5.1; rv:7.0.1",
                    "Macintosh; Intel Mac OS X 10_14_5",
                    "Macintosh; Intel Mac OS X 10_15_4"
          )
    engine <- Gen.oneOf("AppleWebKit/603.3.8 (KHTML, like Gecko)",
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169",
                        "AppleWebKit/605.1.15 (KHTML, like Gecko)"
              )
    version <- Gen.oneOf("Version/11.1.2 Safari/605.1.15", "Chrome/60.0.3112.113 Safari/537.36", "Gecko/20100101 Firefox/40.1")
  } yield s"Mozilla/5.0 ($os) $engine $version"

  val geolocationGen = for {
    latitude <- Gen.choose(-90.0, 90.0)
    longitude <- Gen.choose(-180.0, 180.0)
    payload = json"""{"latitude":$latitude,"longitude":$longitude}"""
    schemaKey = "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0"
  } yield json"""{"schema":$schemaKey, "data": $payload}"""
  val contextsGen = for {
    geo <- Gen.option(geolocationGen).map(_.toList)
    schemaKey = "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1"
  } yield json"""{"schema":$schemaKey, "data": $geo}"""

  val localDateGen: Gen[LocalDate] = Gen.calendar.map(LocalDate.fromCalendarFields).suchThat(_.year().get() < 3000)
  val ipGen: Gen[String] = for {
    part1 <- Gen.choose(2, 255)
    part2 <- Gen.choose(0, 255)
    part3 <- Gen.choose(0, 255)
    part4 <- Gen.choose(0, 255)
  } yield s"$part1.$part2.$part3.$part4"
  val contextGen: Gen[CollectorPayload.Context] = for {
    timestamp <- localDateGen.map(_.toDateTimeAtStartOfDay(DateTimeZone.UTC))
    ip <- Gen.option(ipGen)
    userAgent <- userAgentGen.map(x => Some(x))
    userId <- Gen.option(Gen.uuid)
  } yield CollectorPayload.Context(timestamp, ip, userAgent, None, List(), userId)

  val getPageView = for {
    eventId <- Gen.uuid
    aid <- Gen.oneOf("test-app", "scalacheck")
    cx <- contextsGen.map(json => Base64.getEncoder.encodeToString(json.noSpaces.getBytes))
    querystring = List(
                    new BasicNameValuePair("aid", aid),
                    new BasicNameValuePair("e", "pv"),
                    new BasicNameValuePair("eid", eventId.toString),
                    new BasicNameValuePair("cx", cx)
                  )
    context <- contextGen
  } yield CollectorPayload(api, querystring, None, None, source, context)

  val getPageViewArbitrary: Arbitrary[CollectorPayload] = Arbitrary.apply(getPageView)

  val payloadStream = Stream.repeatEval(IO(getPageView.sample)).collect {
    case Some(x) => x
  }

  def write(dir: Path, cardinality: Long): IO[Unit] =
    for {
      counter <- Ref.of[IO, Int](0)
      _ <- Files[IO].createDirectories(dir)
      filename = counter.updateAndGet(_ + 1).map(i => Path(s"$dir/payload.$i.thrift"))
      write = for {
                payload <- payloadStream.take(cardinality)
                fileName <- Stream.eval(filename)
                _ <- Stream(payload.toRaw: _*).through(Files[IO].writeAll(fileName)).as(())
              } yield ()
      _ <- write.compile.drain
    } yield ()
}
