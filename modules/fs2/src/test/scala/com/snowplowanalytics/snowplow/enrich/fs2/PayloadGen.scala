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

import java.nio.file.{Files, Path, Paths}
import java.util.Base64
import cats.effect.{Blocker, IO}
import cats.effect.concurrent.Ref
import _root_.io.circe.literal._
import _root_.io.circe.Json
import fs2.{Chunk, Stream}
import fs2.io.file.{createDirectory, writeAll}
import org.apache.http.message.BasicNameValuePair
import org.joda.time.{DateTimeZone, LocalDate}
import org.scalacheck.{Arbitrary, Gen}
import cats.effect.testing.specs2.CatsIO
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

object PayloadGen extends CatsIO {

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

  val optimizelyGen: Gen[Json] = for {
    id <- Gen.uuid.map(_.toString)
    name <- Gen.oneOf("pavlov", "marshmallow", "schiehallion")
    payload = json"""{"id":$id, "name":$name}"""
    schemaKey = "iglu:com.optimizely/experiment/jsonschema/1-0-0"
  } yield json"""{"schema":$schemaKey, "data": $payload}"""

  val consentGen: Gen[Json] = for {
    id <- Gen.uuid.map(_.toString)
    version <- Gen.oneOf((1 to 5).map(i => s"$i"))
    payload = json"""{"id":$id, "version":$version}"""
    schemaKey = "iglu:com.snowplowanalytics.snowplow/consent_document/jsonschema/1-0-0"
  } yield json"""{"schema":$schemaKey, "data": $payload}"""

  val mobileGen: Gen[Json] = for {
    osType <- Gen.oneOf("ios", "tvos", "watchos", "osx")
    osVersion <- Gen.oneOf((4 to 10).map(i => s"$i"))
    deviceManufacturer <- Gen.const("Apple Inc.")
    deviceModel <- Gen.oneOf("iPhone", "iPad")
    payload = json"""{"osType":$osType, "osVersion":$osVersion, "deviceManufacturer":$deviceManufacturer, "deviceModel": $deviceModel }"""
    schemaKey = "iglu:com.snowplowanalytics.snowplow/mobile_context/jsonschema/1-0-1"
  } yield json"""{"schema":$schemaKey, "data": $payload}"""

  val mobileScreenGen: Gen[Json] = for {
    name <- Gen.oneOf("home", "checkout", "search", "profile")
    id <- Gen.uuid.map(_.toString)
    payload = json"""{"name":$name, "id":$id}"""
    schemaKey = "iglu:com.snowplowanalytics.mobile/screen/jsonschema/1-0-0"
  } yield json"""{"schema":$schemaKey, "data": $payload}"""

  val mobileAppGen: Gen[Json] = for {
    major <- Gen.choose(1, 20)
    minor <- Gen.choose(0, 20)
    patch <- Gen.choose(0, 50)
    preRelease <- Gen.oneOf("", "-alpha", "-beta", "-beta.1", "-alpha.2")
    version = s"$major.$minor.$patch$preRelease"
    buildMetadata <- Gen.oneOf("", "+002", "+2013013144700", "+exp.sha.51f85", "+21LF2616")
    payload = json"""{"version":$version, "build":$buildMetadata}"""
    schemaKey = "iglu:com.snowplowanalytics.mobile/application/jsonschema/1-0-0"
  } yield json"""{"schema":$schemaKey, "data": $payload}"""

  val geolocationGen: Gen[Json] = for {
    latitude <- Gen.choose(-90.0, 90.0)
    longitude <- Gen.choose(-180.0, 180.0)
    payload = json"""{"latitude":$latitude,"longitude":$longitude}"""
    schemaKey = "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0"
  } yield json"""{"schema":$schemaKey, "data": $payload}"""

  val contextsGen: Gen[Json] = for {
    geo <- Gen.option(geolocationGen).map(_.toList)
    mobileApp <- Gen.option(mobileAppGen).map(_.toList)
    mobileScreen <- Gen.option(mobileScreenGen).map(_.toList)
    mobile <- Gen.option(mobileGen).map(_.toList)
    consent <- Gen.option(consentGen).map(_.toList)
    optimizely <- Gen.option(optimizelyGen).map(_.toList)
    cx = geo ::: mobileApp ::: mobileScreen ::: mobile ::: consent ::: optimizely
    schemaKey = "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1"
  } yield json"""{"schema":$schemaKey, "data": $cx}"""

  val localDateGen: Gen[LocalDate] = for {
    timeInMillis <- Gen.choose[Long](1586093286000L, 1646486886000L)
  } yield new LocalDate(timeInMillis)
  val ipGen: Gen[String] = for {
    part1 <- Gen.choose(2, 255)
    part2 <- Gen.choose(0, 255)
    part3 <- Gen.choose(0, 255)
    part4 <- Gen.choose(0, 255)
  } yield s"$part1.$part2.$part3.$part4"
  val contextGen: Gen[CollectorPayload.Context] = for {
    timestamp <- localDateGen.map(_.toDateTimeAtStartOfDay(DateTimeZone.UTC)).map(Option.apply)
    ip <- Gen.option(ipGen)
    userAgent <- userAgentGen.map(x => Some(x))
    userId <- Gen.option(Gen.uuid)
  } yield CollectorPayload.Context(timestamp, ip, userAgent, None, List(), userId)

  val getPageView: Gen[CollectorPayload] = for {
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

  val payloadStream: Stream[IO, CollectorPayload] = Stream.repeatEval(IO(getPageView.sample)).collect {
    case Some(x) => x
  }

  def write(dir: Path, cardinality: Long): IO[Unit] =
    for {
      counter <- Ref.of[IO, Int](0)
      dir <- Blocker[IO].use(b => createDirectory[IO](b, dir))
      filename = counter.updateAndGet(_ + 1).map(i => Paths.get(s"${dir.toAbsolutePath}/${i / 10000}/payload.$i.thrift"))
      _ <- Blocker[IO].use { b =>
             val result =
               for {
                 payload <- payloadStream.take(cardinality)
                 fileName <- Stream.eval(filename)
                 _ = if (!Files.exists(fileName.getParent)) Files.createDirectories(fileName.getParent)
                 _ <- Stream.chunk(Chunk.bytes(payload.toRaw)).through(writeAll[IO](fileName, b))
               } yield ()
             result.compile.drain
           }
    } yield ()
}
