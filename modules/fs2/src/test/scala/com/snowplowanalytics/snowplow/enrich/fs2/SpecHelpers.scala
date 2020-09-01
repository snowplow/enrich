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

import scala.concurrent.duration.TimeUnit

import cats.effect.{Blocker, Clock, ContextShift, IO, Resource, Timer}

import _root_.io.circe.literal._

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry

object SpecHelpers {
  implicit val ioClock: Clock[IO] =
    Clock.create[IO]

  val StaticTime = 1599750938180L

  implicit val staticIoClock: Clock[IO] =
    new Clock[IO] {
      def realTime(unit: TimeUnit): IO[Long] = IO.pure(StaticTime)
      def monotonic(unit: TimeUnit): IO[Long] = IO.pure(StaticTime)
    }

  val blocker: Resource[IO, Blocker] = Blocker[IO]

  implicit def CS: ContextShift[IO] = IO.contextShift(concurrent.ExecutionContext.global)

  implicit val T: Timer[IO] = IO.timer(concurrent.ExecutionContext.global)

  val resolverConfig = json"""
    {
      "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-2",
      "data": {
        "cacheSize": 500,
        "cacheTtl": 600,
        "repositories": [ {
            "name": "Iglu Central",
            "priority": 1,
            "vendorPrefixes": [],
            "connection": {
              "http": { "uri": "http://iglucentral.com" }
            }
          }
        ]
      }
    }"""

  val igluClient = Client.parseDefault[IO](resolverConfig).value.flatMap {
    case Right(client) => IO.pure(client)
    case Left(error) => IO.raiseError(new RuntimeException(error))
  }

  val enrichmentReg: EnrichmentRegistry[IO] = EnrichmentRegistry[IO]()
}
