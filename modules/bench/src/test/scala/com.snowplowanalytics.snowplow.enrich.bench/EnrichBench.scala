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
package com.snowplowanalytics.snowplow.enrich.bench

import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit

import cats.effect.{ContextShift, IO, Clock, Blocker}

import io.circe.literal._

import fs2.Stream

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.loaders.ThriftLoader
import com.snowplowanalytics.snowplow.enrich.pubsub.test.TestEnvironment
import com.snowplowanalytics.snowplow.enrich.pubsub.{Enrich, Environment, EnrichSpec, Payload}

import org.apache.http.message.BasicNameValuePair


/**
 * @example
 * {{{
 *   jmh:run -i 15 -wi 10 -f1 -t1 EnrichBench
 * }}}
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class EnrichBench {

  implicit val ioClock: Clock[IO] = Clock.create[IO]

  val client = Client.parseDefault[IO](json"""
      {
        "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1",
        "data": {
          "cacheSize": 500,
          "repositories": [
            {
              "name": "Iglu Central",
              "priority": 0,
              "vendorPrefixes": [ "com.snowplowanalytics" ],
              "connection": {
                "http": {
                  "uri": "http://iglucentral.com"
                }
              }
            },
            {
              "name": "Iglu Central - GCP Mirror",
              "priority": 1,
              "vendorPrefixes": [ "com.snowplowanalytics" ],
              "connection": {
                "http": {
                  "uri": "http://mirror01.iglucentral.com"
                }
              }
            }
          ]
        }
      }
      """).rethrowT.unsafeRunSync()

  @Benchmark
  def measureEnrichWithMinimalPayload(state: EnrichBench.BenchState) = {
    implicit val CS: ContextShift[IO] = state.contextShift
    Enrich.enrichWith[IO](IO.pure(EnrichmentRegistry()), client, None, (_: Option[Long]) => IO.unit)(state.raw).unsafeRunSync()
  }

  @Benchmark
  def measureToCollectorPayload(state: EnrichBench.BenchState) = {
    ThriftLoader.toCollectorPayload(state.raw.data, Enrich.processor)
  }

  @Benchmark
  @OperationsPerInvocation(50)  // 5 events repetated 10 times
  def measureRunWithNoEnrichments(state: EnrichBench.BenchState) = {
    // We used this benchmark to check if running the whole `enrichWith` on a blocking
    // thread-pool will give us increase in performance. Results haven't confirm it:
    // EnrichBench.measureRunWithNoEnrichments  avgt   15  341.144 ± 18.884  us/op <- smaller blocker
    // EnrichBench.measureRunWithNoEnrichments  avgt   15  326.608 ± 16.714  us/op <- wrapping blocker
    // EnrichBench.measureRunWithNoEnrichments  avgt   15  292.907 ± 15.894  us/op <- no blocker at all
    // However, I'm still leaving the "smaller blocker" in a hope that with actual IO enrichments
    // it will give the expected increase in performance
    implicit val CS: ContextShift[IO] = state.contextShift
    state.useEnvironment(e => Enrich.run[IO](e).compile.drain).unsafeRunSync()
  }
}

object EnrichBench {
  @State(Scope.Benchmark)
  class BenchState {
    var raw: Payload[IO, Array[Byte]] = _
    var useEnvironment: (Environment[IO] => IO[Unit]) => IO[Unit] = _
    var contextShift: ContextShift[IO] = _
    var blocker: Blocker = _

    @Setup(Level.Trial)
    def setup(): Unit = {

      raw = EnrichSpec.payload[IO]

      val input = Stream.emits(List(
        EnrichSpec.collectorPayload.copy(
          querystring = new BasicNameValuePair("ip", "125.12.2.40") :: EnrichSpec.querystring
        ),
        EnrichSpec.collectorPayload.copy(
          querystring = new BasicNameValuePair("ip", "125.12.2.41") :: EnrichSpec.querystring
        ),
        EnrichSpec.collectorPayload.copy(
          querystring = new BasicNameValuePair("ip", "125.12.2.42") :: EnrichSpec.querystring
        ),
        EnrichSpec.collectorPayload.copy(
          querystring = new BasicNameValuePair("ip", "125.12.2.43") :: EnrichSpec.querystring
        ),
        EnrichSpec.collectorPayload.copy(
          querystring = new BasicNameValuePair("ip", "125.12.2.44") :: EnrichSpec.querystring
        ),
      )).repeatN(10).map(cp => Payload(cp.toRaw, IO.unit)).covary[IO]

      useEnvironment = TestEnvironment.make(input).map(_.env).use(_: Environment[IO] => IO[Unit])

      contextShift = IO.contextShift(scala.concurrent.ExecutionContext.global)

      blocker = Blocker[IO].use(IO.pure).unsafeRunSync()
    }
  }
}
