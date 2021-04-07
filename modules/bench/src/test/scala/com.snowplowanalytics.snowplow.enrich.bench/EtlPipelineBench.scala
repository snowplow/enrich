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

import cats.Id
import cats.data.Validated

import cats.effect.{IO, Clock}

import io.circe.Json

import com.snowplowanalytics.iglu.client.{Resolver, Client, CirceValidator}

import com.snowplowanalytics.snowplow.enrich.common.EtlPipeline
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry

import com.snowplowanalytics.snowplow.enrich.fs2.{Enrich, EnrichSpec}

import org.joda.time.DateTime

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class EtlPipelineBench {

  private implicit val ioClock: Clock[IO] = Clock.create[IO]

  private implicit val idClock: Clock[Id] = new Clock[Id] {
    final def realTime(unit: TimeUnit): Id[Long] =
      unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
    final def monotonic(unit: TimeUnit): Id[Long] =
      unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS)
  }

  @Benchmark
  def measureProcessEventsIO(state: EtlPipelineBench.BenchState) = {
    val payload = EnrichSpec.colllectorPayload
    EtlPipeline.processEvents[IO](state.adapterRegistry, state.enrichmentRegistryIo, Client.IgluCentral, Enrich.processor, state.dateTime, Validated.Valid(Some(payload))).unsafeRunSync()
  }

  @Benchmark
  def measureProcessEventsId(state: EtlPipelineBench.BenchState) = {
    val payload = EnrichSpec.colllectorPayload
    EtlPipeline.processEvents[Id](state.adapterRegistry, state.enrichmentRegistryId, state.clientId, Enrich.processor, state.dateTime, Validated.Valid(Some(payload)))
  }
}

object EtlPipelineBench {


  @State(Scope.Benchmark)
  class BenchState {
    var dateTime: DateTime = _
    var adapterRegistry: AdapterRegistry = _
    var enrichmentRegistryId: EnrichmentRegistry[Id] = _
    var enrichmentRegistryIo: EnrichmentRegistry[IO] = _
    var clientId: Client[Id, Json] = _
    var clientIO: Client[IO, Json] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      dateTime = DateTime.parse("2010-06-30T01:20+02:00")
      adapterRegistry = new AdapterRegistry()
      enrichmentRegistryId = EnrichmentRegistry[Id]()
      enrichmentRegistryIo = EnrichmentRegistry[IO]()
      clientId = Client[Id, Json](Resolver(List(), None), CirceValidator)
      clientIO = Client[IO, Json](Resolver(List(), None), CirceValidator)
    }
  }
}
