/*
 * Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.snowplow.enrich.common.loaders.ThriftLoader
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.pubsub.{Enrich, EnrichSpec}

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
class ThriftLoaderBench {

  @Benchmark
  def measureToCollectorPayload(state: ThriftLoaderBench.BenchState) =
    ThriftLoader.toCollectorPayload(state.data, Enrich.processor)

  @Benchmark
  def measureNormalize(state: ThriftLoaderBench.BenchState) = {
    Enrich.serializeEnriched(state.event)
  }
}

object ThriftLoaderBench {
  @State(Scope.Benchmark)
  class BenchState {
    var data: Array[Byte] = _
    var event: EnrichedEvent = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      data = EnrichSpec.collectorPayload.toRaw

      event = new EnrichedEvent()
      event.setApp_id("foo")
      event.setEvent_id("deadbeef-dead-dead-dead-deaddeafbeef")
      event.setUser_ipaddress("128.0.1.2")
      event.setUnstruct_event("""{"some": "json"}""")
    }
  }
}

