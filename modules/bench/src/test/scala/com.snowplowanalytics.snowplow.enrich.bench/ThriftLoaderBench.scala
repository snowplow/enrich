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
package com.snowplowanalytics.snowplow.enrich.bench

import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit

import com.snowplowanalytics.snowplow.enrich.common.loaders.ThriftLoader
import com.snowplowanalytics.snowplow.enrich.fs2.{Enrich, EnrichSpec}

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
class ThriftLoaderBench {

  @Benchmark
  def measureToCollectorPayload(state: EnrichBench.BenchState) =
    ThriftLoader.toCollectorPayload(state.raw.data, Enrich.processor)
}

object ThriftLoaderBench {
  @State(Scope.Benchmark)
  class BenchState {
    var data: Array[Byte] = _

    @Setup(Level.Trial)
    def setup(): Unit = {
      data = EnrichSpec.colllectorPayload.toRaw
    }
  }
}

