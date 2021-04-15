/*
 * Copyright (c) 2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.fs2.io

import com.codahale.metrics._

import com.snowplowanalytics.snowplow.enrich.fs2.config.io.MetricsReporter
import scala.concurrent.duration.DurationLong

import org.specs2.mutable.Specification

class StatsDReporterSpec extends Specification {
  import StatsDReporterSpec._

  "StatsDeporter" should {
    "report dropwizard gauges" in {
      val registry = new MetricRegistry()

      registry.register("gauge1", fixedGauge(42))

      val result = StatsDReporter.serializedMetrics(registry, TestConfig)

      result must contain(exactly("snowplow.test.gauge1:42|g|#tag1:abc"))
    }

    "report dropwizard counters" in {
      val registry = new MetricRegistry()

      val counter = registry.counter("counter1")
      (1 to 42).foreach(_ => counter.inc())

      val result = StatsDReporter.serializedMetrics(registry, TestConfig)

      result must contain(exactly("snowplow.test.counter1.count:42|g|#tag1:abc"))
    }

    "report dropwizard histograms" in {
      val registry = new MetricRegistry()

      val histogram = registry.histogram("hist1")
      histogram.update(10)
      histogram.update(20)

      val result = StatsDReporter.serializedMetrics(registry, TestConfig)

      result must contain(
        exactly(
          "snowplow.test.hist1.count:2|g|#tag1:abc",
          "snowplow.test.hist1.min:10|g|#tag1:abc",
          "snowplow.test.hist1.max:20|g|#tag1:abc",
          "snowplow.test.hist1.mean:15.0|g|#tag1:abc",
          "snowplow.test.hist1.median:20.0|g|#tag1:abc",
          "snowplow.test.hist1.stdDev:5.0|g|#tag1:abc",
          "snowplow.test.hist1.size:2|g|#tag1:abc",
          beMatching("snowplow\\.test\\.hist1\\.75thPercentile:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.hist1\\.95thPercentile:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.hist1\\.98thPercentile:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.hist1\\.99thPercentile:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.hist1\\.999thPercentile:.*|g|#tag1:abc")
        )
      )
    }

    "report dropwizard meters" in {
      val registry = new MetricRegistry()

      val meter = registry.meter("meter1")
      meter.mark()
      meter.mark()

      val result = StatsDReporter.serializedMetrics(registry, TestConfig)

      result must contain(
        exactly(
          "snowplow.test.meter1.count:2|g|#tag1:abc",
          beMatching("snowplow\\.test\\.meter1\\.fifteenMinuteRate:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.meter1\\.fiveMinuteRate:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.meter1\\.oneMinuteRate:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.meter1\\.meanRate:.*|g|#tag1:abc")
        )
      )
    }

    "report dropwizard timers" in {
      val registry = new MetricRegistry()

      val timer = registry.timer("timer1")
      timer.time({ () => () }: java.util.concurrent.Callable[Unit])

      val result = StatsDReporter.serializedMetrics(registry, TestConfig)

      result must contain(
        allOf(
          "snowplow.test.timer1.count:1|g|#tag1:abc",
          beMatching("snowplow\\.test\\.timer1\\.fifteenMinuteRate:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.timer1\\.fiveMinuteRate:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.timer1\\.oneMinuteRate:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.timer1\\.meanRate:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.timer1\\.min:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.timer1\\.max:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.timer1\\.mean:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.timer1\\.median:.*|g|#tag1:abc"),
          "snowplow.test.timer1.stdDev:0.0|g|#tag1:abc",
          "snowplow.test.timer1.size:1|g|#tag1:abc",
          beMatching("snowplow\\.test\\.timer1\\.75thPercentile:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.timer1\\.95thPercentile:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.timer1\\.98thPercentile:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.timer1\\.99thPercentile:.*|g|#tag1:abc"),
          beMatching("snowplow\\.test\\.timer1\\.999thPercentile:.*|g|#tag1:abc")
        )
      )
    }
  }

}

object StatsDReporterSpec {

  val TestConfig = MetricsReporter.StatsD("localhost", 8125, Map("tag1" -> "abc"), 1.second, Some("snowplow.test."))

  def fixedGauge[T](v: T): Gauge[T] =
    new Gauge[T] {
      def getValue: T = v
    }

}
