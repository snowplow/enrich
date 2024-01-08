/*
 * Copyright (c) 2021-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.io

import scala.concurrent.duration.DurationLong

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.MetricsReporters

class StatsDReporterSpec extends Specification {
  val TestConfig = MetricsReporters.StatsD("localhost", 8125, Map("tag1" -> "abc"), 1.second, Some("snowplow.test."))

  "StatsDeporter" should {
    "serialize metrics" in {
      val snapshot = Metrics.MetricSnapshot(Some(10000L), 10, 20, 30, 0, Some(40), Some(0), Some(0))

      val result = StatsDReporter.serializedMetrics(snapshot, TestConfig)

      result must contain(
        exactly(
          "snowplow.test.raw:10|c|#tag1:abc",
          "snowplow.test.good:20|c|#tag1:abc",
          "snowplow.test.bad:30|c|#tag1:abc",
          "snowplow.test.latency:10000|g|#tag1:abc",
          "snowplow.test.invalid_enriched:0|c|#tag1:abc",
          "snowplow.test.remote_adapters_success:40|c|#tag1:abc",
          "snowplow.test.remote_adapters_failure:0|c|#tag1:abc",
          "snowplow.test.remote_adapters_timeout:0|c|#tag1:abc"
        )
      )
    }

    "serialize metrics when latency is empty" in {
      val snapshot = Metrics.MetricSnapshot(None, 10, 20, 30, 40, Some(40), Some(0), Some(0))

      val result = StatsDReporter.serializedMetrics(snapshot, TestConfig)

      result must contain(
        exactly(
          "snowplow.test.raw:10|c|#tag1:abc",
          "snowplow.test.good:20|c|#tag1:abc",
          "snowplow.test.bad:30|c|#tag1:abc",
          "snowplow.test.invalid_enriched:40|c|#tag1:abc",
          "snowplow.test.remote_adapters_success:40|c|#tag1:abc",
          "snowplow.test.remote_adapters_failure:0|c|#tag1:abc",
          "snowplow.test.remote_adapters_timeout:0|c|#tag1:abc"
        )
      )
    }

    "serialize metrics when remote adapter metrics are empty" in {
      val snapshot = Metrics.MetricSnapshot(Some(10000L), 10, 20, 30, 40, None, None, None)

      val result = StatsDReporter.serializedMetrics(snapshot, TestConfig)

      result must contain(
        exactly(
          "snowplow.test.raw:10|c|#tag1:abc",
          "snowplow.test.good:20|c|#tag1:abc",
          "snowplow.test.bad:30|c|#tag1:abc",
          "snowplow.test.latency:10000|g|#tag1:abc",
          "snowplow.test.invalid_enriched:40|c|#tag1:abc"
        )
      )
    }
  }
}
