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
package com.snowplowanalytics.snowplow.enrich.common.fs2.config

import java.net.URI
import scala.concurrent.duration._

import cats.effect.IO

import cats.effect.testing.specs2.CatsIO

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

class ParsedConfigsSpec extends Specification with CatsIO {
  "validateConfig" should {
    "return accumulated invalid attributes in config file" in {

      val invalidAttr1 = "invalidAttr1"
      val invalidAttr2 = "invalidAttr2"

      val configFile = ConfigFile(
        io.Input.PubSub("projects/test-project/subscriptions/inputSub", None, None),
        io.Outputs(
          io.Output.PubSub("projects/test-project/topics/good-topic", Some(Set("app_id", invalidAttr1)), None, None, None, None),
          Some(io.Output.PubSub("projects/test-project/topics/pii-topic", Some(Set("app_id", invalidAttr2)), None, None, None, None)),
          io.Output.PubSub("projects/test-project/topics/bad-topic", None, None, None, None, None)
        ),
        io.Concurrency(10000, 64),
        Some(7.days),
        Some(
          io.Monitoring(
            Some(Sentry(URI.create("http://sentry.acme.com"))),
            Some(
              io.MetricsReporters(
                Some(io.MetricsReporters.StatsD("localhost", 8125, Map("app" -> "enrich"), 10.seconds, None)),
                Some(io.MetricsReporters.Stdout(10.seconds, None)),
                None
              )
            )
          )
        ),
        io.Telemetry(
          false,
          15.minutes,
          "POST",
          "collector-g.snowplowanalytics.com",
          443,
          true,
          Some("my_pipeline"),
          Some("hfy67e5ydhtrd"),
          Some("665bhft5u6udjf"),
          Some("enrich-kinesis-ce"),
          Some("1.0.0")
        )
      )

      ParsedConfigs.validateConfig[IO](configFile).value.map(result => result must beLeft)
    }
  }

  "outputAttributes" should {
    "fetch attribute values" in {
      val output = io.Output.PubSub("projects/test-project/topics/good-topic", Some(Set("app_id")), None, None, None, None)
      val ee = new EnrichedEvent()
      ee.app_id = "test_app"

      val result = ParsedConfigs.outputAttributes(output)(ee)

      result must haveSize(1)
      result must haveKey("app_id")
      result must haveValue("test_app")
    }
  }
}
