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
package com.snowplowanalytics.snowplow.enrich.pubsub.config

import java.net.URI
import java.nio.file.Paths

import scala.concurrent.duration._

import cats.syntax.either._

import cats.effect.IO

import _root_.io.circe.literal._

import org.specs2.mutable.Specification
import cats.effect.testing.specs2.CatsIO
import com.snowplowanalytics.snowplow.enrich.pubsub.Environment

class ConfigFileSpec extends Specification with CatsIO {
  "parse" should {
    "parse valid HOCON file with path provided" in {
      val configPath = Paths.get(getClass.getResource("/config.pubsub.hocon.sample").toURI)
      val expected = ConfigFile(
        io.Authentication.Gcp,
        io.Input.PubSub("projects/test-project/subscriptions/inputSub", None, None),
        io.Output.PubSub("projects/test-project/topics/good-topic", Some(Set("app_id")), None, None, None, None),
        Some(io.Output.PubSub("projects/test-project/topics/pii-topic", None, None, None, None, None)),
        io.Output.PubSub("projects/test-project/topics/bad-topic", None, None, None, None, None),
        Some(7.days),
        Some(
          io.Monitoring(
            Some(Sentry(URI.create("http://sentry.acme.com"))),
            Some(
              io.MetricsReporters(
                Some(io.MetricsReporters.StatsD("localhost", 8125, Map("app" -> "enrich"), 10.seconds, None)),
                Some(io.MetricsReporters.Stdout(10.seconds, None))
              )
            )
          )
        )
      )
      ConfigFile.parse[IO](configPath.asRight).value.map(result => result must beRight(expected))
    }

    "parse valid 0 minutes as None" in {
      val input =
        json"""{
          "auth": {
            "type": "Gcp"
          },
          "input": {
            "type": "PubSub",
            "subscription": "projects/test-project/subscriptions/inputSub"
          },
          "good": {
            "type": "PubSub",
            "topic": "projects/test-project/topics/good-topic"
          },
          "pii": {
            "type": "PubSub",
            "topic": "projects/test-project/topics/pii-topic"
          },
          "bad": {
            "type": "PubSub",
            "topic": "projects/test-project/topics/bad-topic"
          },

          "assetsUpdatePeriod": "0 minutes",
          "metricsReportPeriod": "10 second"
        }"""

      ConfigFile.parse[IO](Base64Hocon(input).asLeft).value.map {
        case Left(message) => message must contain("assetsUpdatePeriod in config file cannot be less than 0")
        case _ => ko("Decoding should have failed")
      }
    }

    "not throw an exception if file not found" in {
      val configPath = Paths.get("does-not-exist")
      ConfigFile.parse[IO](configPath.asRight).value.map(result => result must beLeft)
    }
  }

  "validateConfig" should {
    "return accumulated invalid attributes in config file" in {

      val invalidAttr1 = "invalidAttr1"
      val invalidAttr2 = "invalidAttr2"

      val configFile = ConfigFile(
        io.Authentication.Gcp,
        io.Input.PubSub("projects/test-project/subscriptions/inputSub", None, None),
        io.Output.PubSub("projects/test-project/topics/good-topic", Some(Set("app_id", invalidAttr1)), None, None, None, None),
        Some(io.Output.PubSub("projects/test-project/topics/pii-topic", Some(Set("app_id", invalidAttr2)), None, None, None, None)),
        io.Output.PubSub("projects/test-project/topics/bad-topic", None, None, None, None, None),
        Some(7.days),
        Some(
          io.Monitoring(
            Some(Sentry(URI.create("http://sentry.acme.com"))),
            Some(
              io.MetricsReporters(
                Some(io.MetricsReporters.StatsD("localhost", 8125, Map("app" -> "enrich"), 10.seconds, None)),
                Some(io.MetricsReporters.Stdout(10.seconds, None))
              )
            )
          )
        )
      )

      Environment.validateConfig[IO](configFile).value.map(result => result must beLeft)
    }
  }
}
