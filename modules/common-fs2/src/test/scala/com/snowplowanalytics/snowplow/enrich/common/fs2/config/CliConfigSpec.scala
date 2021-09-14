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

import cats.syntax.either._

import org.specs2.mutable.Specification

import cats.effect.IO

import cats.effect.testing.specs2.CatsIO

class CliConfigSpec extends Specification with CatsIO {
  "parseHocon" should {
    "parse valid HOCON" in {
      val string = """
           input = {
             type = "PubSub"
             subscription = "inputSub"
           }
          """.stripMargin
      Base64Hocon.parseHocon(string) must beRight
    }
  }

  "ConfigFile.parse" should {
    "parse valid HOCON" in {
      val hocon =
        Base64Hocon
          .parseHocon("""
           input = {
             type = "PubSub"
             subscription = "projects/test-project/subscriptions/inputSub"
           }
           output = {
             good = {
               type = "PubSub"
               topic = "projects/test-project/topics/good-topic"
             }
             pii = {
               type = "PubSub"
               topic = "projects/test-project/topics/pii-topic"
               attributes = [ "app_id", "platform" ]
             }
             bad = {
               type = "PubSub"
               topic = "projects/test-project/topics/bad-topic"
             }
           }
          concurrency = {
            output = 10000
            enrichment = 64
          }
          """)
          .getOrElse(throw new RuntimeException("Cannot parse HOCON file"))

      val expected = ConfigFile(
        io.Input.PubSub("projects/test-project/subscriptions/inputSub", None, None),
        io.Outputs(
          io.Output.PubSub("projects/test-project/topics/good-topic", None, None, None, None, None),
          Some(io.Output.PubSub("projects/test-project/topics/pii-topic", Some(Set("app_id", "platform")), None, None, None, None)),
          io.Output.PubSub("projects/test-project/topics/bad-topic", None, None, None, None, None)
        ),
        io.Concurrency(10000, 64),
        None,
        None
      )

      ConfigFile.parse[IO](hocon.asLeft).value.map(result => result must beRight(expected))
    }
  }
}
