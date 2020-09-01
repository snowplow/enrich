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
package com.snowplowanalytics.snowplow.enrich.fs2.config

import cats.syntax.either._

import cats.effect.IO

import org.specs2.mutable.Specification
import cats.effect.testing.specs2.CatsIO

class CliConfigSpec extends Specification with CatsIO {
  "parseHocon" should {
    "parse valid HOCON" in {
      val string = """
           input = {
             type = "PubSub"
             subscriptionId = "inputSub"
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
           auth = {
             type = "Gcp"
             projectId = "test-project"
           }
           input = {
             type = "PubSub"
             subscriptionId = "inputSub"
           }
           good = {
             type = "PubSub"
             topic = "good-topic"
           }
           bad = {
             type = "PubSub"
             topic = "bad-topic"
           }
          """)
          .getOrElse(throw new RuntimeException("Cannot parse HOCON file"))

      val expected = ConfigFile(
        io.Authentication.Gcp("test-project"),
        io.Input.PubSub("inputSub"),
        io.Output.PubSub("good-topic"),
        io.Output.PubSub("bad-topic"),
        None,
        None,
        None
      )

      ConfigFile.parse[IO](hocon.asLeft).value.map(result => result must beRight(expected))
    }
  }
}
