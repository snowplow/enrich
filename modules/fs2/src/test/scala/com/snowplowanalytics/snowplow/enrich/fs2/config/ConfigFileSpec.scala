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

import java.net.URI
import java.nio.file.Paths

import scala.concurrent.duration._

import cats.syntax.either._

import cats.effect.IO

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.enrich.fs2.SpecHelpers._

class ConfigFileSpec extends Specification {
  "parse" should {
    "parse valid HOCON file with path provided" >> {
      val configPath = Paths.get(getClass.getResource("/config.fs2.hocon.sample").toURI)
      val result = ConfigFile.parse[IO](configPath.asRight).value.unsafeRunSync()
      val expected = ConfigFile(
        io.Authentication.Gcp("test-project"),
        io.Input.PubSub("inputSub"),
        io.Output.PubSub("good-topic"),
        io.Output.PubSub("bad-topic"),
        Some(7.days),
        Some(Sentry(URI.create("http://sentry.acme.com"))),
        Some(1.second)
      )
      result must beRight(expected)
    }

    "not throw an exception if file not found" >> {
      val configPath = Paths.get("does-not-exist")
      val result = ConfigFile.parse[IO](configPath.asRight).value.unsafeRunSync()
      result must beLeft
    }
  }
}
