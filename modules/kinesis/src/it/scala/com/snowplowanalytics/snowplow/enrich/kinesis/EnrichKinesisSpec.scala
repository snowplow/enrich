/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.kinesis

import scala.concurrent.duration._

import cats.effect.IO

import cats.implicits._

import cats.effect.testing.specs2.CatsIO

import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll

class EnrichKinesisSpec extends Specification with AfterAll with CatsIO {

  override protected val Timeout = 10.minutes

  def afterAll: Unit = Containers.localstack.stop()

  "enrich-kinesis" should {
    "be able to parse the minimal config" in {
      Containers.enrich[IO](
        configPath = "config/config.kinesis.minimal.hocon",
        loggerName = "minimal",
        needsLocalstack = false
      ).use { e =>
        IO(e.getLogs must contain("Running Enrich"))
      }
    }

    "emit the correct number of enriched events and bad rows" in {
      import count.Helpers._

      val nbGood = 100l
      val nbBad = 10l

      val testResources = Containers.enrich[IO](
        configPath = "modules/kinesis/src/it/resources/enrich/enrich-localstack.hocon",
        loggerName = "count",
        needsLocalstack = true
      ).flatMap(_ => mkResources[IO](nbGood, nbBad, localstackPort = Containers.localStackPort))

      testResources.use { r =>
        for {
          aggregates <- r.readOutput.concurrently(r.writeInput).compile.toList
          good = aggregates.collect { case OutputRow.Good(e) => e}
          bad = aggregates.collect { case OutputRow.Bad(b) => b}
          _ <- IO(println(s"Bad rows:\n${bad.map(_.compact).mkString("\n")}"))
        } yield {
          good.size.toLong === nbGood
          bad.size.toLong === nbBad
        }
      }
    }

    "shutdown when it receives a SIGTERM" in {
      Containers.enrich[IO](
        configPath = "modules/kinesis/src/it/resources/enrich/enrich-localstack.hocon",
        loggerName = "stop",
        needsLocalstack = true,
        waitLogMessage = "enrich.metrics"
      ).use { enrich =>
        for {
          _ <- IO(println("stop - Sending signal"))
          _ <- IO(enrich.getDockerClient().killContainerCmd(enrich.getContainerId()).withSignal("TERM").exec())
          _ <- Containers.waitUntilStopped(enrich)
        } yield {
          enrich.isRunning() === false
          enrich.getLogs() must contain("Enrich stopped")
        }
      }
    }
  }
}
