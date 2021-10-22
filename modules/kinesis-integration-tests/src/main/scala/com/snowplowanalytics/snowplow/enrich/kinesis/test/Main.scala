/*
 * Copyright (c) 2021-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.kinesis.test

import scala.concurrent.duration._

import cats.effect.{Blocker, ExitCode, IO, IOApp}

object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val region = args(0)
    val collectorPayloadStream = args(1)
    val enrichedStream = args(2)
    val badStream = args(3)

    val nbGood = 100l
    val nbBad = 0l

    Blocker[IO].use { blocker =>


      val generate = CollectorPayloadGen.generate[IO](nbGood, nbBad)
        .through(KinesisSink.init[IO](blocker, region, collectorPayloadStream))

      val aggregateGood =
        KinesisSource.init[IO](blocker, region, enrichedStream)
          .interruptAfter(10.seconds)
          .scan(0l)((acc, _) => acc + 1l)
          .lastOr(0l)
          .evalMap { countGood =>
            if (countGood == nbGood) IO(println(s"$countGood enriched events, awesomeness")) 
            else IO.raiseError(new RuntimeException(s"$countGood enriched events, should be $nbGood"))
          }

      val aggregateBad =
        KinesisSource.init[IO](blocker, region, badStream)
          .interruptAfter(10.seconds)
          .scan(0l)((acc, _) => acc + 1l)
          .lastOr(0l)
          .evalMap { countBad =>
            if (countBad == nbBad) IO(println(s"$countBad bad rows, awesomeness")) 
            else IO.raiseError(new RuntimeException(s"$countBad bad rows, should be $nbBad"))
          }

      generate
        .merge(aggregateGood)
        .merge(aggregateBad)
        .compile
        .drain
        .as(ExitCode.Success)
    }
  }
}
