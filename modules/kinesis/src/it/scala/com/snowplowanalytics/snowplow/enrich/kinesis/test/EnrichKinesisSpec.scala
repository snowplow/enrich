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
package com.snowplowanalytics.snowplow.enrich.kinesis.it

import scala.concurrent.duration._

import scala.concurrent.ExecutionContext

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.effect.{Blocker, ExitCode, IO}
import cats.effect.concurrent.Ref

import fs2.Stream

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsIO

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.enrich.kinesis.KinesisRun

class EnrichKinesisSpec extends Specification with CatsIO {

  private implicit def logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val nbGood = 100l
  val nbBad = 10l

  type AggregateGood = List[Event]
  type AggregateBad = List[String]
  case class Aggregates(good: AggregateGood, bad: AggregateBad)

  def run(): IO[Aggregates] = {
    val resources =
      for {
        _ <- Resources.init[IO]
        blocker <- Blocker[IO]
      } yield blocker

    resources.use { blocker =>

      val generate = CollectorPayloadGen.generate[IO](nbGood, nbBad)
        .through(KinesisSink.init[IO](blocker, Resources.collectorPayloadsStream))
        .onComplete(fs2.Stream.eval(Logger[IO].info(s"Random data has been generated and sent to ${Resources.collectorPayloadsStream}")))

      val args =
        List(
          "--config",
          Resources.configFilePath.toString,
          "--iglu-config",
          Resources.resolverFilePath.toString,
          "--enrichments",
          Resources.enrichmentsPath.toString
        )
      val executionContext = ExecutionContext.global
      val enrich = Stream.eval[IO, ExitCode](KinesisRun.run[IO](args, executionContext))

      def consume(refGood: Ref[IO, AggregateGood], refBad: Ref[IO, AggregateBad]): Stream[IO, Unit] =
        consumeGood(refGood)
          .merge(consumeBad(refBad))

      def consumeGood(ref: Ref[IO, AggregateGood]): Stream[IO, Unit] =
        KinesisSource.init[IO](blocker, Resources.enrichedStream, Resources.dynamoDbTableGood)
          .evalMap(aggregateGood(_, ref))

      def consumeBad(ref: Ref[IO, AggregateBad]): Stream[IO, Unit] =
        KinesisSource.init[IO](blocker, Resources.badRowsStream, Resources.dynamoDbTableBad)
          .evalMap(aggregateBad(_, ref))

      def aggregateGood(r: Array[Byte], ref: Ref[IO, AggregateGood]): IO[Unit] =
        for {
          e <- IO(Event.parse(new String(r)).getOrElse(throw new RuntimeException("can't parse enriched event")))
          _ <- ref.update(updateAggregateGood(_, e))
        } yield ()

      def aggregateBad(r: Array[Byte], ref: Ref[IO, AggregateBad]): IO[Unit] =
        for {
          br <- IO(new String(r))
          _ <- ref.update(updateAggregateBad(_, br))
        } yield ()

      def updateAggregateGood(aggregate: AggregateGood, e: Event): AggregateGood =
        e :: aggregate

      def updateAggregateBad(aggregate: AggregateBad, br: String): AggregateBad =
        br :: aggregate

      for {
        refGood <- Ref.of[IO, AggregateGood](Nil)
        refBad <- Ref.of[IO, AggregateBad](Nil)
        _ <-
          generate
            .merge(enrich)
            .merge(consume(refGood, refBad))
            .interruptAfter(3.minutes)
            .attempt
            .compile
            .drain
        aggregateGood <- refGood.get
        aggregateBad <- refBad.get
      } yield Aggregates(aggregateGood, aggregateBad)
    }
  }

  val aggregates = run().unsafeRunSync()

  println("Bad rows:")
  aggregates.bad.foreach(println)

  "enrich-kinesis" should {
    "emit the expected enriched events" in {
      aggregates.good.size must beEqualTo(nbGood)
    }

    "emit the expected bad rows events" in {
      aggregates.bad.size must beEqualTo(nbBad)
    }
  }
}
