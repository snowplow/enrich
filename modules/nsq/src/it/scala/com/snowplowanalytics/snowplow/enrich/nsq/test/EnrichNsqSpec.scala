/*
 * Copyright (c) 2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.nsq
package test

import scala.concurrent.duration._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.effect.{Blocker, IO}
import cats.effect.concurrent.Ref
import fs2.Stream
import org.specs2.mutable.Specification
import cats.effect.testing.specs2.CatsIO
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Input.{Nsq => InNsq}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Output.{Nsq => OutNsq}
import com.snowplowanalytics.snowplow.enrich.common.fs2.test.CollectorPayloadGen


class EnrichNsqSpec extends Specification with CatsIO {

  override protected val Timeout = 10.minutes

  sequential

  private implicit def logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val collectorPayloadsTopic = "RawEvents"

  val enrichedChannel = "EnrichedChannel"
  val enrichedTopic = "EnrichedEventsMirror"

  val badRowsChannel = "BadRowsChannel"
  val badRowsTopic = "BadEnrichedEventsMirror"

  val nbGood = 100l
  val nbBad = 10l

  type AggregateGood = List[Event]
  type AggregateBad = List[String]
  case class Aggregates(good: AggregateGood, bad: AggregateBad)

  val nsqdHost = "127.0.0.1"
  val nsqdPort = 4150

  val lookupHost = "127.0.0.1"
  val lookupPort = 4261

  val maxBufferQueueSize = 3000

  def run(): IO[Aggregates] = {

    val resources =
      for {
        blocker <- Blocker[IO]
        sink <- Sink.init[IO](blocker, OutNsq(collectorPayloadsTopic, nsqdHost, nsqdPort))
      } yield (sink, blocker)

    resources.use { case (sink, blocker) =>
      val generate =
        CollectorPayloadGen.generate[IO](nbGood, nbBad)
          .evalMap(events => sink(List(events)))
          .onComplete(fs2.Stream.eval(Logger[IO].info(s"Random data has been generated and sent to $collectorPayloadsTopic")))

      def consume(refGood: Ref[IO, AggregateGood], refBad: Ref[IO, AggregateBad]): Stream[IO, Unit] =
        consumeGood(refGood).merge(consumeBad(refBad))

      def consumeGood(ref: Ref[IO, AggregateGood]): Stream[IO, Unit] =
        Source.init[IO](blocker, InNsq(enrichedTopic, enrichedChannel, lookupHost, lookupPort, maxBufferQueueSize)).evalMap(aggregateGood(_, ref))

      def consumeBad(ref: Ref[IO, AggregateBad]): Stream[IO, Unit] =
        Source.init[IO](blocker, InNsq(badRowsTopic, badRowsChannel, lookupHost, lookupPort, maxBufferQueueSize)).evalMap(aggregateBad(_, ref))

      def aggregateGood(r: Record[IO], ref: Ref[IO, AggregateGood]): IO[Unit] = {
        for {
          e <- IO(Event.parse(new String(r.data)).getOrElse(throw new RuntimeException("can't parse enriched event")))
          _ <- r.ack
          _ <- ref.update(updateAggregateGood(_, e))
        } yield ()
      }

      def aggregateBad(r: Record[IO], ref: Ref[IO, AggregateBad]): IO[Unit] = {
        for {
          br <- IO(new String(r.data))
          _ <- r.ack
          _ <- ref.update(updateAggregateBad(_, br))
        } yield ()
      }

      def updateAggregateGood(aggregate: AggregateGood, e: Event): AggregateGood =
        e :: aggregate

      def updateAggregateBad(aggregate: AggregateBad, br: String): AggregateBad =
        br :: aggregate

      for {
        refGood <- Ref.of[IO, AggregateGood](Nil)
        refBad <- Ref.of[IO, AggregateBad](Nil)
        _ <-
          generate
            .merge(consume(refGood, refBad))
            .interruptAfter(2.minutes)
            .attempt
            .compile
            .drain
        aggregateGood <- refGood.get
        aggregateBad <- refBad.get
      } yield Aggregates(aggregateGood, aggregateBad)
    }
  }

  "enrich-nsq" should {
    "emit the correct number of enriched events and bad rows" in {
      run().map { aggregates =>
        aggregates.good.size === nbGood
        aggregates.bad.size === nbBad
      }
    }
  }
}
