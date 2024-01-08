/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.nsq
package test

import scala.concurrent.duration._

import cats.implicits._

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync, Timer, Async}
import cats.effect.concurrent.Ref

import fs2.Stream

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.snowplow.enrich.common.fs2.ByteSink
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Input.{Nsq => InNsq}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Output.{Nsq => OutNsq}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.BackoffPolicy
import com.snowplowanalytics.snowplow.enrich.common.fs2.test.{CollectorPayloadGen, Utils => CommonUtils}
import com.snowplowanalytics.snowplow.enrich.nsq.test.Containers.NetworkTopology

object Utils {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  val maxBufferQueueSize = 3000

  val backoffPolicy = BackoffPolicy(
    minBackoff = 100.milliseconds,
    maxBackoff = 10.seconds,
    maxRetries = Some(10)
  )

  type AggregateGood = List[Event]
  type AggregateBad = List[BadRow]

  def mkResources[F[_]: Async: ContextShift: Timer] =
    for {
      blocker <- Blocker[F]
      topology <- Containers.createContainers[F](blocker)
      sink <- Sink.init[F](
        blocker,
        OutNsq(
          topology.sourceTopic,
          "127.0.0.1",
          topology.nsqd1.tcpPort,
          backoffPolicy
        )
      )
    } yield (blocker, topology, sink)

  def generateEvents[F[_]: Sync: ContextShift: Timer](sink: ByteSink[F], goodCount: Long, badCount: Long, topology: NetworkTopology): Stream[F, Unit] =
    CollectorPayloadGen.generate[F](goodCount, badCount)
      .evalMap(events => sink(List(events)))
      .onComplete(fs2.Stream.eval(Logger[F].info(s"Random data has been generated and sent to ${topology.sourceTopic}")))

  def consume[F[_]: ConcurrentEffect: ContextShift](
    blocker: Blocker,
    refGood: Ref[F, AggregateGood],
    refBad: Ref[F, AggregateBad],
    topology: NetworkTopology
  ): Stream[F, Unit] =
    consumeGood(blocker, refGood, topology).merge(consumeBad(blocker, refBad, topology))

  def consumeGood[F[_]: ConcurrentEffect: ContextShift](
    blocker: Blocker,
    ref: Ref[F, AggregateGood],
    topology: NetworkTopology
  ): Stream[F, Unit] =
    Source.init[F](
      blocker,
      InNsq(
        topology.goodDestTopic,
        "EnrichedChannel",
        "127.0.0.1",
        topology.lookup2.httpPort,
        maxBufferQueueSize,
        backoffPolicy
      )
    ).evalMap(aggregateGood(_, ref))

  def consumeBad[F[_]: ConcurrentEffect: ContextShift](
    blocker: Blocker,
    ref: Ref[F, AggregateBad],
    topology: NetworkTopology
  ): Stream[F, Unit] =
    Source.init[F](
      blocker,
      InNsq(
        topology.badDestTopic,
        "BadRowsChannel",
        "127.0.0.1",
        topology.lookup2.httpPort,
        maxBufferQueueSize,
        backoffPolicy
      )
    ).evalMap(aggregateBad(_, ref))

  def aggregateGood[F[_]: Sync](r: Record[F], ref: Ref[F, AggregateGood]): F[Unit] = {
    for {
      e <- Sync[F].delay(Event.parse(new String(r.data)).getOrElse(throw new RuntimeException("can't parse enriched event")))
      _ <- r.ack
      _ <- ref.update(updateAggregateGood(_, e))
    } yield ()
  }

  def aggregateBad[F[_]: Sync](r: Record[F], ref: Ref[F, AggregateBad]): F[Unit] = {
    for {
      s <- Sync[F].delay(new String(r.data))
      br = CommonUtils.parseBadRow(s) match {
        case Right(br) => br
        case Left(e) =>
          throw new RuntimeException(s"Can't decode bad row $s. Error: $e")
      }
      _ <- r.ack
      _ <- ref.update(updateAggregateBad(_, br))
    } yield ()
  }

  def updateAggregateGood(aggregate: AggregateGood, e: Event): AggregateGood =
    e :: aggregate

  def updateAggregateBad(aggregate: AggregateBad, br: BadRow): AggregateBad =
    br :: aggregate
}
