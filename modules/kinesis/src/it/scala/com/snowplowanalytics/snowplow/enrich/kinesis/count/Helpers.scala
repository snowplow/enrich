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
package com.snowplowanalytics.snowplow.enrich.kinesis.count

import scala.concurrent.duration._

import cats.Parallel
import cats.data.Validated
import cats.syntax.either._

import io.circe.parser

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}

import fs2.Stream

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Input
import com.snowplowanalytics.snowplow.enrich.common.fs2.ByteSink

import com.snowplowanalytics.snowplow.enrich.kinesis.{KinesisRun, Sink, Source}

object Helpers {

  sealed trait OutputRow
  object OutputRow {
    final case class Good(event: Event) extends OutputRow
    final case class Bad(badRow: BadRow) extends OutputRow
  }

  final case class TestResources[F[_]](
    writeInput: Stream[F, Unit],
    readOutput: Stream[F, OutputRow]
  )

  def mkResources[F[_]: Concurrent: ConcurrentEffect: ContextShift: Parallel: Timer](
    nbGood: Long,
    nbBad: Long,
    localstackPort: Int
  ): Resource[F, TestResources[F]] =
    for {
      blocker <- Blocker[F]
      rawSink <- Sink.init[F](blocker, KinesisConfigReadWrite.rawStreamConfig(port = localstackPort))
    } yield {
      val goodSource = asGood(sourceStream[F](blocker, KinesisConfigReadWrite.enrichedStreamConfig(port = localstackPort)))
      val badSource = asBad(sourceStream[F](blocker, KinesisConfigReadWrite.badStreamConfig(port = localstackPort)))
      TestResources(
        writeInput[F](rawSink, nbGood, nbBad),
        goodSource.merge(badSource).interruptAfter(2.minutes)
      )
    }

  private def sourceStream[F[_]: ConcurrentEffect: ContextShift: Timer](blocker: Blocker, config: Input.Kinesis): Stream[F, Array[Byte]] =
    Source.init[F](blocker, config, KinesisConfigReadWrite.monitoring)
      .map(KinesisRun.getPayload)

  private def asGood[F[_]](source: Stream[F, Array[Byte]]): Stream[F, OutputRow.Good] =
    source.map { bytes =>
      OutputRow.Good {
        val s = new String(bytes)
        Event.parse(s) match {
          case Validated.Valid(e) => e
          case Validated.Invalid(e) =>
            throw new RuntimeException(s"Can't parse enriched event [$s]. Error: $e")
        }
      }
    }

  private def asBad[F[_]](source: Stream[F, Array[Byte]]): Stream[F, OutputRow.Bad] =
    source.map { bytes =>
      OutputRow.Bad {
        val s = new String(bytes)
        parseBadRow(s) match {
          case Right(br) => br
          case Left(e) =>
            throw new RuntimeException(s"Can't decode bad row $s. Error: $e")
        }
      }
    }

  private def writeInput[F[_]: Sync](sink: ByteSink[F], nbGood: Long, nbBad: Long): Stream[F, Unit] =
    CollectorPayloadGen.generate[F](nbGood, nbBad)
      .evalMap(events => sink(List(events)))

  private def parseBadRow(s: String): Either[String, BadRow] =
    for {
      json <- parser.parse(s).leftMap(_.message)
      sdj <- SelfDescribingData.parse(json).leftMap(_.message("Can't decode JSON as SDJ"))
      br <- sdj.data.as[BadRow].leftMap(_.getMessage())
    } yield br
}
