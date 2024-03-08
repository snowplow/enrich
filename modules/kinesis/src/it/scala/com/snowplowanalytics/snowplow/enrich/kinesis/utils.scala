/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.kinesis

import scala.concurrent.duration._

import cats.data.Validated

import cats.effect.IO
import cats.effect.kernel.Resource

import cats.effect.testing.specs2.CatsEffect

import fs2.{Pipe, Stream}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Input
import com.snowplowanalytics.snowplow.enrich.common.fs2.test.Utils

object utils extends CatsEffect {

  sealed trait OutputRow
  object OutputRow {
    final case class Good(event: Event) extends OutputRow
    final case class Bad(badRow: BadRow) extends OutputRow
    final case class Incomplete(incomplete: Event) extends OutputRow
  }

  def mkEnrichPipe(
    localstackPort: Int,
    uuid: String
  ): Resource[IO, Pipe[IO, Array[Byte], OutputRow]] =
    for {
      streams <- Resource.pure(KinesisConfig.getStreams(uuid))
      rawSink <- Sink.init[IO](KinesisConfig.rawStreamConfig(localstackPort, streams.raw))
    } yield {
      val enriched = asGood(outputStream(KinesisConfig.enrichedStreamConfig(localstackPort, streams.enriched)))
      val bad = asBad(outputStream(KinesisConfig.badStreamConfig(localstackPort, streams.bad)))
      val incomplete = asIncomplete(outputStream(KinesisConfig.incompleteStreamConfig(localstackPort, streams.incomplete)))

      collectorPayloads =>
        enriched
          .merge(bad)
          .merge(incomplete)
          .interruptAfter(3.minutes)
          .concurrently(collectorPayloads.evalMap(bytes => rawSink(List(bytes))))
    }

  private def outputStream(config: Input.Kinesis): Stream[IO, Array[Byte]] =
    Source.init[IO](config, KinesisConfig.monitoring)
      .map(Main.getPayload)

  private def asGood(source: Stream[IO, Array[Byte]]): Stream[IO, OutputRow.Good] =
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

  private def asBad(source: Stream[IO, Array[Byte]]): Stream[IO, OutputRow.Bad] =
    source.map { bytes =>
      OutputRow.Bad {
        val s = new String(bytes)
        Utils.parseBadRow(s) match {
          case Right(br) => br
          case Left(e) =>
            throw new RuntimeException(s"Can't decode bad row $s. Error: $e")
        }
      }
    }

  private def asIncomplete(source: Stream[IO, Array[Byte]]): Stream[IO, OutputRow.Incomplete] =
    source.map { bytes =>
      OutputRow.Incomplete {
        val s = new String(bytes)
        Event.parse(s) match {
          case Validated.Valid(e) => e
          case Validated.Invalid(e) =>
            throw new RuntimeException(s"Can't parse incomplete event [$s]. Error: $e")
        }
      }
    }

  def parseOutput(output: List[OutputRow], testName: String): (List[Event], List[BadRow], List[Event]) = {
    val good = output.collect { case OutputRow.Good(e) => e}
    println(s"[$testName] Bad rows:")
    val bad = output.collect { case OutputRow.Bad(b) =>
      println(s"[$testName] ${b.compact}")
      b
    }
    val incomplete = output.collect { case OutputRow.Incomplete(i) => i}
    (good, bad, incomplete)
  }
}
