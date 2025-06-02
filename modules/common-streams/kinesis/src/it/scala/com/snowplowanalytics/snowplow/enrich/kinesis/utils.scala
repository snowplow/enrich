/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.streams.kinesis

import java.io._
import java.net._

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import cats.data.Validated

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}

import cats.effect.testing.specs2.CatsEffect

import fs2.Stream

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.snowplow.streams.kinesis.KinesisFactory
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor, ListOfList, Sink}

import com.snowplowanalytics.snowplow.enrich.streams.common.Utils

object utils extends CatsEffect {

  case class Output(
    enriched: List[Event],
    failed: List[Event],
    bad: List[BadRow]
  )
  object Output {
    def empty = Output(Nil, Nil, Nil)
  }

  case class KinesisTestResources(localstack: Containers.Localstack, statsdAdmin: StatsdAdmin)

  def runEnrichPipe(
    input: Stream[IO, Array[Byte]],
    localstackPort: Int,
    uuid: String
  ): IO[Output] = {
    def streams(ref: Ref[IO, Output], factory: KinesisFactory[IO]): Stream[IO, Nothing] = {
      val streams = KinesisConfig.getStreams(uuid)
      val rawSink = Stream
        .resource[IO, Sink[IO]](factory.sink(KinesisConfig.sinkConfig(localstackPort, streams.raw)))
        .flatMap(sink => input.parEvalMapUnordered(10)(cp => sink.sinkSimple(ListOfList.ofItems(cp))))
      val enrichedSource = Stream
        .resource(factory.source(KinesisConfig.sourceConfig(localstackPort, streams.enriched)))
        .flatMap(source => source.stream(EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit), enrichedProcessor(ref)))
      val failedSource = Stream
        .resource(factory.source(KinesisConfig.sourceConfig(localstackPort, streams.failed)))
        .flatMap(source => source.stream(EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit), failedProcessor(ref)))
      val badSource = Stream
        .resource(factory.source(KinesisConfig.sourceConfig(localstackPort, streams.bad)))
        .flatMap(source => source.stream(EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit), badProcessor(ref)))

      enrichedSource
        .concurrently(failedSource)
        .concurrently(badSource)
        .concurrently(rawSink)
        .drain
    }

    KinesisFactory.resource[IO].use { factory =>
      for {
        ref <- Ref.of[IO, Output](Output.empty)
        _ <- streams(ref, factory).interruptAfter(4.minutes).compile.drain
        output <- ref.get
      } yield output
    }
  }

  private def enrichedProcessor(ref: Ref[IO, Output]): EventProcessor[IO] =
    _.parEvalMapUnordered(2) { tokenedEvents =>
      tokenedEvents.events
        .traverse { buffer =>
          IO {
            val arr = new Array[Byte](buffer.remaining())
            buffer.get(arr)
            val s = new String(arr)
            Event.parse(s) match {
              case Validated.Valid(e) => e
              case Validated.Invalid(e) =>
                throw new RuntimeException(s"Can't parse enriched event [$s]. Error: $e")
            }
          }
        }
        .flatMap(enriched => ref.update(output => output.copy(enriched = enriched.toList ++ output.enriched)))
        .as(tokenedEvents.ack)
    }

  private def failedProcessor(ref: Ref[IO, Output]): EventProcessor[IO] =
    _.parEvalMapUnordered(2) { tokenedEvents =>
      tokenedEvents.events
        .traverse { buffer =>
          IO {
            val arr = new Array[Byte](buffer.remaining())
            buffer.get(arr)
            val s = new String(arr)
            Event.parse(s) match {
              case Validated.Valid(e) => e
              case Validated.Invalid(e) =>
                throw new RuntimeException(s"Can't parse failed event [$s]. Error: $e")
            }
          }
        }
        .flatMap(failed => ref.update(output => output.copy(failed = failed.toList ++ output.failed)))
        .as(tokenedEvents.ack)
    }

  private def badProcessor(ref: Ref[IO, Output]): EventProcessor[IO] =
    _.parEvalMapUnordered(2) { tokenedEvents =>
      tokenedEvents.events
        .traverse { buffer =>
          IO {
            val arr = new Array[Byte](buffer.remaining())
            buffer.get(arr)
            val s = new String(arr)
            Utils.parseBadRow(s) match {
              case Right(br) => br
              case Left(e) =>
                throw new RuntimeException(s"Can't decode bad row $s. Error: $e")
            }
          }
        }
        .flatMap(bad => ref.update(output => output.copy(bad = bad.toList ++ output.bad)))
        .as(tokenedEvents.ack)
    }

  trait StatsdAdmin {
    def get(metricType: String): IO[String]
    def getCounters = get("counters")
    def getGauges = get("gauges")
  }

  def mkStatsdAdmin(host: String, port: Int): Resource[IO, StatsdAdmin] =
    for {
      socket <- Resource.make(IO.blocking(new Socket(host, port)))(s => IO(s.close()))
      toStatsd <- Resource.make(IO(new PrintWriter(socket.getOutputStream(), true)))(pw => IO(pw.close()))
      fromStatsd <- Resource.make(IO(new BufferedReader(new InputStreamReader(socket.getInputStream()))))(br => IO(br.close()))
    } yield new StatsdAdmin {
      def get(metricType: String): IO[String] =
        for {
          _ <- IO.blocking(toStatsd.println(metricType))
          stats <- IO.blocking(fromStatsd.lines().iterator().asScala.takeWhile(!_.toLowerCase().contains("end")).mkString("\n"))
        } yield stats
    }
}
