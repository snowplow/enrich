/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.core

import scala.collection.JavaConverters._
import scala.concurrent.duration.{DurationInt, FiniteDuration}

import java.nio.file.{Files, Path, Paths}
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.PosixFilePermission

import cats.data.Validated
import cats.syntax.either._
import cats.implicits._

import cats.effect.IO
import cats.effect.{Ref, Resource}

import fs2.Stream

import io.circe.parser

import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, EventProcessor, Factory, ListOfList, Sink}

import com.snowplowanalytics.iglu.core.SelfDescribingData

object Utils {

  // Specs2 framework timeout - total test execution time
  val TestTimeout: FiniteDuration = 10.minutes
  // Stream processing timeout - should be less than TestTimeout to allow time for cleanup and assertions
  val StreamTimeout: FiniteDuration = TestTimeout - 1.minute

  case class Output(
    enriched: List[Event],
    failed: List[Event],
    bad: List[BadRow]
  )
  object Output {
    def empty = Output(Nil, Nil, Nil)
  }

  def runEnrichPipe[SourceConfig, SinkConfig](
    factory: Factory[IO, SourceConfig, SinkConfig],
    rawSinkConfig: SinkConfig,
    enrichedSourceConfig: SourceConfig,
    failedSourceConfig: SourceConfig,
    badSourceConfig: SourceConfig,
    nbEnriched: Long,
    nbBad: Long,
    nbGoodDrop: Long = 0L,
    nbBadDrop: Long = 0L,
    timeout: FiniteDuration = StreamTimeout
  ): IO[Output] = {
    def streams(
      ref: Ref[IO, Output],
      factory: Factory[IO, SourceConfig, SinkConfig],
      input: Stream[IO, Array[Byte]]
    ): Stream[IO, Nothing] = {
      val rawSink = Stream
        .resource[IO, Sink[IO]](factory.sink(rawSinkConfig))
        .flatMap(sink => input.parEvalMapUnordered(10)(cp => sink.sinkSimple(ListOfList.ofItems(cp))))
      val enrichedSource = Stream
        .resource(factory.source(enrichedSourceConfig))
        .flatMap(source => source.stream(EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit), enrichedProcessor(ref)))
      val failedSource = Stream
        .resource(factory.source(failedSourceConfig))
        .flatMap(source => source.stream(EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit), failedProcessor(ref)))
      val badSource = Stream
        .resource(factory.source(badSourceConfig))
        .flatMap(source => source.stream(EventProcessingConfig(EventProcessingConfig.NoWindowing, _ => IO.unit), badProcessor(ref)))

      enrichedSource
        .concurrently(failedSource)
        .concurrently(badSource)
        .concurrently(rawSink)
        .drain
    }

    for {
      ref <- Ref.of[IO, Output](Output.empty)
      input = CollectorPayloadGen.generate[IO](nbEnriched, nbBad, nbGoodDrop, nbBadDrop)
      _ <- streams(ref, factory, input).interruptAfter(timeout).compile.drain
      output <- ref.get
    } yield output
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

  private def parseBadRow(s: String): Either[String, BadRow] =
    for {
      json <- parser.parse(s).leftMap(_.message)
      sdj <- json.as[SelfDescribingData[BadRow]].leftMap(_.message)
    } yield sdj.data

  def writeEnrichmentsConfigsToDisk(enrichments: List[Enrichment]): Resource[IO, Path] =
    for {
      path <- Resource.make(IO.blocking(Files.createTempDirectory("")))(path => IO.blocking(Files.delete(path)))
      allPermissions = Set(
                         PosixFilePermission.OWNER_READ,
                         PosixFilePermission.OWNER_WRITE,
                         PosixFilePermission.OWNER_EXECUTE,
                         PosixFilePermission.GROUP_READ,
                         PosixFilePermission.GROUP_WRITE,
                         PosixFilePermission.GROUP_EXECUTE,
                         PosixFilePermission.OTHERS_READ,
                         PosixFilePermission.OTHERS_WRITE,
                         PosixFilePermission.OTHERS_EXECUTE
                       ).asJava
      _ <- Resource.eval(IO.blocking(Files.setPosixFilePermissions(path, allPermissions)))
      _ <- enrichments.traverse { enrichment =>
             val enrichmentPath = Paths.get(path.toAbsolutePath.toString, enrichment.fileName)
             Resource.make(
               IO.blocking(Files.write(enrichmentPath, enrichment.config.getBytes(StandardCharsets.UTF_8))) >>
                 IO.blocking(Files.setPosixFilePermissions(enrichmentPath, allPermissions))
             )(_ => IO.blocking(Files.delete(enrichmentPath)))
           }
    } yield path
}
