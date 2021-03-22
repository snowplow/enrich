/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.beam

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.util.concurrent.TimeUnit

import scala.util.Try

import cats.Id
import cats.effect.Clock

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.PiiPseudonymizerConf

object utils {

  /** Format an [[EnrichedEvent]] as a TSV. */
  def tabSeparatedEnrichedEvent(enrichedEvent: EnrichedEvent): String =
    enrichedEvent.getClass.getDeclaredFields
      .filterNot(_.getName.equals("pii"))
      .map { field =>
        field.setAccessible(true)
        Option(field.get(enrichedEvent)).getOrElse("")
      }
      .mkString("\t")

  /** Determine if we have to emit pii transformation events. */
  def emitPii(confs: List[EnrichmentConf]): Boolean =
    confs
      .collectFirst { case c: PiiPseudonymizerConf => c }
      .exists(_.emitIdentificationEvent)

  // We want to take one-tenth of the payload characters (not taking into account multi-bytes char)
  private val ReductionFactor = 10

  /**
   * Truncate an oversized formatted enriched event into a bad row.
   * @param value TSV-formatted oversized enriched event
   * @param maxSizeBytes maximum size in bytes a record can take
   * @param processor metadata about this artifact
   * @return a bad row containing a the truncated enriched event (10 times less than the max size)
   */
  def resizeEnrichedEvent(
    value: String,
    size: Int,
    maxSizeBytes: Int,
    processor: Processor
  ): BadRow = {
    val msg = "event passed enrichment but exceeded the maximum allowed size as a result"
    BadRow
      .SizeViolation(
        processor,
        Failure.SizeViolation(Instant.now(), maxSizeBytes, size, msg),
        Payload.RawPayload(value.take(maxSizeBytes / ReductionFactor))
      )
  }

  /**
   * Resize a bad row if it exceeds the maximum allowed size.
   * @param badRow the original bad row which can be oversized
   * @param maxSizeBytes maximum size in bytes a record can take
   * @return a bad row where the line is 10 times less than the max size
   */
  def resizeBadRow(
    badRow: BadRow,
    maxSizeBytes: Int,
    processor: Processor
  ): BadRow = {
    val originalBadRow = badRow.compact
    val size = getSize(originalBadRow)
    if (size > maxSizeBytes)
      BadRow
        .SizeViolation(
          processor,
          Failure
            .SizeViolation(Instant.now(), maxSizeBytes, size, "bad row exceeded the maximum size"),
          Payload.RawPayload(originalBadRow.take(maxSizeBytes / ReductionFactor))
        )
    else badRow
  }

  /** The size of a string in bytes */
  val getSize: String => Int = evt => evt.getBytes(UTF_8).size

  /** Measure the time spent in a block of code in milliseconds. */
  def timeMs[A](call: => A): (A, Long) = {
    val t0 = System.currentTimeMillis()
    val result = call
    val t1 = System.currentTimeMillis()
    (result, t1 - t0)
  }

  /**
   * Create a symbolic link.
   * @param file to create the sym link for
   * @param symLink path to the symbolic link to be created
   * @return either the path of the created sym link or the error
   */
  def createSymLink(file: File, symLink: String): Either[String, Path] = {
    val symLinkPath = Paths.get(symLink)
    if (!Files.exists(symLinkPath))
      Try(Files.createSymbolicLink(symLinkPath, file.toPath)) match {
        case scala.util.Success(p) => Right(p)
        case scala.util.Failure(t) => Left(s"Symlink can't be created: ${t.getMessage}")
      }
    else Left(s"A file at path $symLinkPath already exists")
  }

  /**
   * Set up dynamic counter metrics from an [[EnrichedEvent]].
   * @param enrichedEvent to extract the metrics from
   * @return the name of the counter metrics that needs to be incremented.
   */
  def getEnrichedEventMetrics(enrichedEvent: EnrichedEvent): List[String] =
    List(
      Option(enrichedEvent.event_vendor).map(v => ("vendor", v)),
      Option(enrichedEvent.v_tracker).map(t => ("tracker", t))
    ).flatten
      .map { case (n, v) => n + "_" + v.replaceAll("[.-]", "_") }

  implicit val idClock: Clock[Id] = new Clock[Id] {
    final def realTime(unit: TimeUnit): Id[Long] =
      unit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
    final def monotonic(unit: TimeUnit): Id[Long] =
      unit.convert(System.nanoTime(), TimeUnit.NANOSECONDS)
  }
}
