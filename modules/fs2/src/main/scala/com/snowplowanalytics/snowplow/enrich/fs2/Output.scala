/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.fs2

import java.time.Instant
import java.nio.charset.StandardCharsets.UTF_8

import cats.effect.Concurrent
import fs2.Pipe

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, Payload => BadRowPayload}
import com.snowplowanalytics.snowplow.enrich.common.utils.ConversionUtils

/** An item that can be sunk into any one of the enrichment sinks: Bad, Good, or Pii */
sealed trait Output[+Bad, +Good, +Pii]

object Output {

  /** An item destined for the Bad Sink */
  final case class Bad[B](badRow: B) extends Output[B, Nothing, Nothing]

  /** An item destined for the Good Sink */
  final case class Good[G](event: G) extends Output[Nothing, G, Nothing]

  /** An item destined for the Pii Sink */
  final case class Pii[P](event: P) extends Output[Nothing, Nothing, P]

  type Attributed[B, G, P] = Output[B, AttributedData[G], AttributedData[P]]

  type Parsed = Attributed[BadRow, EnrichedEvent, EnrichedEvent]
  type Serialized = Attributed[Array[Byte], Array[Byte], Array[Byte]]

  /** Sends a bad/good/pii item into the appropriate sink */
  def sink[F[_]: Concurrent, B, G, P](
    bad: Pipe[F, B, Unit],
    good: Pipe[F, G, Unit],
    pii: Pipe[F, P, Unit]
  ): Pipe[F, Output[B, G, P], Unit] =
    _.observeAsync(Int.MaxValue)(_.collect { case Good(event) => event }.through(good))
      .observeAsync(Int.MaxValue)(_.collect { case Bad(br) => br }.through(bad))
      .observeAsync(Int.MaxValue)(_.collect { case Pii(event) => event }.through(pii))
      .drain

  /**
   * Serializes events and bad rows into byte arrays. Good/Pii events exceeding
   *  a maximum size after serialization are converted to bad events.
   */
  val serialize: Parsed => Serialized =
    serializeGood.andThen(serializePii).andThen(serializeBad)

  private def serializeGood[P]: Attributed[BadRow, EnrichedEvent, P] => Attributed[BadRow, Array[Byte], P] = {
    case Good(AttributedData(g, attr)) =>
      serializeEnriched(g).fold(Bad(_), s => Good(AttributedData(s, attr)))
    case b @ Bad(_) => b
    case p @ Pii(_) => p
  }

  private def serializePii[G]: Attributed[BadRow, G, EnrichedEvent] => Attributed[BadRow, G, Array[Byte]] = {
    case Pii(AttributedData(p, attr)) =>
      serializeEnriched(p).fold(Bad(_), s => Pii(AttributedData(s, attr)))
    case b @ Bad(_) => b
    case g @ Good(_) => g
  }

  private def serializeBad[G, P]: Output[BadRow, G, P] => Output[Array[Byte], G, P] = {
    case Bad(br) => Bad(br.compact.getBytes(UTF_8))
    case g @ Good(_) => g
    case p @ Pii(_) => p
  }

  private def serializeEnriched(enriched: EnrichedEvent): Either[BadRow, Array[Byte]] = {
    val asStr = ConversionUtils.tabSeparatedEnrichedEvent(enriched)
    val asBytes = asStr.getBytes(UTF_8)
    val size = asBytes.length
    if (size > MaxRecordSize) {
      val msg = s"event passed enrichment but then exceeded the maximum allowed size $MaxRecordSize"
      val br = BadRow
        .SizeViolation(
          Enrich.processor,
          Failure.SizeViolation(Instant.now(), MaxRecordSize, size, msg),
          BadRowPayload.RawPayload(asStr.take(MaxErrorMessageSize))
        )
      Left(br)
    } else Right(asBytes)
  }

  /**
   * The maximum size of a serialized payload that can be written to pubsub.
   *
   *  Equal to 6.9 MB. The message will be base64 encoded by the underlying library, which brings the
   *  encoded message size to near 10 MB, which is the maximum allowed for PubSub.
   */
  private val MaxRecordSize = 6900000

  /** The maximum substring of the message that we write into a SizeViolation bad row */
  private val MaxErrorMessageSize = MaxRecordSize / 10

}
