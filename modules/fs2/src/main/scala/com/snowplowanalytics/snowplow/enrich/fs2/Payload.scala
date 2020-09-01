/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
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

import scala.annotation.tailrec

import cats.Applicative
import cats.syntax.either._
import cats.data.Validated

import fs2.{Pure, Stream}

import com.snowplowanalytics.snowplow.enrich.fs2.Payload.Parsed

/**
 * Anything that has been read from [[RawSource]] and needs to be acknowledged
 * or a derivative (parsed `A`) that can be used to acknowledge the original message
 * @param data original data or anything it has been transformed to
 * @param finalise a side-effect to acknowledge (commit, log on-finish) the message or
 *                 no-op in case the original message has been flattened into
 *                 multiple rows and only last row contains the actual side-effect
 */
case class Payload[F[_], A](data: A, finalise: F[Unit]) {

  /**
   * Flatten all payloads from a list and replace an `ack` action to no-op everywhere
   * except last message, so that original collector payload (with multiple events)
   * will be ack'ed only when last event has sunk into good or bad sink
   */
  def decompose[L, R](implicit ev: A <:< List[Validated[L, R]], F: Applicative[F]): Stream[F, Parsed[F, L, R]] = {
    val _ = ev
    val noop: F[Unit] = Applicative[F].unit
    def use(op: F[Unit])(v: Validated[L, R]): Parsed[F, L, R] =
      v.fold(a => Payload(a, op).asLeft, b => Payload(b, op).asRight)

    Payload.mapWithLast(use(noop), use(finalise))(data)
  }
}

object Payload {

  /**
   * Original [[Payload]] that has been transformed into either `A` or `B`
   * Despite of the result (`A` or `B`) the original one still has to be acknowledged
   *
   * If original contained only one row (good or bad), the `Parsed` must have a real
   * `ack` action, otherwise if it has been accompanied by other rows, only the last
   * element from the original will contain the `ack`, all others just `noop`
   */
  type Parsed[F[_], A, B] = Either[Payload[F, A], Payload[F, B]]

  /** Apply `f` function to all elements in a list, except last one, where `lastF` applied */
  def mapWithLast[A, B](f: A => B, lastF: A => B)(as: List[A]): Stream[Pure, B] = {
    @tailrec
    def go(aas: List[A], accum: Vector[B]): Vector[B] =
      aas match {
        case Nil =>
          accum
        case last :: Nil =>
          accum :+ lastF(last)
        case a :: remaining =>
          go(remaining, accum :+ f(a))
      }

    Stream.emits(go(as, Vector.empty))
  }
}
