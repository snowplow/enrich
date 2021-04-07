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

import cats.effect.IO
import cats.implicits._
import cats.effect.concurrent.Ref
import cats.effect.testing.specs2.CatsIO
import fs2.Stream

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

class PayloadSpec extends Specification with CatsIO with ScalaCheck {

  "sink" should {
    "execute finalize action only once" in {
      val input = List(1, 2, 3)
      val pipe = { (s: Stream[IO, List[Int]]) => s.flatMap(Stream.emits(_)).drain }
      for {
        ref <- Ref.of[IO, Int](0)
        payload = Payload(input, ref.update(_ + 1))
        _ <- Stream.emit(payload).through(Payload.sink(pipe)).compile.drain
        result <- ref.get
      } yield result must beEqualTo(1)
    }

    "not execute finalize action when an error occurs" in {
      val pipe = { (s: Stream[IO, Int]) => s.evalMap(_ => IO.raiseError(new RuntimeException("boom!"))) }
      for {
        ref <- Ref.of[IO, Int](0)
        payload = Payload(1, ref.update(_ + 1))
        _ <- Stream.emit(payload).through(Payload.sink(pipe)).compile.drain.handleError(_ => ())
        result <- ref.get
      } yield result must beEqualTo(0)
    }
  }
}
