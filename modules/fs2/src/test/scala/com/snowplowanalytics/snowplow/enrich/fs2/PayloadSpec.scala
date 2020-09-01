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

import cats.implicits._

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.effect.testing.specs2.CatsIO

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

class PayloadSpec extends Specification with CatsIO with ScalaCheck {
  "mapWithLast" should {
    "always apply a lastF function to the last element" in {
      prop { (list: List[String]) =>
        val lastF: String => String = _ => "unique"
        val result = Payload.mapWithLast(identity[String], lastF)(list).toList
        result.lastOption must (beSome("unique") or beNone)
      }
    }

    "always apply an f function to all elements except last" in {
      prop { (list: List[String]) =>
        val f: String => String = _ => "unique"
        val result = Payload.mapWithLast(f, identity[String])(list).toList
        list match {
          case Nil => ok
          case _ =>
            val init = List.fill(list.length - 1)("unique")
            result.mkString("-") must startWith(init.mkString("-"))
        }
      }
    }
  }

  "decompose" should {
    "preserve the order" in {
      val input = List("error-1".invalid, 42.valid, "error-2".invalid)
      val payload = Payload(input, IO.unit)
      payload.decompose[String, Int].compile.toList.map {
        case List(error1, valid, error2) =>
          error1 must beLeft.like {
            case Payload(data, _) => data must be("error-1")
          }
          valid must beRight.like {
            case Payload(data, _) => data must beEqualTo(42)
          }
          error2 must beLeft.like {
            case Payload(data, _) => data must be("error-2")
          }
        case other =>
          ko(s"Expected list of 3, got $other")
      }
    }

    "execute finalize action only once" in {
      val input = List("error-1".invalid, 42.valid, "error-2".invalid)
      for {
        ref <- Ref.of[IO, Int](0)
        payload = Payload(input, ref.update(_ + 1))
        parsed <- payload.decompose[String, Int].compile.toList
        _ <- parsed.traverse_(_.fold(_.finalise, _.finalise))
        result <- ref.get
      } yield result must beEqualTo(1)
    }

    "not execute finalize action until last element" in {
      val input = List("error-1".invalid, 42.valid, "error-2".invalid)
      for {
        ref <- Ref.of[IO, Int](0)
        payload = Payload(input, ref.update(_ + 1))
        parsed <- payload.decompose[String, Int].compile.toList
        _ <- parsed.init.traverse_(_.fold(_.finalise, _.finalise))
        result <- ref.get
      } yield result must beEqualTo(0)
    }
  }
}
