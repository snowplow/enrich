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

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import org.specs2.mutable.Specification

import scala.concurrent.duration._

class CircuitBreakerSpec extends Specification with CatsEffect {

  val testConfig = Config.Identity.CircuitBreakerConfig(
    maxConsecutiveFailures = 3,
    failureRateThreshold = 0.5,
    failureRateWindow = 1.minute,
    minRequestsForRateCheck = 5,
    initialBackoff = 100.millis,
    maxBackoff = 1.second,
    backoffMultiplier = 2.0
  )

  "CircuitBreaker" should {
    "start in Closed state" in {
      val test = for {
        cb <- CircuitBreaker.create[IO](testConfig)
        state <- cb.getState
      } yield state

      test.map(_ must beEqualTo(CircuitBreaker.State.Closed))
    }

    "open circuit after consecutive failures" in {
      val test = for {
        cb <- CircuitBreaker.create[IO](testConfig)
        _ <- cb.protect(IO.raiseError(new Exception("fail 1")))
        _ <- cb.protect(IO.raiseError(new Exception("fail 2")))
        _ <- cb.protect(IO.raiseError(new Exception("fail 3")))
        state <- cb.getState
      } yield state

      test.map(_ must beLike { case CircuitBreaker.State.Open(_) => ok })
    }

    "reset consecutive failures on success" in {
      val test = for {
        cb <- CircuitBreaker.create[IO](testConfig)
        _ <- cb.protect(IO.raiseError(new Exception("fail 1")))
        _ <- cb.protect(IO.raiseError(new Exception("fail 2")))
        _ <- cb.protect(IO.pure("success"))
        _ <- cb.protect(IO.raiseError(new Exception("fail 3")))
        _ <- cb.protect(IO.pure("success 2"))
        _ <- cb.protect(IO.pure("success 3"))
        state <- cb.getState
      } yield state

      test.map(_ must beEqualTo(CircuitBreaker.State.Closed))
    }

    "open circuit on failure rate threshold" in {
      val test = for {
        cb <- CircuitBreaker.create[IO](testConfig)
        _ <- cb.protect(IO.pure("success 1"))
        _ <- cb.protect(IO.raiseError(new Exception("fail 1")))
        _ <- cb.protect(IO.pure("success 2"))
        _ <- cb.protect(IO.raiseError(new Exception("fail 2")))
        _ <- cb.protect(IO.raiseError(new Exception("fail 3")))
        state <- cb.getState
      } yield state

      test.map(_ must beLike { case CircuitBreaker.State.Open(_) => ok })
    }

    "not trigger rate check with insufficient requests" in {
      val test = for {
        cb <- CircuitBreaker.create[IO](testConfig)
        _ <- cb.protect(IO.raiseError(new Exception("fail 1")))
        _ <- cb.protect(IO.raiseError(new Exception("fail 2")))
        state <- cb.getState
      } yield state

      test.map(_ must beEqualTo(CircuitBreaker.State.Closed))
    }

    "close circuit after successful half-open test" in {
      val test = for {
        cb <- CircuitBreaker.create[IO](testConfig)
        _ <- cb.protect(IO.raiseError(new Exception("fail 1")))
        _ <- cb.protect(IO.raiseError(new Exception("fail 2")))
        _ <- cb.protect(IO.raiseError(new Exception("fail 3")))
        _ <- IO.sleep(150.millis)
        _ <- cb.protect(IO.pure("recovery success"))
        state <- cb.getState
      } yield state

      test.map(_ must beEqualTo(CircuitBreaker.State.Closed))
    }

    "reopen circuit after failed half-open test" in {
      val test = for {
        cb <- CircuitBreaker.create[IO](testConfig)
        _ <- cb.protect(IO.raiseError(new Exception("fail 1")))
        _ <- cb.protect(IO.raiseError(new Exception("fail 2")))
        _ <- cb.protect(IO.raiseError(new Exception("fail 3")))
        _ <- IO.sleep(150.millis)
        _ <- cb.protect(IO.raiseError(new Exception("recovery fail")))
        state <- cb.getState
      } yield state

      test.map(_ must beLike { case CircuitBreaker.State.Open(_) => ok })
    }
  }
}
