/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import scala.concurrent.duration._

import cats.effect.Clock
import cats.effect.IO

import cats.effect.testing.specs2.CatsEffect

import io.circe.Json
import io.circe.literal.JsonStringContext

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.enrich.common.enrichments.CachingEvaluatorSpec.{TestClock, TestContext}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.CachingEvaluator
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.CachingEvaluator._
import cats.Applicative

class CachingEvaluatorSpec extends Specification with CatsEffect {

  private val successTtl = 5
  private val errorTtl = 2

  "Cached evaluation should work when" >> {
    "TTL is not exceeded, second call not evaluated" >> {
      "for success" in {
        for {
          context <- setupContext()
          v1 <- getValue(context, ifEvaluated = successful(result = json""" { "field": "value1" } """))
          _ <- IO(context.addSeconds(4)) // for success => 4 < 5
          v2 <- getValue(context, ifEvaluated = successful(result = json""" { "field": "value2" } """))
        } yield {
          v1 must beRight(json""" { "field": "value1" } """)
          v2 must beRight(json""" { "field": "value1" } """)
        }
      }

      "for errors" in {
        for {
          context <- setupContext()
          v1 <- getValue(context, ifEvaluated = error(new RuntimeException("Some error1!")))
          _ <- IO(context.addSeconds(1)) // for error => 1 < 2
          v2 <- getValue(context, ifEvaluated = error(new RuntimeException("This second error should not be evaluated!")))
        } yield {
          v1.left.map(_.getMessage) must beLeft("Some error1!")
          v2.left.map(_.getMessage) must beLeft("Some error1!")
        }
      }
    }

    "TTL is exceeded, second call is evaluated" >> {
      "1 call - success, 2 call - success => use new json" in {
        for {
          context <- setupContext()
          v1 <- getValue(context, ifEvaluated = successful(result = json""" { "field": "value1" } """))
          _ <- IO(context.addSeconds(6)) // for success => 6 > 5
          v2 <- getValue(context, ifEvaluated = successful(result = json""" { "field": "value2" } """))
        } yield {
          v1 must beRight(json""" { "field": "value1" } """)
          v2 must beRight(json""" { "field": "value2" } """)
        }
      }: Unit

      "1 call - success, 2 call - error => fallback to previous success, still TTL for errors in force " in {
        for {
          context <- setupContext()
          v1 <- getValue(context, ifEvaluated = successful(result = json""" { "field": "value1" } """))
          _ <- IO(context.addSeconds(6)) // for success => 6 > 5
          v2 <- getValue(context, ifEvaluated = error(new RuntimeException("This second error should be evaluated but not returned!")))
          _ <- IO(context.addSeconds(3)) // for error => 3 > 2
          v3 <- getValue(context, ifEvaluated = successful(result = json""" { "field": "value2" } """))
        } yield {
          v1 must beRight(json""" { "field": "value1" } """)
          v2 must beRight(json""" { "field": "value1" } """)
          v3 must beRight(json""" { "field": "value2" } """)
        }
      }: Unit

      "1 call - error, 2 call - error => use new error" in {
        for {
          context <- setupContext()
          v1 <- getValue(context, ifEvaluated = error(new RuntimeException("Some error1!")))
          _ <- IO(context.addSeconds(3)) // for error => 3 > 2
          v2 <- getValue(context, ifEvaluated = error(new RuntimeException("This second error should be evaluated!")))
        } yield {
          v1.left.map(_.getMessage) must beLeft("Some error1!")
          v2.left.map(_.getMessage) must beLeft("This second error should be evaluated!")
        }
      }

      "1 call - error, 2 call - success => use new json" in {
        for {
          context <- setupContext()
          v1 <- getValue(context, ifEvaluated = error(new RuntimeException("Some error1!")))
          _ <- IO(context.addSeconds(3)) // for error => 3 > 2
          v2 <- getValue(context, ifEvaluated = successful(result = json""" { "field": "value2" } """))
        } yield {
          v1.left.map(_.getMessage) must beLeft("Some error1!")
          v2 must beRight(json""" { "field": "value2" } """)
        }
      }
    }
  }: Unit

  private def getValue(context: TestContext, ifEvaluated: GetResult[IO, Json]): IO[Either[Throwable, Json]] = {
    implicit val clock: TestClock = context.clock
    context.evaluation.evaluateForKey("key", ifEvaluated)
  }

  private def setupContext(): IO[TestContext] =
    for {
      evaluator <- CachingEvaluator.create[IO, String, Json](Config(size = 1, successTtl, errorTtl))
      context = TestContext(new TestClock, evaluator)
    } yield context

  private def successful(result: Json): GetResult[IO, Json] = () => IO.pure(Right(result))
  private def error(ex: Throwable): GetResult[IO, Json] = () => IO.pure(Left(ex))

}

object CachingEvaluatorSpec {

  final case class TestContext(clock: TestClock, evaluation: CachingEvaluator[IO, String, Json]) {
    def addSeconds(value: Int): Unit =
      clock.secondsCounter += value
  }

  final class TestClock extends Clock[IO] {
    var secondsCounter: Long = 0

    override def applicative = Applicative[IO]
    override def realTime: IO[FiniteDuration] = IO.pure(secondsCounter.seconds)
    override def monotonic: IO[FiniteDuration] = IO.pure(secondsCounter.seconds)
  }
}
