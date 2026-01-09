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

import cats.effect.{Async, Ref, Sync}
import cats.syntax.all._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant
import scala.collection.immutable.Queue
import scala.concurrent.duration._

object CircuitBreaker {

  sealed trait State
  object State {
    case object Closed extends State
    case class Open(nextRetry: Instant) extends State
    case object HalfOpen extends State
  }

  case class Stats(
    consecutiveFailures: Int,
    recentResults: Queue[(Instant, Boolean)],
    backoffAttempts: Int
  )

  trait CircuitBreaker[F[_]] {
    def protect[A](fa: F[A]): F[Option[A]]
    def getState: F[State]
  }

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def create[F[_]: Async](
    config: Config.Identity.CircuitBreakerConfig
  ): F[CircuitBreaker[F]] =
    Ref.of[F, (State, Stats)]((State.Closed, Stats(0, Queue.empty, 0))).map { stateRef =>
      new CircuitBreaker[F] {
        override def protect[A](fa: F[A]): F[Option[A]] =
          for {
            state <- checkAndUpdateState
            result <- state match {
                        case State.Closed | State.HalfOpen =>
                          fa.attempt.flatMap[Option[A]] {
                            case Right(value) =>
                              recordSuccess.as(Some(value))
                            case Left(error) =>
                              recordFailure(error).as(None)
                          }
                        case State.Open(_) =>
                          Logger[F].debug("Circuit breaker is open, skipping request").as(None)
                      }
          } yield result

        override def getState: F[State] = stateRef.get.map(_._1)

        private def checkAndUpdateState: F[State] =
          for {
            now <- Async[F].realTimeInstant
            state <- stateRef
                       .modify[F[State]] {
                         case (State.Open(nextRetry), stats) if !now.isBefore(nextRetry) =>
                           val newState = (State.HalfOpen, stats)
                           (newState, Logger[F].info("Circuit breaker transitioning from Open to HalfOpen").as(State.HalfOpen))
                         case (state, stats) =>
                           ((state, stats), state.pure[F])
                       }
                       .flatten
          } yield state

        private def recordSuccess: F[Unit] =
          for {
            now <- Async[F].realTimeInstant
            _ <- stateRef
                   .modify[F[Unit]] {
                     case (State.HalfOpen, stats) =>
                       val pruned = pruneWindow(stats.recentResults, now)
                       val newResults = pruned.enqueue((now, true))
                       val newState = (State.Closed, Stats(0, newResults, 0))
                       (newState, Logger[F].info("Circuit breaker closing after successful request in HalfOpen state"))
                     case (State.Closed, stats) =>
                       val pruned = pruneWindow(stats.recentResults, now)
                       val newResults = pruned.enqueue((now, true))
                       val newState = (State.Closed, stats.copy(consecutiveFailures = 0, backoffAttempts = 0, recentResults = newResults))
                       (newState, ().pure[F])
                     case (state, stats) =>
                       ((state, stats), ().pure[F])
                   }
                   .flatten
          } yield ()

        private def recordFailure(error: Throwable): F[Unit] =
          for {
            now <- Async[F].realTimeInstant
            _ <- stateRef
                   .modify[F[Unit]] {
                     case (state @ (State.Closed | State.HalfOpen), stats) =>
                       val newConsecutive = stats.consecutiveFailures + 1
                       val pruned = pruneWindow(stats.recentResults, now)
                       val newResults = pruned.enqueue((now, false))

                       if (checkShouldOpen(newConsecutive, newResults)) {
                         val backoff = calculateBackoff(stats.backoffAttempts)
                         val nextRetry = now.plusMillis(backoff.toMillis)
                         val newStats = Stats(newConsecutive, newResults, stats.backoffAttempts + 1)
                         val newState = (State.Open(nextRetry), newStats)
                         (newState,
                          Logger[F].error(s"Circuit breaker opening due to failures. Next retry at $nextRetry. Error: ${error.getMessage}")
                         )
                       } else {
                         val newState = (state, Stats(newConsecutive, newResults, stats.backoffAttempts))
                         (newState, ().pure[F])
                       }
                     case (state, stats) =>
                       ((state, stats), ().pure[F])
                   }
                   .flatten
          } yield ()

        private def pruneWindow(results: Queue[(Instant, Boolean)], now: Instant): Queue[(Instant, Boolean)] = {
          val cutoff = now.minusMillis(config.failureRateWindow.toMillis)
          results.dropWhile { case (timestamp, _) => timestamp.isBefore(cutoff) }
        }

        private def checkShouldOpen(consecutiveFailures: Int, recentResults: Queue[(Instant, Boolean)]): Boolean = {
          val consecutiveThreshold = consecutiveFailures >= config.maxConsecutiveFailures

          val rateThreshold = if (recentResults.size >= config.minRequestsForRateCheck) {
            val failures = recentResults.count { case (_, success) => !success }
            val rate = failures.toDouble / recentResults.size.toDouble
            rate >= config.failureRateThreshold
          } else
            false

          consecutiveThreshold || rateThreshold
        }

        private def calculateBackoff(attempts: Int): FiniteDuration = {
          val backoffMillis = config.initialBackoff.toMillis * math.pow(config.backoffMultiplier, attempts.toDouble)
          FiniteDuration(math.min(backoffMillis.toLong, config.maxBackoff.toMillis), MILLISECONDS)
        }
      }
    }
}
