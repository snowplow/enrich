/*
 * Copyright (c) 2024-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2

import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.implicits._

import cats.effect.implicits._
import cats.effect.{Async, Deferred, Sync}
import cats.effect.kernel.Resource.ExitCase
import cats.effect.std.Queue

import fs2.{Pipe, Stream}

object CleanCancellation {
  private implicit def logger[F[_]: Sync]: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  /**
   * This is the machinery needed to make sure that pending chunks are sunk and checkpointed when
   * the app terminates.
   *
   * The `source` and `sink` each run in concurrent streams, so they are not immediately cancelled
   * upon receiving a SIGINT
   *
   * The "main" stream just waits for a SIGINT, and then cleanly shuts down the concurrent
   * processes.
   *
   * We use a queue as a level of indirection between the stream of transformed events and the sink
   * + checkpointing. When we receive a SIGINT or exception then we terminate the sink by pushing a
   * `None` to the queue.
   */
  def apply[F[_]: Async, A](
    sinkAndCheckpoint: Pipe[F, A, Unit]
  ): Pipe[F, A, Nothing] =
    source =>
      Stream.eval(Queue.synchronous[F, Option[A]]).flatMap { queue =>
        Stream.eval(Deferred[F, Unit]).flatMap { sig =>
          impl(source, sinkAndCheckpoint, queue, sig)
        }
      }

  private def impl[F[_]: Async, A](
    source: Stream[F, A],
    sinkAndCheckpoint: Pipe[F, A, Unit],
    queue: Queue[F, Option[A]],
    sig: Deferred[F, Unit]
  ): Stream[F, Nothing] =
    Stream
      .eval(sig.get)
      .onFinalizeCase {
        case ExitCase.Succeeded =>
          // Both the source and sink have completed "naturally", e.g. processed all input files in the directory
          Sync[F].unit
        case ExitCase.Canceled =>
          // SIGINT received. We wait for the transformed events already in the queue to get sunk and checkpointed
          terminateStream(queue, sig)
        case ExitCase.Errored(_) =>
          // Runtime exception either in the source or in the sink.
          // The exception is already logged by the concurrent process.
          // We wait for the transformed events already in the queue to get sunk and checkpointed.
          terminateStream(queue, sig).handleErrorWith { e2 =>
            Logger[F].error(e2)("Error when terminating the sink and checkpoint")
          }
      }
      .drain
      .concurrently {
        Stream.bracket(().pure[F])(_ => sig.complete(()).void) >>
          Stream
            .fromQueueNoneTerminated(queue, 1)
            .through(sinkAndCheckpoint)
            .onFinalizeCase {
              case ExitCase.Succeeded =>
                // The queue has completed "naturally", i.e. a `None` got enqueued.
                Logger[F].info("Completed sinking and checkpointing events")
              case ExitCase.Canceled =>
                Logger[F].info("Sinking and checkpointing was cancelled")
              case ExitCase.Errored(e) =>
                Logger[F].error(e)("Error on sinking and checkpointing events")
            }
      }
      .concurrently {
        source
          .evalMap(x => queue.offer(Some(x)))
          .onFinalizeCase {
            case ExitCase.Succeeded =>
              // The source has completed "naturally", e.g. processed all events in this window
              Logger[F].debug("Reached the end of the source of events") *>
                closeQueue(queue)
            case ExitCase.Canceled =>
              Logger[F].info("Source of events was cancelled")
            case ExitCase.Errored(e) =>
              Logger[F].error(e)("Error in the source of events") *>
                terminateStream(queue, sig)
          }
      }

  // Closing the queue allows the sink to finish once all pending events have been flushed.
  // We spawn it as a Fiber because `queue.offer(None)` can hang if the queue is already closed.
  private def closeQueue[F[_]: Async, A](queue: Queue[F, Option[A]]): F[Unit] =
    queue.offer(None).start.void

  private def terminateStream[F[_]: Async, A](queue: Queue[F, Option[A]], sig: Deferred[F, Unit]): F[Unit] =
    for {
      _ <- Logger[F].warn(s"Requesting event processor to terminate cleanly...")
      _ <- closeQueue(queue)
      _ <- sig.get
    } yield ()
}
