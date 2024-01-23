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
package com.snowplowanalytics.snowplow.enrich.nsq

import cats.syntax.all._

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.{Dispatcher, Queue}

import fs2.Stream

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.client.nsq._
import com.snowplowanalytics.client.nsq.callbacks._
import com.snowplowanalytics.client.nsq.exceptions.NSQException
import com.snowplowanalytics.client.nsq.lookup.DefaultNSQLookup

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Input

object Source {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def init[F[_]: Async](
    input: Input
  ): Stream[F, Record[F]] =
    input match {
      case config: Input.Nsq =>
        for {
          queue <- Stream.eval(Queue.bounded[F, Either[Throwable, Record[F]]](config.maxBufferQueueSize))
          dispatcher <- Stream.resource(Dispatcher.sequential(await = true))
          _ <- Stream.resource(startConsumer(queue, dispatcher, config))
          either <- Stream.fromQueueUnterminated(queue)
          record <- Stream.fromEither[F](either)
        } yield record
      case i =>
        Stream.raiseError[F](new IllegalArgumentException(s"Input $i is not NSQ"))
    }

  private def startConsumer[F[_]: Sync](
    queue: Queue[F, Either[Throwable, Record[F]]],
    dispatcher: Dispatcher[F],
    config: Input.Nsq
  ): Resource[F, NSQConsumer] =
    Resource.make(
      Sync[F].delay {
        val consumer = createConsumer(queue, dispatcher, config)
        consumer.start()
      }
    )(consumer =>
      Sync[F]
        .blocking(consumer.shutdown())
        .handleErrorWith(e => Logger[F].error(e)(s"Cannot terminate NSQ consumer"))
    )

  private def createConsumer[F[_]: Sync](
    queue: Queue[F, Either[Throwable, Record[F]]],
    dispatcher: Dispatcher[F],
    config: Input.Nsq
  ): NSQConsumer = {
    val messageCallback = new NSQMessageCallback {
      override def message(message: NSQMessage): Unit = {
        val msgBytes = message.getMessage
        val enqueue = queue.offer(Right(Record(msgBytes, Sync[F].delay(message.finished()))))
        dispatcher.unsafeRunAndForget(enqueue)
      }
    }
    val errorCallback = new NSQErrorCallback {
      override def error(e: NSQException): Unit = {
        val enqueue = queue.offer(Left(e))
        dispatcher.unsafeRunAndForget(enqueue)
      }
    }
    val lookup = new DefaultNSQLookup
    lookup.addLookupAddress(config.lookupHost, config.lookupPort)
    new NSQConsumer(
      lookup,
      config.topic,
      config.channel,
      messageCallback,
      new NSQConfig(),
      errorCallback
    )
  }
}
