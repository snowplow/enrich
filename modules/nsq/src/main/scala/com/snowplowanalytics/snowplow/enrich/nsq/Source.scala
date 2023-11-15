/*
 * Copyright (c) 2023-2023 Snowplow Analytics Ltd. All rights reserved.
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
