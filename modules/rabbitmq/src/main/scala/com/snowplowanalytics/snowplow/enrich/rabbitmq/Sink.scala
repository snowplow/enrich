/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.rabbitmq

import cats.implicits._
import cats.Parallel

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Sync}

import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.model._
import dev.profunktor.fs2rabbit.config.declaration._

import com.snowplowanalytics.snowplow.enrich.common.fs2.{AttributedByteSink, AttributedData, ByteSink}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Output

object Sink {

  def init[F[_]: ConcurrentEffect: ContextShift: Parallel: Sync](
    blocker: Blocker,
    output: Output
  ): Resource[F, ByteSink[F]] =
    for {
      sink <- initAttributed(blocker, output)
    } yield records => sink(records.map(AttributedData(_, Map.empty)))

  def initAttributed[F[_]: ConcurrentEffect: ContextShift: Parallel: Sync](
    blocker: Blocker,
    output: Output
  ): Resource[F, AttributedByteSink[F]] =
    output match {
      case o: Output.RabbitMQ =>
        val mapped = mapConfig(o)
        initSink[F](blocker, o, mapped)
      case o =>
        Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Output $o is not RabbitMQ")))
    }

  private def initSink[F[_]: ConcurrentEffect: ContextShift: Parallel](
    blocker: Blocker,
    rawConfig: Output.RabbitMQ,
    config: Fs2RabbitConfig
  ): Resource[F, AttributedByteSink[F]] =
    for {
      client <- Resource.eval(createClient[F](blocker, config))
      channel <- client.createConnectionChannel
      publisher <- Resource.eval {
                     implicit val ch = channel
                     val exchangeName = ExchangeName(rawConfig.exchangeName)
                     val routingKey = RoutingKey(rawConfig.routingKey)
                     val queueName = QueueName(rawConfig.queueName)
                     val queueConfig =
                       DeclarationQueueConfig(
                         queueName,
                         Durable,
                         NonExclusive,
                         NonAutoDelete,
                         Map.empty
                       )
                     client.declareExchange(exchangeName, ExchangeType.Topic) *>
                       client.declareQueue(queueConfig) *>
                       client.bindQueue(queueName, exchangeName, RoutingKey(rawConfig.routingKey)) *>
                       client.createPublisher[String](exchangeName, routingKey)
                   }
      sink = (records: List[AttributedData[Array[Byte]]]) =>
               records
                 .map(_.data)
                 .parTraverse_(bytes => publisher(new String(bytes)))
    } yield sink
}
