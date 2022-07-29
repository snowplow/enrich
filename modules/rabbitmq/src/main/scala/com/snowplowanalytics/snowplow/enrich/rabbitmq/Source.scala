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

import cats.Applicative
import cats.data.Kleisli
import cats.implicits._

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Sync}

import fs2.Stream

import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.model._
import dev.profunktor.fs2rabbit.config.declaration._
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.effects.EnvelopeDecoder

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Input

object Source {

  def init[F[_]: ConcurrentEffect: ContextShift](
    blocker: Blocker,
    input: Input
  ): Stream[F, Record[F]] =
    input match {
      case r: Input.RabbitMQ =>
        val mapped = mapConfig(r)
        initSource[F](blocker, r, mapped)
      case i =>
        Stream.raiseError[F](new IllegalArgumentException(s"Input $i is not RabbitMQ"))
    }

  private def initSource[F[_]: ConcurrentEffect: ContextShift](
    blocker: Blocker,
    rawConfig: Input.RabbitMQ,
    config: Fs2RabbitConfig
  ): Stream[F, Record[F]] =
    for {
      client <- Stream.eval[F, RabbitClient[F]](createClient[F](blocker, config))
      records <- createStreamFromClient(client, rawConfig)
    } yield records

  private def createStreamFromClient[F[_]: Sync](
    client: RabbitClient[F],
    rawConfig: Input.RabbitMQ
  ): Stream[F, Record[F]] =
    Stream.resource(client.createConnectionChannel).flatMap { implicit channel =>
      val exchangeName = ExchangeName(rawConfig.exchangeName)
      val exchangeConfig =
        DeclarationExchangeConfig(
          exchangeName,
          ExchangeType.Topic,
          Durable,
          NonAutoDelete,
          NonInternal,
          Map.empty
        )
      val queueName = QueueName(rawConfig.queueName)
      val queueConfig =
        DeclarationQueueConfig(
          queueName,
          Durable,
          NonExclusive,
          NonAutoDelete,
          Map.empty
        )

      for {
        _ <- Stream.eval(client.declareExchange(exchangeConfig))
        _ <- Stream.eval(client.declareQueue(queueConfig))
        _ <- Stream.eval(client.bindQueue(queueName, exchangeName, RoutingKey(rawConfig.routingKey)))
        stream <- Stream.eval(client.createAutoAckConsumer[Array[Byte]](queueName))
        records <- stream.map(env => Record(env.payload, Sync[F].unit))
      } yield records
    }

  implicit def bytesDecoder[F[_]: Applicative]: EnvelopeDecoder[F, Array[Byte]] =
    Kleisli(_.payload.pure[F])
}
