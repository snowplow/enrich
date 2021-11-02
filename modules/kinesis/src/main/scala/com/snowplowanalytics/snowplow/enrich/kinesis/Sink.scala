/*
 * Copyright (c) 2021-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.kinesis

import java.nio.ByteBuffer
import java.util.UUID

import cats.implicits._

import cats.effect.{Async, Concurrent, ContextShift, Resource, Sync, Timer}

import fs2.aws.internal.{KinesisProducerClient, KinesisProducerClientImpl}

import com.amazonaws.services.kinesis.producer.{KinesisProducerConfiguration}

import com.snowplowanalytics.snowplow.enrich.common.fs2.{AttributedByteSink, AttributedData, ByteSink}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Monitoring, Output}

object Sink {

  def init[F[_]: Concurrent: ContextShift: Timer](
    output: Output,
    monitoring: Option[Monitoring]
  ): Resource[F, ByteSink[F]] =
    output match {
      case o: Output.Kinesis =>
        o.region.orElse(getRuntimeRegion) match {
          case Some(region) =>
            kinesis[F](o, region, monitoring).map(sink => bytes => sink(AttributedData(bytes, Map.empty)))
          case None =>
            Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Region not found in the config and in the runtime")))
        }
      case o =>
        Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Output $o is not Kinesis")))
    }

  def initAttributed[F[_]: Concurrent: ContextShift: Timer](
    output: Output,
    monitoring: Option[Monitoring]
  ): Resource[F, AttributedByteSink[F]] =
    output match {
      case o: Output.Kinesis =>
        o.region.orElse(getRuntimeRegion) match {
          case Some(region) =>
            kinesis[F](o, region, monitoring)
          case None =>
            Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Region not found in the config and in the runtime")))
        }
      case o =>
        Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Output $o is not Kinesis")))
    }

  private def kinesis[F[_]: Async: Timer](
    config: Output.Kinesis,
    region: String,
    monitoring: Option[Monitoring]
  ): Resource[F, AttributedData[Array[Byte]] => F[Unit]] =
    mkProducer(config, region, monitoring).map(writeToKinesis(config))

  private def mkProducer[F[_]: Sync](
    config: Output.Kinesis,
    region: String,
    monitoring: Option[Monitoring]
  ): Resource[F, KinesisProducerClient[F]] =
    Resource.eval(
      Sync[F].delay(
        new KinesisProducerClientImpl[F](Some(mkProducerConfig(config, region, monitoring)))
      )
    )

  private def mkProducerConfig[F[_]](
    config: Output.Kinesis,
    region: String,
    monitoring: Option[Monitoring]
  ): KinesisProducerConfiguration = {
    val disableCloudwatch = monitoring.fold(false)(m => m.metrics.fold(false)(r => r.cloudwatch.contains(true)))
    val metricsLevel = if (disableCloudwatch) "none" else "detailed"

    new KinesisProducerConfiguration()
      .setThreadingModel(KinesisProducerConfiguration.ThreadingModel.POOLED)
      .setRegion(region)
      .setMetricsLevel(metricsLevel)
      .setRecordMaxBufferedTime(config.delayThreshold.toMillis)
      .setAggregationEnabled(false)
  }

  private def writeToKinesis[F[_]: Async: Timer](
    config: Output.Kinesis
  )(
    producer: KinesisProducerClient[F]
  )(
    data: AttributedData[Array[Byte]]
  ): F[Unit] = {
    val partitionKey = data.attributes.toList match { // there can be only one attribute : the partition key
      case head :: Nil => head._2
      case _ => UUID.randomUUID().toString
    }
    val res = for {
      byteBuffer <- Async[F].delay(ByteBuffer.wrap(data.data))
      cb <- producer.putData(config.streamName, partitionKey, byteBuffer)
    } yield cb
    res.void
  }
}
