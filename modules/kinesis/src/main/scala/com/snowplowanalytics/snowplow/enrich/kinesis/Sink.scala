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

import cats.effect.{Async, Concurrent, ContextShift, Sync, Timer}

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import fs2.aws.internal.{KinesisProducerClient, KinesisProducerClientImpl}

import retry.syntax.all._
import retry.RetryPolicies._

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture, MoreExecutors}

import com.amazonaws.services.kinesis.producer.{KinesisProducerConfiguration, UserRecordResult}

import com.snowplowanalytics.snowplow.enrich.common.fs2.{AttributedByteSink, AttributedData, ByteSink}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Monitoring, Output}

object Sink {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def init[F[_]: Concurrent: ContextShift: Timer](
    output: Output,
    monitoring: Option[Monitoring]
  ): F[ByteSink[F]] =
    output match {
      case o: Output.Kinesis =>
        o.region.orElse(getRuntimeRegion) match {
          case Some(region) =>
            val sink = kinesis[F](o, region, monitoring)
            Sync[F].pure(bytes => sink(AttributedData(bytes, Map.empty)))
          case None =>
            Sync[F].raiseError(new IllegalArgumentException(s"Region not found in the config and in the runtime"))
        }
      case o =>
        Sync[F].raiseError(new IllegalArgumentException(s"Output $o is not Kinesis"))
    }

  def initAttributed[F[_]: Concurrent: ContextShift: Timer](
    output: Output,
    monitoring: Option[Monitoring]
  ): F[AttributedByteSink[F]] =
    output match {
      case o: Output.Kinesis =>
        o.region.orElse(getRuntimeRegion) match {
          case Some(region) =>
            Sync[F].pure(kinesis[F](o, region, monitoring))
          case None =>
            Sync[F].raiseError(new IllegalArgumentException(s"Region not found in the config and in the runtime"))
        }
      case o =>
        Sync[F].raiseError(new IllegalArgumentException(s"Output $o is not Kinesis"))
    }

  private def kinesis[F[_]: Async: ContextShift: Timer](
    config: Output.Kinesis,
    region: String,
    monitoring: Option[Monitoring]
  ): AttributedByteSink[F] = {
    val producer = mkProducer[F](config, region, monitoring)
    data => writeToKinesis[F](config, producer, data)
  }

  private def mkProducer[F[_]](
    config: Output.Kinesis,
    region: String,
    monitoring: Option[Monitoring]
  ): KinesisProducerClient[F] = {
    val disableCloudwatch = monitoring.fold(false)(m => m.metrics.fold(false)(r => r.cloudwatch.contains(true)))
    val metricsLevel = if (disableCloudwatch) "none" else "detailed"

    val producerConfig = new KinesisProducerConfiguration()
      .setThreadingModel(KinesisProducerConfiguration.ThreadingModel.POOLED)
      .setRegion(region)
      .setMetricsLevel(metricsLevel)
      .setRecordMaxBufferedTime(config.maxBufferedTime.toMillis)
      .setCollectionMaxCount(config.collection.maxCount)
      .setCollectionMaxSize(config.collection.maxSize)

    // See https://docs.aws.amazon.com/streams/latest/dev/kinesis-kpl-concepts.html
    val withAggregation = config.aggregation match {
      case Some(agg) =>
        producerConfig
          .setAggregationMaxCount(agg.maxCount)
          .setAggregationMaxSize(agg.maxSize)
      case None =>
        producerConfig
          .setAggregationEnabled(false)
    }

    new KinesisProducerClientImpl[F](Some(withAggregation))
  }

  private def writeToKinesis[F[_]: Async: ContextShift: Timer](
    config: Output.Kinesis,
    producer: KinesisProducerClient[F],
    data: AttributedData[Array[Byte]]
  ): F[Unit] = {
    val retryPolicy = capDelay[F](config.backoffPolicy.maxBackoff, exponentialBackoff[F](config.backoffPolicy.minBackoff))
    val partitionKey = data.attributes.toList match { // there can be only one attribute : the partition key
      case head :: Nil => head._2
      case _ => UUID.randomUUID().toString
    }
    val res = for {
      byteBuffer <- Async[F].delay(ByteBuffer.wrap(data.data))
      cb <- producer.putData(config.streamName, partitionKey, byteBuffer)
      cbRes <- registerCallback(cb)
      _ <- ContextShift[F].shift
    } yield cbRes

    res
      .retryingOnFailuresAndAllErrors(
        wasSuccessful = _.isSuccessful,
        policy = retryPolicy,
        onFailure = (result, retryDetails) =>
          Logger[F].warn(s"Writing to shard ${result.getShardId()} failed after ${retryDetails.retriesSoFar} retry"),
        onError = (exception, retryDetails) =>
          Logger[F]
            .error(s"Writing to Kinesis errored after ${retryDetails.retriesSoFar} retry. Error: ${exception.toString}") >>
            Async[F].raiseError(exception)
      )
      .void
  }

  private def registerCallback[F[_]: Async](f: ListenableFuture[UserRecordResult]): F[UserRecordResult] =
    Async[F].async[UserRecordResult] { cb =>
      Futures.addCallback(
        f,
        new FutureCallback[UserRecordResult] {
          override def onFailure(t: Throwable): Unit = cb(Left(t))
          override def onSuccess(result: UserRecordResult): Unit = cb(Right(result))
        },
        MoreExecutors.directExecutor
      )
    }
}
