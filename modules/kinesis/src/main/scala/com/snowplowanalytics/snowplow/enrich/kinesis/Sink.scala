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
import scala.collection.JavaConverters._

import cats.implicits._

import cats.effect.{Async, Concurrent, ContextShift, Resource, Sync, Timer}

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
    monitoring: Monitoring
  ): Resource[F, ByteSink[F]] =
    output match {
      case o: Output.Kinesis =>
        o.region.orElse(getRuntimeRegion) match {
          case Some(region) =>
            for {
              producer <- Resource.pure[F, KinesisProducerClient[F]](mkProducer[F](o, region, monitoring))
            } yield bytes => writeToKinesis(o, producer, AttributedData(bytes, Map.empty))
          case None =>
            Resource.eval(Sync[F].raiseError(new RuntimeException(s"Region not found in the config and in the runtime")))
        }
      case o =>
        Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Output $o is not Kinesis")))
    }

  def initAttributed[F[_]: Concurrent: ContextShift: Timer](
    output: Output,
    monitoring: Monitoring
  ): Resource[F, AttributedByteSink[F]] =
    output match {
      case o: Output.Kinesis =>
        o.region.orElse(getRuntimeRegion) match {
          case Some(region) =>
            for {
              producer <- Resource.pure[F, KinesisProducerClient[F]](mkProducer[F](o, region, monitoring))
            } yield data => writeToKinesis(o, producer, data)
          case None =>
            Resource.eval(Sync[F].raiseError(new RuntimeException(s"Region not found in the config and in the runtime")))
        }
      case o =>
        Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Output $o is not Kinesis")))
    }

  private def mkProducer[F[_]](
    config: Output.Kinesis,
    region: String,
    monitoring: Monitoring
  ): KinesisProducerClient[F] = {
    val metricsLevel = if (monitoring.metrics.cloudwatch) "detailed" else "none"

    val producerConfig = new KinesisProducerConfiguration()
      .setThreadingModel(KinesisProducerConfiguration.ThreadingModel.POOLED)
      .setRegion(region)
      .setMetricsLevel(metricsLevel)
      .setRecordMaxBufferedTime(config.maxBufferedTime.toMillis)
      .setCollectionMaxCount(config.collection.maxCount)
      .setCollectionMaxSize(config.collection.maxSize)
      .setMaxConnections(config.maxConnections)
      .setLogLevel(config.logLevel)
      .setRecordTtl(Long.MaxValue) // retry records forever

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

    val withKinesisEndpoint =
      (config.customEndpoint, config.customPort)
        .mapN { case (host, port) => withAggregation.setKinesisEndpoint(host.toString).setKinesisPort(port) }
        .getOrElse(producerConfig)

    val withCloudwatchEndpoint =
      (config.cloudwatchEndpoint, config.cloudwatchPort)
        .mapN { case (host, port) => withKinesisEndpoint.setCloudwatchEndpoint(host.toString).setCloudwatchPort(port) }
        .getOrElse(withKinesisEndpoint)

    new KinesisProducerClientImpl[F](Some(withCloudwatchEndpoint))
  }

  private def writeToKinesis[F[_]: Async: ContextShift: Timer](
    config: Output.Kinesis,
    producer: KinesisProducerClient[F],
    data: AttributedData[Array[Byte]]
  ): F[Unit] = {
    val retryPolicy = capDelay[F](config.backoffPolicy.maxBackoff, fullJitter[F](config.backoffPolicy.minBackoff))
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
          Logger[F]
            .warn(
              List(
                s"Writing to shard ${result.getShardId()} failed.",
                s"${result.getAttempts().size()} KPL attempts (${retryDetails.retriesSoFar} retries from cats-retry).",
                s"Errors : [${getErrorMessages(result)}]"
              ).mkString(" ")
            ),
        onError = (exception, retryDetails) =>
          Logger[F]
            .error(exception)(s"Writing to Kinesis errored (${retryDetails.retriesSoFar} retries from cats-retry)")
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

  private def getErrorMessages(result: UserRecordResult): String =
    result.getAttempts.asScala
      .map(_.getErrorMessage)
      .distinct // because of unlimited retry
      .toList
      .toString
}
