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
package com.snowplowanalytics.snowplow.enrich.pubsub

import java.util.{Date, UUID}
import java.net.InetAddress

import cats.implicits._

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Sync, Timer}

import fs2.{Stream, Pipe}

import fs2.aws.kinesis.{CommittableRecord, Kinesis, KinesisCheckpointSettings}

import software.amazon.awssdk.regions.Region
import software.amazon.kinesis.common.{ConfigsBuilder, InitialPositionInStream, InitialPositionInStreamExtended}
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.metrics.MetricsLevel
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.polling.PollingConfig
import software.amazon.kinesis.retrieval.fanout.FanOutConfig
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Authentication, Input, Monitoring}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Input.Kinesis.InitPosition

object Source {

  def init[F[_]: ConcurrentEffect: ContextShift: Timer](
    blocker: Blocker,
    auth: Authentication,
    input: Input,
    monitoring: Option[Monitoring]
  ): (Stream[F, CommittableRecord], Resource[F, Pipe[F, CommittableRecord, Unit]]) =
    (auth, input) match {
      case (Authentication.Aws, k: Input.Kinesis) =>
        kinesis(blocker, k, monitoring)
      case (auth, input) =>
        throw new IllegalArgumentException(s"Auth $auth is not GCP and/or input $input is not PubSub")
    }

  def kinesis[F[_]: ConcurrentEffect: ContextShift: Sync: Timer](
    blocker: Blocker,
    kinesisConfig: Input.Kinesis,
    monitoring: Option[Monitoring]
  ): (Stream[F, CommittableRecord], Resource[F, Pipe[F, CommittableRecord, Unit]]) = {
    val checkpointSettings = 
      KinesisCheckpointSettings(kinesisConfig.checkpointing.maxBatchSize, kinesisConfig.checkpointing.maxBatchWait) match {
        case Left(err) => throw err
        case Right(settings) => settings 
      }

    val kinesis = for {
      region <- Resource.eval(Sync[F].pure(Region.of(kinesisConfig.region)))
      kinesisClient <- makeKinesisClient[F](region)
      dynamoClient <- makeDynamoDbClient[F](region)
      cloudWatchClient <- makeCloudWatchClient[F](region)
    } yield Kinesis.create(blocker, scheduler(kinesisClient, dynamoClient, cloudWatchClient, kinesisConfig, monitoring, _))
    
    val stream = for {
      k <- Stream.resource(kinesis)
      record <- k.readFromKinesisStream("THIS DOES NOTHING", "THIS DOES NOTHING")
    } yield record
    val checkpointer = kinesis.map(_.checkpointRecords(checkpointSettings).andThen(_.as(())))

    (stream, checkpointer)
  }

  private def scheduler[F[_]: Sync](
    kinesisClient: KinesisAsyncClient,
    dynamoDbClient: DynamoDbAsyncClient,
    cloudWatchClient: CloudWatchAsyncClient,
    kinesisConfig: Input.Kinesis,
    monitoring: Option[Monitoring],
    recordProcessorFactory: ShardRecordProcessorFactory
  ): F[Scheduler] =
    Sync[F].delay(UUID.randomUUID()).map { uuid =>
      val hostname = InetAddress.getLocalHost().getCanonicalHostName()

      val configsBuilder =
        new ConfigsBuilder(
          kinesisConfig.streamName,
          kinesisConfig.appName,
          kinesisClient,
          dynamoDbClient,
          cloudWatchClient,
          s"$hostname:$uuid",
          recordProcessorFactory)

      val initPositionExtended = kinesisConfig.initialPosition match {
        case InitPosition.Latest =>
          InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
        case InitPosition.TrimHorizon =>
          InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
        case InitPosition.AtTimestamp(timestamp) =>
          InitialPositionInStreamExtended.newInitialPositionAtTimestamp(Date.from(timestamp))
      }

      val retrievalConfig =
        configsBuilder
          .retrievalConfig
          .initialPositionInStreamExtended(initPositionExtended)
          .retrievalSpecificConfig {
            kinesisConfig.retrievalMode match {
              case Input.Kinesis.Retrieval.FanOut =>
                new FanOutConfig(kinesisClient).streamName(kinesisConfig.streamName).applicationName(kinesisConfig.appName)
              case Input.Kinesis.Retrieval.Polling(maxRecords) =>
                new PollingConfig(kinesisConfig.streamName, kinesisClient).maxRecords(maxRecords)
            }
          }

      val metricsConfig = configsBuilder.metricsConfig.metricsLevel {
        val disableCloudwatch = monitoring.fold(false)(m => m.metrics.fold(false)(r => r.cloudwatch.contains(true)))
        if (disableCloudwatch) MetricsLevel.NONE else MetricsLevel.DETAILED
      }

      new Scheduler(
        configsBuilder.checkpointConfig,
        configsBuilder.coordinatorConfig,
        configsBuilder.leaseManagementConfig,
        configsBuilder.lifecycleConfig,
        metricsConfig,
        configsBuilder.processorConfig,
        retrievalConfig
      )
    }

  private def makeKinesisClient[F[_]: Sync](region: Region): Resource[F, KinesisAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        KinesisAsyncClient.builder()
          .region(region)
          .build
      }
    }

  private def makeDynamoDbClient[F[_]: Sync](region: Region): Resource[F, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        DynamoDbAsyncClient.builder()
          .region(region)
          .build
      }
    }

  private def makeCloudWatchClient[F[_]: Sync](region: Region): Resource[F, CloudWatchAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        CloudWatchAsyncClient.builder()
          .region(region)
          .build
      }
    }
}
