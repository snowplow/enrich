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
package com.snowplowanalytics.snowplow.enrich.kinesis.it

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Sync, Timer}

import cats.implicits._

import fs2.Stream

import fs2.aws.kinesis.Kinesis

import java.util.UUID
import java.net.InetAddress

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.kinesis.common.{ConfigsBuilder, InitialPositionInStream, InitialPositionInStreamExtended}
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.metrics.MetricsLevel
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.polling.PollingConfig

import com.snowplowanalytics.snowplow.enrich.kinesis.KinesisRun

object KinesisSource {

  def init[F[_]: ConcurrentEffect: ContextShift: Timer](blocker: Blocker, streamName: String, appName: String): Stream[F, Array[Byte]] = {
    val awsRegion = Region.of(Resources.region)

    val kinesis: Resource[F, Kinesis[F]] = for {
      kinesisClient    <- Resource.fromAutoCloseable(Sync[F].delay(KinesisAsyncClient.builder().region(awsRegion).build()))
      dynamoDbClient   <- Resource.fromAutoCloseable(Sync[F].delay(DynamoDbAsyncClient.builder().region(awsRegion).build()))
      cloudWatchClient <- Resource.fromAutoCloseable(Sync[F].delay(CloudWatchAsyncClient.builder().region(awsRegion).build()))
    } yield Kinesis.create(blocker, scheduler(streamName, appName, kinesisClient, dynamoDbClient, cloudWatchClient, _))

    Stream.resource(kinesis).flatMap { kinesis =>
      kinesis.readFromKinesisStream("", "")
        .map(r => KinesisRun.getPayload(r))
    }
  }

  private def scheduler[F[_]: Sync](
    streamName: String,
    appName: String,
    kinesisClient: KinesisAsyncClient,
    dynamoDbClient: DynamoDbAsyncClient,
    cloudWatchClient: CloudWatchAsyncClient,
    recordProcessorFactory: ShardRecordProcessorFactory
  ): F[Scheduler] =
    Sync[F].delay(UUID.randomUUID()).map { uuid =>
      val hostname = InetAddress.getLocalHost().getCanonicalHostName()

      val configsBuilder =
        new ConfigsBuilder(streamName,
                           appName,
                           kinesisClient,
                           dynamoDbClient,
                           cloudWatchClient,
                           s"$hostname:$uuid",
                           recordProcessorFactory
        )

      val retrievalConfig =
        configsBuilder.retrievalConfig
          .initialPositionInStreamExtended(
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
          )
          .retrievalSpecificConfig(
            new PollingConfig(streamName, kinesisClient).maxRecords(1000)
          )

      new Scheduler(
        configsBuilder.checkpointConfig,
        configsBuilder.coordinatorConfig,
        configsBuilder.leaseManagementConfig,
        configsBuilder.lifecycleConfig,
        configsBuilder.metricsConfig.metricsLevel(MetricsLevel.NONE),
        configsBuilder.processorConfig,
        retrievalConfig
      )
    }
}
