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

//import cats.implicits._

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Sync, Timer}

import fs2.Stream
import fs2.concurrent.Queue

import fs2.aws.kinesis.{CommittableRecord, Kinesis, KinesisCheckpointSettings}

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

import com.snowplowanalytics.snowplow.enrich.common.fs2.{Payload, RawSource}

object Source {

  def init[F[_]: ConcurrentEffect: ContextShift: Sync: Timer]: RawSource[F] = {
    def region: Region = ???

    for {
      blocker <- Stream.resource(Blocker[F])
      kinesisClient <- Stream.resource(makeKinesisClient[F](region))
      dynamoClient <- Stream.resource(makeDynamoDbClient[F](region))
      cloudWatchClient <- Stream.resource(makeCloudWatchClient[F](region))
      kinesis = Kinesis.create(kinesisClient, dynamoClient, cloudWatchClient, blocker)
      stream = kinesis.readFromKinesisStream("THIS DOES NOTHING", "THIS DOES NOTHING")
      checkpointSettings = KinesisCheckpointSettings.defaultInstance
      checkpointer = kinesis.checkpointRecords(checkpointSettings)
      toCommit <- Stream.eval(Queue.bounded[F, CommittableRecord](checkpointSettings.maxBatchSize))
      rawSource = stream.map(r => Payload(r.record.data().array(), toCommit.enqueue1(r)))
      record <- rawSource.concurrently(toCommit.dequeue.through(checkpointer))
    } yield record
  }

  def makeKinesisClient[F[_]: Sync](region: Region): Resource[F, KinesisAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        KinesisAsyncClient.builder()
          .region(region)
          .build
      }
    }

  def makeDynamoDbClient[F[_]: Sync](region: Region): Resource[F, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        DynamoDbAsyncClient.builder()
          .region(region)
          .build
      }
    }

  def makeCloudWatchClient[F[_]: Sync](region: Region): Resource[F, CloudWatchAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        CloudWatchAsyncClient.builder()
          .region(region)
          .build
      }
    }

  //def scheduler[F[_]: Sync](kinesisClient: KinesisAsyncClient,
  //                          dynamoDbClient: DynamoDbAsyncClient,
  //                          cloudWatchClient: CloudWatchAsyncClient,
  //                          recordProcessorFactory: ShardRecordProcessorFactory): F[Scheduler] =
  //  Sync[F].delay(UUID.randomUUID()).map { uuid =>
  //    val hostname = InetAddress.getLocalHost().getCanonicalHostName()

  //    val configsBuilder =
  //      new ConfigsBuilder(config.streamName,
  //                         config.appName,
  //                         kinesisClient,
  //                         dynamoDbClient,
  //                         cloudWatchClient,
  //                         s"$hostname:$uuid",
  //                         recordProcessorFactory)

  //    val retrievalConfig =
  //      configsBuilder
  //        .retrievalConfig
  //        .initialPositionInStreamExtended(config.initialPosition.unwrap)
  //        .retrievalSpecificConfig {
  //          config.retrievalMode match {
  //            case Source.Kinesis.Retrieval.FanOut =>
  //              new FanOutConfig(kinesisClient)
  //            case Source.Kinesis.Retrieval.Polling(maxRecords) =>
  //              new PollingConfig(config.streamName, kinesisClient).maxRecords(maxRecords)
  //          }
  //        }

  //    val metricsConfig = configsBuilder.metricsConfig.metricsLevel {
  //      if (metrics.cloudWatch) MetricsLevel.DETAILED else MetricsLevel.NONE
  //    }

  //    new Scheduler(
  //      configsBuilder.checkpointConfig,
  //      configsBuilder.coordinatorConfig,
  //      configsBuilder.leaseManagementConfig,
  //      configsBuilder.lifecycleConfig,
  //      metricsConfig,
  //      configsBuilder.processorConfig,
  //      retrievalConfig
  //    )
  //  }
}
