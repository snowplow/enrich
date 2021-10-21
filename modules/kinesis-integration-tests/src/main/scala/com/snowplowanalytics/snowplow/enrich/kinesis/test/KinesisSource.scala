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
package com.snowplowanalytics.snowplow.enrich.kinesis.test

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Timer}

import fs2.Stream

import fs2.aws.kinesis.Kinesis

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

object KinesisSource {

  def init[F[_]: ConcurrentEffect: ContextShift: Timer](
    blocker: Blocker,
    region: String,
    stream: String
  ): Stream[F, Array[Byte]] = {
    val awsRegion = Region.of(region)
    val kinesisClient = KinesisAsyncClient.builder().region(awsRegion).build
    val dynamoDbClient = DynamoDbAsyncClient.builder().region(awsRegion).build
    val cloudWatchClient = CloudWatchAsyncClient.builder().region(awsRegion).build

    val kinesis = Kinesis.create(kinesisClient, dynamoDbClient, cloudWatchClient, blocker)

    kinesis.readFromKinesisStream("enrich-kinesis-integration-tests", stream)
      .map(_.record.data.array())
  }
}
