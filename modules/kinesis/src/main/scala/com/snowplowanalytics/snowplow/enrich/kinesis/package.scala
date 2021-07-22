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
package com.snowplowanalytics.snowplow.enrich

import com.amazonaws.regions.DefaultAwsRegionProviderChain

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest

import cats.implicits._

import cats.effect.{ContextShift, Resource, Sync}

package object kinesis {
  def getRuntimeRegion: Option[String] =
    Option((new DefaultAwsRegionProviderChain).getRegion)

  def streamExists[F[_]: Sync: ContextShift](region: String, streamName: String): F[Unit] =
    Resource
      .fromAutoCloseable(
        Sync[F].delay(KinesisClient.builder().region(Region.of(region)).build)
      )
      .use { client =>
        val describeStreamRequest = DescribeStreamRequest
          .builder()
          .streamName(streamName)
          .build()
        // throws ResourceNotFoundException if stream doesn't exist
        Sync[F].delay(client.describeStream(describeStreamRequest)).void
      }
}
