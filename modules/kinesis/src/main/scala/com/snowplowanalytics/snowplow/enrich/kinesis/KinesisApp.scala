/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.kinesis

import cats.effect.IO

import com.snowplowanalytics.snowplow.streams.kinesis.{KinesisFactory, KinesisSinkConfig, KinesisSourceConfig}

import com.snowplowanalytics.snowplow.enrich.cloudutils.aws.S3BlobClient

import com.snowplowanalytics.snowplow.enrich.core.EnrichApp

object KinesisApp extends EnrichApp[EmptyConfig, KinesisSourceConfig, KinesisSinkConfig, EmptyConfig](BuildInfo) {

  override def toFactory: FactoryProvider = _ => KinesisFactory.resource[IO]

  override def toBlobClients: BlobClientsProvider = _ => List(S3BlobClient.client[IO])
}
