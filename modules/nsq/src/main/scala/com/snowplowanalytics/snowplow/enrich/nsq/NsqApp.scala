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
package com.snowplowanalytics.snowplow.enrich.nsq

import cats.effect.IO

import com.snowplowanalytics.snowplow.streams.nsq.{NsqFactory, NsqSinkConfig, NsqSourceConfig}

import com.snowplowanalytics.snowplow.enrich.cloudutils.aws.S3BlobClient
import com.snowplowanalytics.snowplow.enrich.cloudutils.azure._
import com.snowplowanalytics.snowplow.enrich.cloudutils.gcp.GcsBlobClient

import com.snowplowanalytics.snowplow.enrich.core.EnrichApp

object NsqApp extends EnrichApp[EmptyConfig, NsqSourceConfig, NsqSinkConfig, AzureStorageConfig](BuildInfo) {

  override def toFactory: NsqApp.FactoryProvider = _ => NsqFactory.resource[IO]

  override def toBlobClients: NsqApp.BlobClientsProvider =
    c =>
      List(
        AzureBlobClient.client[IO](c),
        GcsBlobClient.client[IO],
        S3BlobClient.client[IO](S3BlobClient.Config(awsUserAgent = None))
      )
}
