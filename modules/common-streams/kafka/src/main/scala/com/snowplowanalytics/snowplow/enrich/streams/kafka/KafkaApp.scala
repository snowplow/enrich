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
package com.snowplowanalytics.snowplow.enrich.streams.kafka

import cats.effect.IO
import com.snowplowanalytics.snowplow.streams.kafka.{KafkaFactory, KafkaSinkConfig, KafkaSourceConfig}
import com.snowplowanalytics.snowplow.enrich.cloudutils.azure._
import com.snowplowanalytics.snowplow.enrich.streams.common.EnrichApp

object KafkaApp extends EnrichApp[EmptyConfig, KafkaSourceConfig, KafkaSinkConfig, AzureStorageConfig](BuildInfo) {

  override def toFactory: KafkaApp.FactoryProvider = _ => KafkaFactory.resource[IO]

  override def toBlobClients: KafkaApp.BlobClientsProvider = c => List(AzureBlobClient.client[IO](c))
}
