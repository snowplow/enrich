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
package com.snowplowanalytics.snowplow.enrich.pubsub

import cats.effect.IO

import com.snowplowanalytics.snowplow.streams.pubsub.{PubsubFactory, PubsubFactoryConfig, PubsubSinkConfig, PubsubSourceConfig}

import com.snowplowanalytics.snowplow.enrich.cloudutils.gcp.GcsBlobClient

import com.snowplowanalytics.snowplow.enrich.core.EnrichApp

object PubsubApp extends EnrichApp[PubsubFactoryConfig, PubsubSourceConfig, PubsubSinkConfig, EmptyConfig](BuildInfo) {

  override def toFactory: FactoryProvider = PubsubFactory.resource[IO](_)

  override def toBlobClients: PubsubApp.BlobClientsProvider = _ => List(GcsBlobClient.client[IO])
}
