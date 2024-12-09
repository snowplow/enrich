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
package com.snowplowanalytics.snowplow.enrich.streams.kinesis

import com.snowplowanalytics.snowplow.sources.kinesis.{KinesisSource, KinesisSourceConfig}
import com.snowplowanalytics.snowplow.sinks.kinesis.{KinesisSink, KinesisSinkConfig}

import com.snowplowanalytics.snowplow.enrich.streams.common.EnrichApp

object KinesisApp extends EnrichApp[KinesisSourceConfig, KinesisSinkConfig](BuildInfo) {

  override def source: SourceProvider = KinesisSource.build(_)

  override def sink: SinkProvider = KinesisSink.resource(_)
}
