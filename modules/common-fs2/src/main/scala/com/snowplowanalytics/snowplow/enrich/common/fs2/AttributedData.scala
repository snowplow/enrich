/*
 * Copyright (c) 2021-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2

/**
 * Represents a payload, partition key and attributes of the payload
 * @param data payload to be sent to the sink
 * @param partitionKey field name to be used as partition key, supported by Kinesis and Kafka
 * @param attributes key-value pairs to be added to the message as attributes, supported by PubSub and Kafka
 * @tparam A type of the payload
 */
final case class AttributedData[A](
  data: A,
  partitionKey: String,
  attributes: Map[String, String]
)
