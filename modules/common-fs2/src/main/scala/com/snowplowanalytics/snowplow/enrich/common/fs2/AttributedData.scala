/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
