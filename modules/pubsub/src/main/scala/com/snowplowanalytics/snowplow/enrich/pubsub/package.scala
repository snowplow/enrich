/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich

import cats.syntax.either._

import com.permutive.pubsub.consumer.decoder.MessageDecoder
import com.permutive.pubsub.producer.encoder.MessageEncoder

package object pubsub {

  implicit val byteArrayEncoder: MessageEncoder[Array[Byte]] =
    new MessageEncoder[Array[Byte]] {
      def encode(a: Array[Byte]): Either[Throwable, Array[Byte]] =
        a.asRight
    }

  implicit val byteArrayMessageDecoder: MessageDecoder[Array[Byte]] =
    new MessageDecoder[Array[Byte]] {
      def decode(message: Array[Byte]): Either[Throwable, Array[Byte]] =
        message.asRight
    }
}
