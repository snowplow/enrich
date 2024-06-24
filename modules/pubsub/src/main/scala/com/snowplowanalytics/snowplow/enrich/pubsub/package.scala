/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
