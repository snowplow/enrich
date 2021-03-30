package com.snowplowanalytics.snowplow.enrich.fs2

import cats.syntax.either._

import com.permutive.pubsub.consumer.decoder.MessageDecoder
import com.permutive.pubsub.producer.encoder.MessageEncoder

package object io {

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
