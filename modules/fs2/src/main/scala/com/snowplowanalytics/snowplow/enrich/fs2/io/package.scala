package com.snowplowanalytics.snowplow.enrich.fs2

import cats.syntax.either._

import com.permutive.pubsub.consumer.decoder.MessageDecoder
import com.permutive.pubsub.producer.encoder.MessageEncoder

import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

package object io {

  implicit val badRowEncoder: MessageEncoder[BadRow] =
    new MessageEncoder[BadRow] {
      def encode(a: BadRow): Either[Throwable, Array[Byte]] =
        a.compact.getBytes.asRight
    }

  implicit val enrichedEventEncoder: MessageEncoder[EnrichedEvent] =
    new MessageEncoder[EnrichedEvent] {
      def encode(enrichedEvent: EnrichedEvent): Either[Throwable, Array[Byte]] =
        Enrich.encodeEvent(enrichedEvent).getBytes.asRight
    }

  implicit val byteArrayMessageDecoder: MessageDecoder[Array[Byte]] =
    new MessageDecoder[Array[Byte]] {
      def decode(message: Array[Byte]): Either[Throwable, Array[Byte]] =
        message.asRight
    }
}
