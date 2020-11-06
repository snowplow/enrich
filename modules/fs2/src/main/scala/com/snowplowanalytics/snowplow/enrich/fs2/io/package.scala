package com.snowplowanalytics.snowplow.enrich.fs2

import cats.syntax.either._

import com.permutive.pubsub.consumer.decoder.MessageDecoder
import com.permutive.pubsub.producer.encoder.MessageEncoder

import org.joda.time.DateTime
import org.apache.http.NameValuePair
import org.apache.http.message.BasicNameValuePair

import _root_.io.circe.{Decoder, DecodingFailure, HCursor}
import _root_.io.circe.generic.semiauto._

import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
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

  implicit val nvpDecoder: Decoder[NameValuePair] = new Decoder[NameValuePair] {
    final def apply(cur: HCursor): Decoder.Result[NameValuePair] =
      cur.focus match {
        case Some(json) =>
          json.fold(
            DecodingFailure("NameValuePair can not be null", cur.history).asLeft,
            b => DecodingFailure(s"NameValuePair can not be boolean, [$b] provided", cur.history).asLeft,
            n => DecodingFailure(s"NameValuePair can not be number, [$n] provided", cur.history).asLeft,
            s => DecodingFailure(s"NameValuePair can not be string, [$s] provided", cur.history).asLeft,
            a => DecodingFailure(s"NameValuePair can not be array, [$a] provided", cur.history).asLeft,
            o =>
              o.toList match {
                case List((k, v)) => new BasicNameValuePair(k, v.asString.get).asRight
                case _ => DecodingFailure(s"NameValuePair [$o] is map with more than one element", cur.history).asLeft,
              }
          )
        case None => DecodingFailure("NameValuePair missing", cur.history).asLeft
      }
  }

  implicit val dateTimeDecoder: Decoder[DateTime] =
    Decoder[String].emap(s => Either.catchNonFatal(DateTime.parse(s)).leftMap(_.getMessage))
  implicit val apiDecoder: Decoder[CollectorPayload.Api] = deriveDecoder[CollectorPayload.Api]
  implicit val sourceDecoder: Decoder[CollectorPayload.Source] = deriveDecoder[CollectorPayload.Source]
  implicit val contextDecoder: Decoder[CollectorPayload.Context] = deriveDecoder[CollectorPayload.Context]

  implicit val collectorPayloadDecoder: Decoder[CollectorPayload] =
    deriveDecoder[CollectorPayload]
}
