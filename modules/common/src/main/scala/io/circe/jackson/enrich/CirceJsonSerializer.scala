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

package io.circe.jackson.enrich

import java.math.{BigDecimal => JBigDecimal, BigInteger}

import cats.syntax.either._

import com.fasterxml.jackson.databind.node.{BigIntegerNode, DecimalNode}

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import io.circe.{BiggerDecimalJsonNumber, Json, JsonBigDecimal, JsonBiggerDecimal, JsonDecimal, JsonDouble, JsonFloat, JsonLong}

/**
 * Only difference from original CirceJsonSerializer is serialization of JsonDecimal and
 * JsonBiggerDecimal. In original CirceJsonSerializer, they are written as string. In this
 * customer serializer, they are written as number if it is possible.
 */
private[jackson] final object CirceJsonSerializer extends JsonSerializer[Json] {

  override final def serialize(
    value: Json,
    json: JsonGenerator,
    provider: SerializerProvider
  ): Unit =
    value match {
      case Json.JNumber(v) =>
        v match {
          case JsonLong(x) => json.writeNumber(x)
          case JsonDouble(x) => json.writeNumber(x)
          case JsonFloat(x) => json.writeNumber(x)
          case d: JsonDecimal => writeBigDecimal(json, d)
          case d: JsonBiggerDecimal => writeBigDecimal(json, d)
          case JsonBigDecimal(x) =>
            // Workaround #3784: Same behaviour as if JsonGenerator were
            // configured with WRITE_BIGDECIMAL_AS_PLAIN, but forced as this
            // configuration is ignored when called from ObjectMapper.valueToTree
            val raw = x.stripTrailingZeros.toPlainString

            if (raw contains ".") json.writeTree(new DecimalNode(new JBigDecimal(raw)))
            else json.writeTree(new BigIntegerNode(new BigInteger(raw)))
        }
      case Json.JString(v) => json.writeString(v)
      case Json.JBoolean(v) => json.writeBoolean(v)
      case Json.JArray(elements) =>
        json.writeStartArray()
        elements.foreach(t => serialize(t, json, provider))
        json.writeEndArray()
      case Json.JObject(values) =>
        json.writeStartObject()
        values.toList.foreach { t =>
          json.writeFieldName(t._1)
          serialize(t._2, json, provider)
        }
        json.writeEndObject()
      case Json.JNull => json.writeNull()
    }

  private def writeBigDecimal(json: JsonGenerator, v: BiggerDecimalJsonNumber): Unit =
    Either.catchNonFatal(v.toBigDecimal).toOption.flatten match {
      case None => json.writeString(v.toString())
      case Some(d) => json.writeNumber(d.bigDecimal)
    }
}
