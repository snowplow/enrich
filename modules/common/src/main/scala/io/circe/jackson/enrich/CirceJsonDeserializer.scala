/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package io.circe.jackson.enrich

import java.util

import com.fasterxml.jackson.core.{JsonParser, JsonTokenId}
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import io.circe.jackson.{DeserializerContext, JacksonCompat, ReadingList, ReadingMap}
import io.circe.{Json, JsonBigDecimal, JsonLong}

import scala.annotation.{switch, tailrec}
import scala.jdk.CollectionConverters._

private[jackson] final class CirceJsonDeserializer(klass: Class[_]) extends JsonDeserializer[Object] with JacksonCompat {
  override def isCachable: Boolean = true

  override def deserialize(jp: JsonParser, ctxt: DeserializationContext): Json = {
    val value = deserialize(jp, ctxt, List())
    if (!klass.isAssignableFrom(value.getClass)) handleUnexpectedToken(ctxt)(klass, jp)

    value
  }

  @tailrec
  def deserialize(
    jp: JsonParser,
    ctxt: DeserializationContext,
    parserContext: List[DeserializerContext]
  ): Json = {
    if (jp.getCurrentToken == null) jp.nextToken()

    val (maybeValue, nextContext) = (jp.getCurrentToken.id(): @switch) match {
      case JsonTokenId.ID_NUMBER_INT => (Some(Json.JNumber(JsonLong(jp.getLongValue))), parserContext)
      case JsonTokenId.ID_NUMBER_FLOAT => (Some(Json.JNumber(JsonBigDecimal(jp.getDecimalValue))), parserContext)
      case JsonTokenId.ID_STRING => (Some(Json.JString(jp.getText)), parserContext)
      case JsonTokenId.ID_TRUE => (Some(Json.JBoolean(true)), parserContext)
      case JsonTokenId.ID_FALSE => (Some(Json.JBoolean(false)), parserContext)
      case JsonTokenId.ID_NULL => (Some(Json.JNull), parserContext)
      case JsonTokenId.ID_START_ARRAY => (None, ReadingList(new util.ArrayList) +: parserContext)

      case JsonTokenId.ID_END_ARRAY =>
        parserContext match {
          case ReadingList(content) :: stack =>
            (Some(Json.fromValues(content.asScala)), stack)
          case _ => throw new IllegalStateException("Jackson read ']' but parser context is not an array")
        }

      case JsonTokenId.ID_START_OBJECT => (None, ReadingMap(new util.ArrayList) +: parserContext)

      case JsonTokenId.ID_FIELD_NAME =>
        parserContext match {
          case (c: ReadingMap) :: stack => (None, c.setField(jp.getCurrentName) +: stack)
          case _ =>
            throw new IllegalStateException("Jackson read a String field name but parser context is not a json object")
        }

      case JsonTokenId.ID_END_OBJECT =>
        parserContext match {
          case ReadingMap(content) :: stack =>
            (
              Some(Json.fromFields(content.asScala)),
              stack
            )
          case _ => throw new IllegalStateException("Jackson read '}' but parser context is not a json object")
        }

      case JsonTokenId.ID_NOT_AVAILABLE =>
        throw new IllegalStateException("Jackson can't return the json token yet")

      case JsonTokenId.ID_EMBEDDED_OBJECT =>
        throw new IllegalStateException("Jackson read embedded object but json object was expected")
    }

    maybeValue match {
      case Some(v) if nextContext.isEmpty => v
      case maybeValue =>
        jp.nextToken()
        val toPass = maybeValue
          .map { v =>
            val previous :: stack = nextContext
            previous.addValue(v) +: stack
          }
          .getOrElse(nextContext)

        deserialize(jp, ctxt, toPass)
    }
  }

  override def getNullValue = Json.JNull
}
