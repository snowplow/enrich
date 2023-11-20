/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */

package io.circe.jackson.enrich

import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.Module.SetupContext
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.deser.Deserializers
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.Serializers
import io.circe.Json
import io.circe.jackson.CirceJsonSerializer

object CirceJsonModule extends SimpleModule("SPCirceJson", Version.unknownVersion()) {
  override final def setupModule(context: SetupContext): Unit = {
    context.addDeserializers(
      new Deserializers.Base {
        override final def findBeanDeserializer(
          javaType: JavaType,
          config: DeserializationConfig,
          beanDesc: BeanDescription
        ): CirceJsonDeserializer = {
          val klass = javaType.getRawClass
          if (classOf[Json].isAssignableFrom(klass) || klass == Json.JNull.getClass)
            new CirceJsonDeserializer(klass)
          else null
        }
      }
    )

    context.addSerializers(
      new Serializers.Base {
        override final def findSerializer(
          config: SerializationConfig,
          javaType: JavaType,
          beanDesc: BeanDescription
        ): JsonSerializer[Object] = {
          val ser: Object =
            if (classOf[Json].isAssignableFrom(beanDesc.getBeanClass))
              CirceJsonSerializer
            else null

          ser.asInstanceOf[JsonSerializer[Object]]
        }
      }
    )
  }
}
