package io.circe.jackson.enrich

import java.io.{File, Writer}

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import com.fasterxml.jackson.databind.ObjectMapper
import com.snowplowanalytics.snowplow.enrich.common.utils.CirceUtils

class WithJacksonMapper {
  final val mapper: ObjectMapper = CirceUtils.mapper
  private[this] final val jsonFactory: JsonFactory = new JsonFactory(mapper)

  protected final def jsonStringParser(input: String): JsonParser = jsonFactory.createParser(input)
  protected final def jsonFileParser(file: File): JsonParser = jsonFactory.createParser(file)
  protected final def jsonBytesParser(bytes: Array[Byte]): JsonParser =
    jsonFactory.createParser(bytes)
  protected final def jsonGenerator(out: Writer) = jsonFactory.createGenerator(out)
}
