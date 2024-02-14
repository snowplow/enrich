/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import cats.syntax.either._
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.{Configuration, JsonPath => JaywayJsonPath, Option => JOption}
import io.circe._
import io.circe.jackson.{circeToJackson, jacksonToCirce}

import scala.jdk.CollectionConverters._

/** Wrapper for `com.jayway.jsonpath` for circe */
object JsonPath {

  private val JacksonNodeJsonObjectMapper = {
    val objectMapper = new ObjectMapper()
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    objectMapper
  }
  private val JsonPathConf =
    Configuration
      .builder()
      .options(JOption.SUPPRESS_EXCEPTIONS)
      .options(JOption.ALWAYS_RETURN_LIST)
      .jsonProvider(new JacksonJsonNodeJsonProvider(JacksonNodeJsonObjectMapper))
      .build()

  /**
   * Pimp-up JsonPath class to work with JValue
   * Unlike `query(jsonPath, json)` it gives empty list on any error (like JNothing)
   * @param jsonPath precompiled with [[compileQuery]] JsonPath object
   */
  implicit class CirceExtractor(jsonPath: JaywayJsonPath) {
    def circeQuery(json: Json): List[Json] =
      arrayNodeToCirce(jsonPath.read[ArrayNode](circeToJackson(json), JsonPathConf))
  }

  /**
   * Query some JSON by `jsonPath`. It always return List, even for single match.
   * Unlike `json.circeQuery(stringPath)` it gives error if JNothing was given
   */
  def query(jsonPath: String, json: Json): Either[String, List[Json]] =
    Either.catchNonFatal {
      JaywayJsonPath
        .using(JsonPathConf)
        .parse(circeToJackson(json))
        .read[ArrayNode](jsonPath)
    } match {
      case Right(jacksonArrayNode) =>
        arrayNodeToCirce(jacksonArrayNode).asRight
      case Left(error) =>
        error.getMessage.asLeft
    }

  /**
   * Precompile JsonPath query
   *
   * @param query JsonPath query as a string
   * @return valid JsonPath object either error message
   */
  def compileQuery(query: String): Either[String, JaywayJsonPath] =
    Either.catchNonFatal(JaywayJsonPath.compile(query)).leftMap(_.getMessage)

  /**
   * Wrap list of values into JSON array if several values present
   * Use in conjunction with `query`. JNothing will represent absent value
   * @param values list of JSON values
   * @return array if there's >1 values in list
   */
  def wrapArray(values: List[Json]): Json =
    values match {
      case Nil => Json.fromValues(List.empty)
      case one :: Nil => one
      case many => Json.fromValues(many)
    }

  private def arrayNodeToCirce(jacksonArrayNode: ArrayNode): List[Json] =
    jacksonArrayNode.elements().asScala.toList.map(jacksonToCirce)
}
