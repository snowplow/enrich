/*
 * Copyright (c) 2017-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.{Configuration, Option => JOption}

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

package object pii {
  type DigestFunction = Array[Byte] => String
  type ModifiedFields = List[ModifiedField]
  type ApplyStrategyFn = (String, PiiStrategy) => (String, ModifiedFields)
  type MutatorFn = (EnrichedEvent, PiiStrategy, ApplyStrategyFn) => ModifiedFields

  val JsonMutators = Mutators.JsonMutators
  val ScalarMutators = Mutators.ScalarMutators

  // Configuration for JsonPath, SerializationFeature.FAIL_ON_EMPTY_BEANS is required otherwise an
  // invalid path causes an exception
  private[pii] val JacksonNodeJsonObjectMapper = {
    val objectMapper = new ObjectMapper()
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    objectMapper
  }
  // SUPPRESS_EXCEPTIONS is useful here as we prefer an empty list to an exception when a path is
  // not found.
  private[pii] val JsonPathConf =
    Configuration
      .builder()
      .options(JOption.SUPPRESS_EXCEPTIONS)
      .jsonProvider(new JacksonJsonNodeJsonProvider(JacksonNodeJsonObjectMapper))
      .build()
}

package pii {

  /**
   * PiiStrategy trait. This corresponds to a strategy to apply to a single field. Currently only
   * String input is supported.
   */
  sealed trait PiiStrategy {
    def scramble(clearText: String): String
  }

  /**
   * Implements a pseudonymization strategy using any algorithm known to DigestFunction
   * @param functionName string representation of the function
   * @param hashFunction the DigestFunction to apply
   * @param salt salt added to the plain string before hashing
   */
  final case class PiiStrategyPseudonymize(
    functionName: String,
    hashFunction: DigestFunction,
    salt: String
  ) extends PiiStrategy {
    val TextEncoding = "UTF-8"
    override def scramble(clearText: String): String = hash(clearText + salt)
    def hash(text: String): String = hashFunction(text.getBytes(TextEncoding))
  }

  /**
   * The mutator class encapsulates the mutator function and the field name where the mutator will
   * be applied.
   */
  private[pii] final case class Mutator(fieldName: String, muatatorFn: MutatorFn)

  /**
   * Parent class for classes that serialize the values that were modified during the PII enrichment
   */
  private[pii] final case class PiiModifiedFields(modifiedFields: ModifiedFields, strategy: PiiStrategy)

  /** Case class for capturing scalar field modifications. */
  private[pii] final case class ScalarModifiedField(
    fieldName: String,
    originalValue: String,
    modifiedValue: String
  ) extends ModifiedField

  /** Case class for capturing JSON field modifications. */
  private[pii] final case class JsonModifiedField(
    fieldName: String,
    originalValue: String,
    modifiedValue: String,
    jsonPath: String,
    schema: String
  ) extends ModifiedField

  /**
   * PiiField trait. This corresponds to a configuration top-level field (i.e. either a scalar or a
   * JSON field) along with a function to apply that strategy to the EnrichedEvent POJO (A scalar
   * field is represented in config py "pojo")
   */
  trait PiiField {

    /**
     * The POJO mutator for this field
     * @return fieldMutator
     */
    def fieldMutator: Mutator

    /**
     * Gets an enriched event from the enrichment manager and modifies it according to the specified
     * strategy.
     * @param event The enriched event
     */
    def transform(event: EnrichedEvent, strategy: PiiStrategy): ModifiedFields =
      fieldMutator.muatatorFn(event, strategy, applyStrategy)

    protected def applyStrategy(fieldValue: String, strategy: PiiStrategy): (String, ModifiedFields)
  }

  /**
   * The modified field trait represents an item that is transformed in either the JSON or a scalar
   * mutators.
   */
  sealed trait ModifiedField

  /** Abstract type to get salt using the supported methods */
  private[pii] trait PiiStrategyPseudonymizeSalt {
    def getSalt: String
  }
}
