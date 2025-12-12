/*
 * Copyright (c) 2017-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.pii

import scala.collection.JavaConverters._
import scala.collection.mutable.MutableList

import cats.Foldable
import cats.data.ValidatedNel
import cats.implicits._

import io.circe._
import io.circe.jackson._
import io.circe.syntax._

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, NullNode, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.ObjectMapper

import com.jayway.jsonpath.{Configuration, JsonPath => JJsonPath}
import com.jayway.jsonpath.MapFunction

import org.apache.commons.codec.digest.DigestUtils

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.PiiPseudonymizerConf
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.ParseableEnrichment
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.pii.serializers._
import com.snowplowanalytics.snowplow.enrich.common.utils.CirceUtils

/** Companion object. Lets us create a PiiPseudonymizerEnrichment from a Json. */
object PiiPseudonymizerEnrichment extends ParseableEnrichment {

  override val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "pii_enrichment_config",
      "jsonschema",
      2,
      0
    )

  override def parse(
    config: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, PiiPseudonymizerConf] = {
    for {
      conf <- isParseable(config, schemaKey)
      emitIdentificationEvent = CirceUtils
                                  .extract[Boolean](conf, "emitEvent")
                                  .toOption
                                  .getOrElse(false)
      piiFields <- CirceUtils
                     .extract[List[Json]](conf, "parameters", "pii")
                     .toEither
      piiStrategy <- CirceUtils
                       .extract[PiiStrategyPseudonymize](config, "parameters", "strategy")
                       .toEither
      mutators <- extractMutators(piiFields)
      anonymousOnly <- CirceUtils
                         .extract[Option[Boolean]](conf, "anonymousOnly")
                         .map {
                           case Some(value) => value
                           case None => false
                         }
                         .toEither
    } yield PiiPseudonymizerConf(schemaKey, mutators, emitIdentificationEvent, piiStrategy, anonymousOnly)
  }.toValidatedNel

  private[pii] def getHashFunction(strategyFunction: String): Either[String, DigestFunction] =
    strategyFunction match {
      case "MD2" => ((x: Array[Byte]) => DigestUtils.md2Hex(x)).asRight
      case "MD5" => ((x: Array[Byte]) => DigestUtils.md5Hex(x)).asRight
      case "SHA-1" => ((x: Array[Byte]) => DigestUtils.sha1Hex(x)).asRight
      case "SHA-256" => ((x: Array[Byte]) => DigestUtils.sha256Hex(x)).asRight
      case "SHA-384" => ((x: Array[Byte]) => DigestUtils.sha384Hex(x)).asRight
      case "SHA-512" => ((x: Array[Byte]) => DigestUtils.sha512Hex(x)).asRight
      case fName => s"Unknown function $fName".asLeft
    }

  /**
   * Part of the config parser
   *  @param piiFields The plain JSON provided by the enrichment config
   *  @return If JSON config is recognized, then a `PiiMutators` which contains the mutators that act on incoming events
   */
  private def extractMutators(piiFields: List[Json]): Either[String, PiiMutators] =
    Foldable[List].foldM(piiFields, PiiMutators(Nil, Nil, Nil, Nil)) {
      case (mutators, json) =>
        CirceUtils
          .extract[String](json, "pojo", "field")
          .toEither
          .flatMap(extractPojoMutator)
          .map { pojoMutator =>
            mutators.copy(pojo = mutators.pojo :+ pojoMutator)
          }
          .orElse {
            json.hcursor
              .downField("json")
              .focus
              .toRight("No json field")
              .flatMap(extractFieldLocator(mutators, _))
          }
          .orElse {
            ("PII Configuration: pii field does not include 'pojo' nor 'json' fields. " +
              s"Got: [${json.noSpaces}]").asLeft
          }
    }

  /**
   * Part of the config parser
   *  @param fieldName A pojo field name provided in the enrichment config
   *  @return If field name is recognized, then a mutator that acts on incoming events
   */
  private def extractPojoMutator(fieldName: String): Either[String, MutatorFn] =
    ScalarMutators.byFieldName
      .get(fieldName)
      .map(_.asRight)
      .getOrElse(s"The specified pojo field $fieldName is not supported".asLeft)

  /**
   * Part of the config parser. Used inside a `fold` function; accumulates more mutators.
   *  @param mutators The accumulated mutators so far
   *  @param jsonField The plain JSON received in the enrichment config, corresponding to a single mutator
   *  @param If JSON is recognized, then returns the accumulated `mutators` appended with an extra mutator
   */
  private def extractFieldLocator(mutators: PiiMutators, jsonField: Json): Either[String, PiiMutators] = {
    val schemaCriterion = CirceUtils
      .extract[String](jsonField, "schemaCriterion")
      .toEither
      .flatMap(sc => SchemaCriterion.parse(sc).toRight(s"Could not parse schema criterion $sc"))
      .toValidatedNel
    val jsonPath = CirceUtils.extract[String](jsonField, "jsonPath").toValidatedNel
    val entityType = CirceUtils
      .extract[String](jsonField, "field")
      .toEither
      .flatMap {
        case "unstruct_event" => EntityType.Unstruct.asRight
        case "contexts" => EntityType.Contexts.asRight
        case "derived_contexts" => EntityType.DerivedContexts.asRight
        case other => s"The specified json field $other is not supported".asLeft
      }
      .toValidatedNel
    (entityType, schemaCriterion, jsonPath)
      .mapN {
        case (EntityType.Unstruct, sc, jp) =>
          mutators.copy(unstruct = mutators.unstruct :+ JsonFieldLocator(sc, jp))
        case (EntityType.Contexts, sc, jp) =>
          mutators.copy(contexts = mutators.contexts :+ JsonFieldLocator(sc, jp))
        case (EntityType.DerivedContexts, sc, jp) =>
          mutators.copy(derivedContexts = mutators.derivedContexts :+ JsonFieldLocator(sc, jp))
      }
      .leftMap(x => s"Unable to extract PII JSON: ${x.toList.mkString(",")}")
      .toEither
  }

  /** Helper to remove fields that were wrongly added and are not in the original JSON. See #351. */
  private[pii] def removeAddedFields(hashed: Json, original: Json): Json = {
    val fixedObject = for {
      hashedFields <- hashed.asObject
      originalFields <- original.asObject
      newFields = hashedFields.toList.flatMap {
                    case (k, v) => originalFields(k).map(origV => (k, removeAddedFields(v, origV)))
                  }
    } yield Json.fromFields(newFields)

    lazy val fixedArray = for {
      hashedArr <- hashed.asArray
      originalArr <- original.asArray
      newArr = hashedArr.zip(originalArr).map { case (hashed, orig) => removeAddedFields(hashed, orig) }
    } yield Json.fromValues(newArr)

    fixedObject.orElse(fixedArray).getOrElse(hashed)
  }

  /**
   * Specifies the location of field in a self-describing JSON entity
   * @param schemaCriterion A SDJ will be pseudonymized when its schema matches this criterion
   * @param jsonPath The field in the SDJ to pseudonymize
   */
  case class JsonFieldLocator(schemaCriterion: SchemaCriterion, jsonPath: String)

  sealed trait EntityType {
    val fieldName: String
  }
  object EntityType {
    case object Unstruct extends EntityType {
      val fieldName: String = "unstruct_event"
    }
    case object Contexts extends EntityType {
      val fieldName: String = "contexts"
    }
    case object DerivedContexts extends EntityType {
      val fieldName: String = "derived_contexts"
    }
  }

  /**
   * Runs pseudonymization on a JSON field, after it has been identified as matching a locator from the config
   *  @param entityType The entity type this JSON came from
   *  @param json The json to be pseudonymized (i.e. the `data` field of a SDJ)
   *  @param strategy How to hash this field
   *  @param schemaKey The schema this JSON came from
   *  @param jsonPath Locates the specific field in the JSON
   */
  private def jsonPathReplace(
    entityType: EntityType,
    json: Json,
    strategy: PiiStrategy,
    schema: SchemaKey,
    jsonPath: String
  ): (Json, List[JsonModifiedField]) = {
    val objectNode = CirceUtils.mapper.valueToTree[ObjectNode](json)
    val documentContext = JJsonPath.using(JsonPathConf).parse(objectNode)
    val modifiedFields = MutableList[JsonModifiedField]()
    Option(documentContext.read[AnyRef](jsonPath)) match { // check that json object not null
      case None => (jacksonToCirce(documentContext.json[JsonNode]()), modifiedFields.toList)
      case _ =>
        val documentContext2 = documentContext.map(
          jsonPath,
          new ScrambleMapFunction(strategy, modifiedFields, entityType.fieldName, jsonPath, schema)
        )
        (jacksonToCirce(documentContext2.json[JsonNode]()), modifiedFields.toList)
    }
  }
}

/**
 * The PiiPseudonymizerEnrichment runs after all other enrichments to find fields that are
 * configured as PII (personally identifiable information) and apply some anonymization (currently
 * only pseudonymization) on them. Currently a single strategy for all the fields is supported due
 * to the configuration format, and there is only one implemented strategy, however the enrichment
 * supports a strategy per field.
 * The user may specify two types of fields in the config `pojo` or `json`. A `pojo` field is
 * effectively a scalar field in the EnrichedEvent, whereas a `json` is a "context" formatted field
 * and it can either contain a single value in the case of unstruct_event, or an array in the case
 * of derived_events and contexts.
 * @param mutators list of mutators that operatre on the fields
 * @param emitIdentificationEvent to emit an identification event
 * @param strategy pseudonymization strategy to use
 */
final case class PiiPseudonymizerEnrichment(
  mutators: PiiMutators,
  emitIdentificationEvent: Boolean,
  strategy: PiiStrategy,
  anonymousOnly: Boolean
) {
  import PiiPseudonymizerEnrichment._

  val piiTransformationSchema: SchemaKey = SchemaKey(
    "com.snowplowanalytics.snowplow",
    "pii_transformation",
    "jsonschema",
    SchemaVer.Full(1, 0, 0)
  )

  /**
   * Mask PII fields except if anonymousOnly is true and SP-Anonymous header is not present
   * Header check is case-insensitive
   */
  def transformer(
    event: EnrichedEvent,
    headers: List[String],
    derivedContexts: List[SelfDescribingData[Json]]
  ): (Option[SelfDescribingData[Json]], List[SelfDescribingData[Json]]) = {
    lazy val anonymousHeaderExist = headers.exists(_.toLowerCase.startsWith("sp-anonymous"))
    if (anonymousOnly && !anonymousHeaderExist)
      (None, derivedContexts)
    else {
      val pojoModifiedFields = mutators.pojo.flatMap(_.apply(event, strategy))
      val ueModifiedFields = transformUnstruct(event)
      val contextsModifiedFields = transformContexts(event)
      val (derivedContextsModifiedFields, modifiedDerivedContexts) = transformDerivedContexts(derivedContexts)

      if (emitIdentificationEvent) {
        val allModifications = pojoModifiedFields ::: ueModifiedFields ::: contextsModifiedFields ::: derivedContextsModifiedFields
        if (allModifications.nonEmpty)
          (SelfDescribingData(piiTransformationSchema, PiiModifiedFields(allModifications, strategy).asJson).some, modifiedDerivedContexts)
        else (None, modifiedDerivedContexts)
      } else (None, modifiedDerivedContexts)
    }
  }

  private def transformUnstruct(event: EnrichedEvent): ModifiedFields =
    event.unstruct_event match {
      case Some(sdj) =>
        val locators = mutators.unstruct.filter { locator =>
          locator.schemaCriterion.matches(sdj.schema)
        }
        val (hashed, modifiedFields) = locators.foldLeft((sdj.data, List.empty[ModifiedField])) {
          case ((data, modifiedFields), locator) =>
            val (fixed, moreModifiedFields) = jsonPathReplace(EntityType.Unstruct, data, strategy, sdj.schema, locator.jsonPath)
            (fixed, modifiedFields ::: moreModifiedFields)
        }
        val fixed = if (modifiedFields.nonEmpty) removeAddedFields(hashed, sdj.data) else hashed
        event.unstruct_event = Some(sdj.copy(data = fixed))
        modifiedFields
      case None =>
        Nil
    }

  private def transformContexts(event: EnrichedEvent): ModifiedFields = {
    val finalResult = MutableList[ModifiedField]()
    val fixed = event.contexts.map { sdj =>
      val locators = mutators.contexts.filter { locator =>
        locator.schemaCriterion.matches(sdj.schema)
      }
      val (hashed, modifiedFields) = locators.foldLeft((sdj.data, List.empty[ModifiedField])) {
        case ((data, modifiedFields), locator) =>
          val (fixed, moreModifiedFields) = jsonPathReplace(EntityType.Contexts, data, strategy, sdj.schema, locator.jsonPath)
          (fixed, modifiedFields ::: moreModifiedFields)
      }
      val fixed = if (modifiedFields.nonEmpty) removeAddedFields(hashed, sdj.data) else hashed
      finalResult ++= modifiedFields
      sdj.copy(data = fixed)
    }
    event.contexts = fixed
    finalResult.toList
  }

  private def transformDerivedContexts(
    derivedContexts: List[SelfDescribingData[Json]]
  ): (ModifiedFields, List[SelfDescribingData[Json]]) = {
    val finalResult = MutableList[ModifiedField]()
    val fixed = derivedContexts.map { sdj =>
      val locators = mutators.derivedContexts.filter { locator =>
        locator.schemaCriterion.matches(sdj.schema)
      }
      val (hashed, modifiedFields) = locators.foldLeft((sdj.data, List.empty[ModifiedField])) {
        case ((data, modifiedFields), locator) =>
          val (fixed, moreModifiedFields) = jsonPathReplace(EntityType.DerivedContexts, data, strategy, sdj.schema, locator.jsonPath)
          (fixed, modifiedFields ::: moreModifiedFields)
      }
      val fixed = if (modifiedFields.nonEmpty) removeAddedFields(hashed, sdj.data) else hashed
      finalResult ++= modifiedFields
      sdj.copy(data = fixed)
    }
    (finalResult.toList, fixed)
  }
}

private final case class ScrambleMapFunction(
  strategy: PiiStrategy,
  modifiedFields: MutableList[JsonModifiedField],
  fieldName: String,
  jsonPath: String,
  schema: SchemaKey
) extends MapFunction {
  override def map(currentValue: AnyRef, configuration: Configuration): AnyRef =
    currentValue match {
      case t: TextNode =>
        val originalValue = t.asText()
        val newValue = strategy.scramble(originalValue)
        val _ = modifiedFields += JsonModifiedField(fieldName, originalValue, newValue, jsonPath, schema.toSchemaUri)
        new TextNode(newValue)
      case a: ArrayNode =>
        val mapper = new ObjectMapper()
        val arr = mapper.createArrayNode()
        a.elements.asScala.foreach {
          case t: TextNode =>
            val originalValue = t.asText()
            val newValue = strategy.scramble(originalValue)
            modifiedFields += JsonModifiedField(
              fieldName,
              originalValue,
              newValue,
              jsonPath,
              schema.toSchemaUri
            )
            arr.add(newValue)
          case default: AnyRef => arr.add(default)
          case null => arr.add(NullNode.getInstance())
        }
        arr
      case _ => currentValue
    }
}
