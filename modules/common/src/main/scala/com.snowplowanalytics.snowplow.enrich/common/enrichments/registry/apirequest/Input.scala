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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest

import scala.util.control.NonFatal

import cats.data.ValidatedNel
import cats.implicits._

import io.circe.{Json => JSON, DecodingFailure, Decoder}
import io.gatling.jsonpath.{JsonPath => GatlingJsonPath}

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SelfDescribingData}
import com.snowplowanalytics.snowplow.badrows.igluSchemaCriterionDecoder

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonPath._

/**
 * Container for key with one (and only one) of possible input sources
 * Basically, represents a key for future template context and way to get value
 * out of EnrichedEvent, custom context, derived event or unstruct event.
 */
sealed trait Input extends Product with Serializable {
  def key: String

  // We could short-circuit enrichment process on invalid JSONPath,
  // but it won't give user meaningful error message
  def validatedJsonPath: Either[String, GatlingJsonPath] =
    this match {
      case json: Input.Json => compileQuery(json.jsonPath)
      case _ => "No JSON Path given".asLeft
    }

  /**
   * Get key-value pair input from specific `event` for composing
   * @param event currently enriching event
   * @return template context with empty or with single element this particular input
   */
  def pull(
    event: EnrichedEvent,
    derived: List[SelfDescribingData[JSON]],
    custom: List[SelfDescribingData[JSON]],
    unstruct: Option[SelfDescribingData[JSON]]
  ): Input.TemplateContext =
    this match {
      case pojoInput: Input.Pojo =>
        try {
          val method = event.getClass.getMethod(pojoInput.field)
          val value = Option(method.invoke(event)).map(_.toString)
          value.map(v => Map(key -> v)).validNel
        } catch {
          case NonFatal(err) => s"Error accessing POJO input field [$key]: [$err]".invalidNel
        }
      case jsonInput: Input.Json =>
        val validatedJson = jsonInput.field match {
          case "derived_contexts" =>
            Input.getBySchemaCriterion(derived, jsonInput.criterion).validNel
          case "contexts" => Input.getBySchemaCriterion(custom, jsonInput.criterion).validNel
          case "unstruct_event" =>
            Input.getBySchemaCriterion(unstruct.toList, jsonInput.criterion).validNel
          case other =>
            s"Error: wrong field [$other] passed to Input.getFromJson. Should be one of: derived_contexts, contexts, unstruct_event".invalidNel
        }

        (validatedJson, validatedJsonPath.toValidatedNel).mapN { (validJson, jsonPath) =>
          validJson
            .map(jsonPath.circeQuery) // Query context/UE (always valid)
            .map(wrapArray) // Check if array
            .flatMap(Input.stringifyJson) // Transform to valid string
            .map(v => Map(key -> v)) // Transform to Key-Value
        }
    }
}

/**
 * Companion object, containing common methods for input data manipulation and
 * template context building
 */
object Input {

  /**
   * Describes how to take key from POJO source
   * @param field `EnrichedEvent` object field
   */
  final case class Pojo(
    key: String,
    field: String,
    allowMissing: Boolean = false
  ) extends Input

  /**
   * @param field where to get this JSON, one of unstruct_event, contexts or derived_contexts
   * @param criterion self-describing JSON you are looking for in the given JSON field.
   * You can specify only the SchemaVer MODEL (e.g. 1-), MODEL plus REVISION (e.g. 1-1-) etc
   * @param jsonPath JSONPath statement to navigate to the field inside the JSON that you want to use
   * as the input
   */
  final case class Json(
    key: String,
    field: String,
    criterion: SchemaCriterion,
    jsonPath: String,
    allowMissing: Boolean = false
  ) extends Input

  implicit val inputApiCirceDecoder: Decoder[Input] =
    Decoder.instance { cur =>
      for {
        obj <- cur.value.as[Map[String, JSON]]
        key <- obj
                 .get("key")
                 .toRight(DecodingFailure("Key is missing", cur.history))
        keyString <- key.as[String]
        pojo = obj.get("pojo").map { pojoJson =>
                 for {
                   field <- pojoJson.hcursor.downField("field").as[String]
                   allowMissing <- pojoJson.hcursor.downField("allowMissing").as[Boolean].handleError(_ => false)
                 } yield Pojo(keyString, field, allowMissing)
               }
        json = obj.get("json").map { jsonJson =>
                 for {
                   field <- jsonJson.hcursor.downField("field").as[String]
                   criterion <- jsonJson.hcursor.downField("schemaCriterion").as[SchemaCriterion]
                   jsonPath <- jsonJson.hcursor.downField("jsonPath").as[String]
                   allowMissing <- jsonJson.hcursor.downField("allowMissing").as[Boolean].handleError(_ => false)
                 } yield Json(keyString, field, criterion, jsonPath, allowMissing)
               }
        _ <- if (json.isDefined && pojo.isDefined)
               DecodingFailure("Either json or pojo input must be specified, both provided", cur.history).asLeft
             else ().asRight
        result <- pojo
                    .orElse(json)
                    .toRight(
                      DecodingFailure(
                        "Either json or pojo input must be specified, none provided",
                        cur.history
                      )
                    )
                    .flatten
      } yield result
    }

  /**
   * Validated Optional Map of Strings used to inject values into corresponding placeholders
   * (key inside double curly braces) in template strings
   * Failure means failure while accessing particular field, like invalid JSONPath, POJO-access, etc
   * None means any of required fields were not found, so this lookup need to be skipped in future
   * Tag used to not merge values on colliding keys (`Tags.FirstVal` can be used as well)
   */
  type TemplateContext = ValidatedNel[String, Option[Map[String, String]]]

  val emptyTemplateContext: TemplateContext =
    Map.empty[String, String].some.validNel

  /**
   * Get template context out of input configurations
   * If any required input is missing it will return None.
   * If an optional input is missing it will not affect the result of the fold.
   * Example 1:
   *  Input1 is required and exists in the json, which yields Some(x), however optional Input2 is missing, therefore the result is Some(Map(x'))
   * Example 2:
   *  Input1 is required and missing in the json, which yields None, Input2 is optional and available but the result is None
   * Example 3:
   *  Input1 is optional and missing, the result is Some(Map.empty)
   * @param inputs input-configurations with for keys and instructions how to get values
   * @param event current enriching event
   * @param derivedContexts list of contexts derived on enrichment process
   * @param customContexts list of custom contexts shredded out of event
   * @param unstructEvent optional unstruct event object
   * @return final template context
   */
  def buildTemplateContext(
    inputs: List[Input],
    event: EnrichedEvent,
    derivedContexts: List[SelfDescribingData[JSON]],
    customContexts: List[SelfDescribingData[JSON]],
    unstructEvent: Option[SelfDescribingData[JSON]]
  ): TemplateContext = {
    def pull(input: Input) = input.pull(event, derivedContexts, customContexts, unstructEvent)

    inputs
      .traverse {
        case json @ Input.Json(_, _, _, _, true) => pull(json).map(_.orElse(Some(Map.empty)))
        case pojo @ Input.Pojo(_, _, true) => pull(pojo).map(_.orElse(Some(Map.empty)))
        case input => pull(input)
      }
      .map { filledInputs =>
        filledInputs.sequence // Swap List[Option[Map[K, V]]] with Option[List[Map[K, V]]]
          .map(_.foldLeft(List.empty[(String, String)]) { (acc, e) =>
            acc |+| e.filterNot(_._2.isEmpty).toList
          }.toMap)
      }
  }

  /**
   * Get data out of all JSON contexts matching `schemaCriterion`
   * If more than one context match schemaCriterion, first will be picked
   * @param contexts list of self-describing JSON contexts attached to event
   * @param criterion part of URI
   * @return first (optional) self-desc JSON matched `schemaCriterion`
   */
  def getBySchemaCriterion(contexts: List[SelfDescribingData[JSON]], criterion: SchemaCriterion): Option[JSON] =
    contexts.find(context => criterion.matches(context.schema)).map(_.data)

  /**
   * Helper function to stringify JValue to URL-friendly format
   * JValue should be converted to string for further use in URL template with following rules:
   * 1. string -> as is
   * 2. number, booleans, nulls -> stringify
   * 3. array -> concatenate with comma ([1,true,"foo"] -> "1,true,foo"). Nested will be flattened
   * 4. object -> use as is
   * @param json arbitrary JSON value
   * @return some string best represenging json or None if there's no way to stringify it
   */
  private def stringifyJson(json: JSON): Option[String] =
    json.fold(
      "null".some,
      _.toString.some,
      _.toString.some,
      _.some,
      _.map(stringifyJson).mkString(",").some,
      o => JSON.fromJsonObject(o).noSpaces.some
    )
}
