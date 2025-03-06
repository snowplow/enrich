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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import scala.collection.JavaConverters._

import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._

import io.circe._
import io.circe.syntax._

import javax.script._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.JavascriptScriptConf
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.{CirceUtils, ConversionUtils, JsonUtils}
import JavascriptScriptEnrichment.{JavascriptRejectionException, Result}

object JavascriptScriptEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow",
      "javascript_script_config",
      "jsonschema",
      1,
      0
    )

  /**
   * Creates a JavascriptScriptConf from a Json.
   * @param c The JavaScript script enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return a JavascriptScript configuration
   */
  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, JavascriptScriptConf] =
    (for {
      _ <- isParseable(c, schemaKey)
      encoded <- CirceUtils.extract[String](c, "parameters", "script").toEither
      script <- ConversionUtils.decodeBase64Url(encoded)
      params <- CirceUtils.extract[Option[JsonObject]](c, "parameters", "config").toEither
      _ <- if (script.isEmpty) Left("Provided script for JS enrichment is empty") else Right(())
    } yield JavascriptScriptConf(schemaKey, script, params.getOrElse(JsonObject.empty))).toValidatedNel

  /**
   * Represents the result of JS enrichment script
   */
  sealed trait Result extends Product with Serializable

  object Result {

    /**
     * Failed result from the JS enrichment script
     */
    case class Failure(f: FailureDetails.EnrichmentFailure) extends Result

    /**
     * Successful result from the JS enrichment script
     */
    case class Success(l: List[SelfDescribingData[Json]]) extends Result

    /**
     * Event is dropped in JS enrichment script
     */
    case object Dropped extends Result
  }

  class JavascriptRejectionException extends Exception
}

final case class JavascriptScriptEnrichment(
  schemaKey: SchemaKey,
  rawFunction: String,
  params: JsonObject = JsonObject.empty
) extends Enrichment {
  private val enrichmentInfo =
    FailureDetails.EnrichmentInformation(schemaKey, "Javascript enrichment").some

  private val engine = new ScriptEngineManager(null)
    .getEngineByMimeType("text/javascript")
    .asInstanceOf[ScriptEngine with Invocable with Compilable]

  private val stringified = rawFunction + s"""
    var getJavascriptContexts = function() {
      const params = ${params.asJson.noSpaces};
      return function(event, headers) {
        const result = process(event, params, headers);
        if (result == null) {
          return "[]"
        } else {
          return JSON.stringify(result);
        }
      }
    }()
    """

  private val invocable =
    Either
      .catchNonFatal(engine.compile(stringified).eval())
      .leftMap(e => s"Error compiling JavaScript function: [${e.getMessage}]")

  /**
   * Run the process function from the Javascript configuration on the supplied EnrichedEvent.
   * @param event The enriched event to pass into our process function.
   *              The event can be updated in-place by the JS function.
   * @return either a JSON array of contexts on Success, or an error String on Failure
   */
  def process(
    event: EnrichedEvent,
    headers: List[String],
    maxJsonDepth: Int
  ): Result =
    (for {
      _ <- invocable.leftMap(createFailure)
      contexts <- Either
                    .catchNonFatal(engine.invokeFunction("getJavascriptContexts", event, headers.asJava).asInstanceOf[String])
                    .leftMap {
                      case e if isRejectionException(e) => Result.Dropped
                      case e => createFailure(s"Error during execution of JavaScript function: [${e.getMessage}]")
                    }
      json <- JsonUtils
                .extractJson(contexts, maxJsonDepth)
                .leftMap(err => createFailure(s"Could not parse output JSON of Javascript function. Error: [$err]"))
      l <- parseContexts(json).leftMap(createFailure)
    } yield Result.Success(l)).merge

  private def parseContexts(json: Json): Either[String, List[SelfDescribingData[Json]]] =
    json.asArray match {
      case Some(array) =>
        array
          .parTraverse(json =>
            SelfDescribingData
              .parse(json)
              .leftMap(error => (error, json))
              .leftMap(NonEmptyList.one)
          )
          .map(_.toList)
          .leftMap { s =>
            val msg = s.toList
              .map {
                case (error, json) => s"error code:[${error.code}],json:[${json.noSpaces}]"
              }
              .mkString(";")
            s"Resulting contexts are not self-desribing. Error(s): [$msg]"
          }
      case None =>
        Left(s"Output of JavaScript function [$json] could be parsed as JSON but is not read as an array")
    }

  private def createFailure(errorMsg: String) =
    Result.Failure(FailureDetails.EnrichmentFailure(enrichmentInfo, FailureDetails.EnrichmentFailureMessage.Simple(errorMsg)))

  private def isRejectionException(t: Throwable): Boolean =
    Option(t.getCause) match {
      case Some(cause) if cause.isInstanceOf[JavascriptRejectionException] => true
      case Some(cause) => isRejectionException(cause)
      case None => false
    }
}
