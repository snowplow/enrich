/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import cats.Monad
import cats.data.{EitherT, IorT, NonEmptyList}
import cats.effect.Clock
import cats.implicits._

import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

import com.snowplowanalytics.iglu.client.{ClientError, IgluCirceClient}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentManager
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent

/**
 * Contain the functions to validate:
 *  - An unstructured event,
 *  - The input contexts of an event,
 *  - The contexts added by the enrichments.
 */
object IgluUtils {

  /**
   * Extract unstructured event (if any) and input contexts (if any) from input event
   * and validate them against their schema
   * @param enriched Contain the input Jsons
   * @param client Iglu client used to validate the SDJs
   * @param raw Raw input event, used only to put in the bad row in case of problem
   * @param processor Meta data to put in the bad row
   * @return Extracted unstructured event and input contexts if any and if everything valid,
   *         `BadRow.SchemaViolations` if something went wrong. For instance if the
   *         unstructured event is invalid and has a context that is invalid,
   *         the bad row will contain the 2 associated `FailureDetails.SchemaViolation`s
   */
  def extractAndValidateInputJsons[F[_]: Monad: Clock](
    enriched: EnrichedEvent,
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F]
  ): IorT[F, NonEmptyList[FailureDetails.SchemaViolation], EventExtractResult] =
    for {
      contexts <- extractAndValidateInputContexts(enriched, client, registryLookup)
      unstruct <- extractAndValidateUnstructEvent(enriched, client, registryLookup)
    } yield {
      val validationInfoContexts = (contexts.flatMap(_.validationInfo) ::: unstruct.flatMap(_.validationInfo).toList).distinct
        .map(_.toSdj)
      EventExtractResult(contexts = contexts.map(_.sdj),
                         unstructEvent = unstruct.map(_.sdj),
                         validationInfoContexts = validationInfoContexts
      )
    }

  private implicit class ExtraEitherTSyntax[F[_], A, B](val eitherT: EitherT[F, A, B]) extends AnyVal {
    def toIorT(implicit F: Monad[F]): IorT[F, A, B] =
      IorT(eitherT.value.map(_.toIor))
  }

  /**
   * Extract unstructured event from event and validate against its schema
   *  @param enriched Snowplow event from which to extract unstructured event (in String)
   *  @param client Iglu client used for SDJ validation
   *  @param field Name of the field containing the unstructured event, to put in the bad row
   *               in case of failure
   *  @param criterion Expected schema for the JSON containing the unstructured event
   *  @return Valid unstructured event if the input event has one
   */
  private[common] def extractAndValidateUnstructEvent[F[_]: Monad: Clock](
    enriched: EnrichedEvent,
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F],
    field: String = "ue_properties",
    criterion: SchemaCriterion = SchemaCriterion("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", 1, 0)
  ): IorT[F, NonEmptyList[FailureDetails.SchemaViolation], Option[SdjExtractResult]] =
    Option(enriched.unstruct_event) match {
      case Some(rawUnstructEvent) =>
        for {
          // Validate input Json string and extract unstructured event
          unstruct <- extractInputData(rawUnstructEvent, field, criterion, client, registryLookup)
                        .leftMap(NonEmptyList.one)
                        .toIorT
          // Parse Json unstructured event as SelfDescribingData[Json]
          unstructSDJ <- parseAndValidateSDJ_sv(unstruct, client, registryLookup)
        } yield unstructSDJ.some
      case None =>
        IorT.rightT[F, NonEmptyList[FailureDetails.SchemaViolation]](none[SdjExtractResult])
    }

  /**
   * Extract list of custom contexts from event and validate each against its schema
   *  @param enriched Snowplow enriched event from which to extract custom contexts (in String)
   *  @param client Iglu client used for SDJ validation
   *  @param field Name of the field containing the contexts, to put in the bad row
   *               in case of failure
   *  @param criterion Expected schema for the JSON containing the contexts
   *  @return List will all contexts provided that they are all valid
   */
  private[common] def extractAndValidateInputContexts[F[_]: Monad: Clock](
    enriched: EnrichedEvent,
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F],
    field: String = "contexts",
    criterion: SchemaCriterion = SchemaCriterion("com.snowplowanalytics.snowplow", "contexts", "jsonschema", 1, 0)
  ): IorT[F, NonEmptyList[FailureDetails.SchemaViolation], List[SdjExtractResult]] =
    Option(enriched.contexts) match {
      case Some(rawContexts) =>
        for {
          // Validate input Json string and extract contexts
          contexts <- extractInputData(rawContexts, field, criterion, client, registryLookup)
                        .map(_.asArray.get.toList) // .get OK because SDJ wrapping the contexts valid
                        .leftMap(NonEmptyList.one)
                        .toIorT
          // Parse and validate each SDJ and merge the errors
          contextsSdj <- contexts.traverse(parseAndValidateSDJ_sv(_, client, registryLookup))
        } yield contextsSdj
      case None =>
        IorT.rightT[F, NonEmptyList[FailureDetails.SchemaViolation]](
          List.empty[SdjExtractResult]
        )
    }

  /**
   * Validate each context added by the enrichments against its schema
   *  @param client Iglu client used for SDJ validation
   *  @param sdjs List of enrichments contexts to be added to the enriched event
   *  @param raw Input event to put in the bad row if at least one context is invalid
   *  @param processor Meta data for the bad row
   *  @param enriched Partially enriched event to put in the bad row
   *  @return Unit if all the contexts are valid
   */
  private[common] def validateEnrichmentsContexts[F[_]: Monad: Clock](
    client: IgluCirceClient[F],
    sdjs: List[SelfDescribingData[Json]],
    raw: RawEvent,
    processor: Processor,
    enriched: EnrichedEvent,
    registryLookup: RegistryLookup[F]
  ): IorT[F, BadRow.EnrichmentFailures, Unit] =
    checkList(client, sdjs, registryLookup)
      .leftMap(
        _.map {
          case (schemaKey, clientError) =>
            val enrichmentInfo =
              FailureDetails.EnrichmentInformation(schemaKey, "enrichments-contexts-validation")
            FailureDetails.EnrichmentFailure(
              enrichmentInfo.some,
              FailureDetails.EnrichmentFailureMessage.IgluError(schemaKey, clientError)
            )
        }
      )
      .leftMap { enrichmentFailures =>
        EnrichmentManager.buildEnrichmentFailuresBadRow(
          enrichmentFailures,
          EnrichedEvent.toPartiallyEnrichedEvent(enriched),
          RawEvent.toRawEvent(raw),
          processor
        )
      }

  /** Used to extract .data for input custom contexts and input unstructured event */
  private def extractInputData[F[_]: Monad: Clock](
    rawJson: String,
    field: String, // to put in the bad row
    expectedCriterion: SchemaCriterion,
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F]
  ): EitherT[F, FailureDetails.SchemaViolation, Json] =
    for {
      // Parse Json string with the SDJ
      json <- JsonUtils
                .extractJson(rawJson)
                .leftMap(e => FailureDetails.SchemaViolation.NotJson(field, rawJson.some, e))
                .toEitherT[F]
      // Parse Json as SelfDescribingData[Json] (which contains the .data that we want)
      sdj <- SelfDescribingData
               .parse(json)
               .leftMap(FailureDetails.SchemaViolation.NotIglu(json, _))
               .toEitherT[F]
      // Check that the schema of SelfDescribingData[Json] is the expected one
      _ <- if (validateCriterion(sdj, expectedCriterion))
             EitherT.rightT[F, FailureDetails.SchemaViolation](sdj)
           else
             EitherT
               .leftT[F, SelfDescribingData[Json]](
                 FailureDetails.SchemaViolation.CriterionMismatch(sdj.schema, expectedCriterion)
               )
      // Check that the SDJ holding the .data is valid
      _ <- check(client, sdj, registryLookup)
             .leftMap {
               case (schemaKey, clientError) =>
                 FailureDetails.SchemaViolation.IgluError(schemaKey, clientError)
             }
      // Extract .data of SelfDescribingData[Json]
      data <- EitherT.rightT[F, FailureDetails.SchemaViolation](sdj.data)
    } yield data

  /** Check that the schema of a SDJ matches the expected one */
  private def validateCriterion(sdj: SelfDescribingData[Json], criterion: SchemaCriterion): Boolean =
    criterion.matches(sdj.schema)

  /** Check that a SDJ is valid */
  private def check[F[_]: Monad: Clock](
    client: IgluCirceClient[F],
    sdj: SelfDescribingData[Json],
    registryLookup: RegistryLookup[F]
  ): EitherT[F, (SchemaKey, ClientError), Option[SchemaVer.Full]] = {
    implicit val rl = registryLookup
    client
      .check(sdj)
      .leftMap((sdj.schema, _))
  }

  /** Check a list of SDJs and merge the Iglu errors */
  private def checkList[F[_]: Monad: Clock](
    client: IgluCirceClient[F],
    sdjs: List[SelfDescribingData[Json]],
    registryLookup: RegistryLookup[F]
  ): IorT[F, NonEmptyList[(SchemaKey, ClientError)], Unit] =
    sdjs.traverse { sdj =>
      check(client, sdj, registryLookup).toIorT
        .leftMap(NonEmptyList.one)
    }.void

  /** Parse a Json as a SDJ and check that it's valid */
  private def parseAndValidateSDJ_sv[F[_]: Monad: Clock]( // _sv for SchemaViolation
    json: Json,
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F]
  ): IorT[F, NonEmptyList[FailureDetails.SchemaViolation], SdjExtractResult] =
    for {
      sdj <- IorT
               .fromEither[F](SelfDescribingData.parse(json))
               .leftMap[FailureDetails.SchemaViolation](FailureDetails.SchemaViolation.NotIglu(json, _))
               .leftMap(NonEmptyList.one)
      supersedingSchema <- check(client, sdj, registryLookup)
                             .leftMap {
                               case (schemaKey, clientError) =>
                                 FailureDetails.SchemaViolation
                                   .IgluError(schemaKey, clientError): FailureDetails.SchemaViolation

                             }
                             .leftMap(NonEmptyList.one)
                             .toIorT
      validationInfo = supersedingSchema.map(s => ValidationInfo(sdj.schema, s))
      sdjUpdated = replaceSchemaVersion(sdj, validationInfo)
    } yield SdjExtractResult(sdj, None)

  private def replaceSchemaVersion(
    sdj: SelfDescribingData[Json],
    validationInfo: Option[ValidationInfo]
  ): SelfDescribingData[Json] =
    validationInfo match {
      case None => sdj
      case Some(s) => sdj.copy(schema = sdj.schema.copy(version = s.validatedWith))
    }

  case class ValidationInfo(originalSchema: SchemaKey, validatedWith: SchemaVer.Full) {
    def toSdj: SelfDescribingData[Json] =
      SelfDescribingData(ValidationInfo.schemaKey, (this: ValidationInfo).asJson)
  }

  object ValidationInfo {
    val schemaKey = SchemaKey("com.snowplowanalytics.iglu", "validation_info", "jsonschema", SchemaVer.Full(1, 0, 0))

    implicit val schemaVerFullEncoder: Encoder[SchemaVer.Full] =
      Encoder.encodeString.contramap(v => v.asString)

    implicit val validationInfoEncoder: Encoder[ValidationInfo] =
      deriveEncoder[ValidationInfo]
  }

  case class SdjExtractResult(sdj: SelfDescribingData[Json], validationInfo: Option[ValidationInfo])

  case class EventExtractResult(
    contexts: List[SelfDescribingData[Json]],
    unstructEvent: Option[SelfDescribingData[Json]],
    validationInfoContexts: List[SelfDescribingData[Json]]
  )
}
