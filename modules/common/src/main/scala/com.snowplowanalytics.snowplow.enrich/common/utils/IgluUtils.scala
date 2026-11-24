/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import java.time.Instant

import cats.data.{EitherT, NonEmptyList, WriterT}
import cats.effect.Sync
import cats.implicits._

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

import com.snowplowanalytics.iglu.client.{ClientError, IgluCirceClient}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.enrich.common.FatalEnrichError
import com.snowplowanalytics.snowplow.enrich.common.enrichments.Failure

import com.snowplowanalytics.snowplow.badrows._

/**
 * Contains the functions to validate the SDJs attached to an event:
 *  - Unstruct event
 *  - Contexts/entities
 *  - Derived contexts (added by enrichments)
 */
object IgluUtils {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  /**
   * Error raised when Iglu Server is unreachable, causing the application to crash.
   * This prevents bad rows from being created due to infrastructure issues.
   *
   * Thrown when `Resolver.isSystemError` returns true, which happens when schema resolution
   * fails with RepoFailure or ClientFailure errors (server unavailability, connection issues)
   * rather than NotFound errors (schema genuinely doesn't exist).
   *
   * @see com.snowplowanalytics.iglu.client.resolver.Resolver#isSystemError
   */
  case class IgluSystemError(message: String) extends FatalEnrichError(message)

  case class ValidSDJ(
    sdj: SelfDescribingData[Json],
    validationInfo: Option[ValidationInfo]
  )

  sealed trait UnstructResult
  case class ValidUnstruct(unstruct: ValidSDJ) extends UnstructResult
  case class InvalidUnstruct(schemaKey: SchemaKey) extends UnstructResult

  case class Contexts(contexts: NonEmptyList[ValidSDJ])

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

  /**
   *  @param unstructEvent Raw string extracted from tracker payload, i.e. ue_pr or ue_px tracker field
   *  @param contexts Raw string extracted from tracker payload, i.e. co or cx tracker field
   */
  case class EventExtractInput(
    unstructEvent: Option[String],
    contexts: Option[String]
  )

  /**
   * Parse and validate unstruct event and contexts, if any.
   * @return `SchemaViolation`s accumulated in the log. `UnstructResult` and valid contexts in the
   *         value; each is None if absent or if the schema could not be resolved.
   */
  def parseAndValidateInput[F[_]: Sync](
    input: EventExtractInput,
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F],
    maxJsonDepth: Int,
    etlTstamp: Instant
  ): WriterT[F, List[Failure.SchemaViolation], (Option[UnstructResult], Option[Contexts])] =
    for {
      unstruct <- parseAndValidateUnstruct(input.unstructEvent, client, registryLookup, maxJsonDepth, etlTstamp)
      contexts <- parseAndValidateContexts(input.contexts, client, registryLookup, maxJsonDepth, etlTstamp)
    } yield (unstruct, contexts)

  /**
   * Parse and validate unstruct event, if any.
   * @return `SchemaViolation`s accumulated in the log. `ValidUnstruct` if the event data conforms
   *         to its schema, `InvalidUnstruct` if the schema was resolved but data is invalid,
   *         or None if the event is absent or the schema could not be resolved.
   */
  private[common] def parseAndValidateUnstruct[F[_]: Sync](
    maybeUnstruct: Option[String],
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F],
    maxJsonDepth: Int,
    etlTstamp: Instant
  ): WriterT[F, List[Failure.SchemaViolation], Option[UnstructResult]] =
    maybeUnstruct match {
      case Some(unstructStr) =>
        val field = "unstruct"
        val criterion = SchemaCriterion("com.snowplowanalytics.snowplow", "unstruct_event", "jsonschema", 1, 0)

        WriterT {
          extractInputData(unstructStr, field, criterion, hollowEntity, client, registryLookup, maxJsonDepth, etlTstamp).value.flatMap {
            case Left(outerError) =>
              Sync[F].pure((List(outerError), Option.empty[UnstructResult]))
            case Right(innerJson) =>
              decodeAndValidateSDJ(innerJson, client, registryLookup, field, etlTstamp).value.map {
                case Right(valid) =>
                  (Nil, Option[UnstructResult](ValidUnstruct(valid)))
                case Left(innerError) =>
                  val maybeResult: Option[UnstructResult] = innerError.schemaViolation match {
                    case FailureDetails.SchemaViolation.IgluError(key, ve: ClientError.ValidationError) =>
                      val resolvedKey = ve.supersededBy
                        .flatMap(v => SchemaVer.parseFull(v).toOption)
                        .fold(key)(ver => key.copy(version = ver))
                      Some(InvalidUnstruct(resolvedKey))
                    case _ =>
                      None
                  }
                  (List(innerError), maybeResult)
              }
          }
        }
      case None =>
        WriterT(Sync[F].pure((List.empty[Failure.SchemaViolation], Option.empty[UnstructResult])))
    }

  /**
   * Parse and validate contexts/entities, if any
   * @return `SchemaViolation`s accumulated in the log. Valid contexts in the value, or None if absent/all invalid.
   */
  private[common] def parseAndValidateContexts[F[_]: Sync](
    maybeContexts: Option[String],
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F],
    maxJsonDepth: Int,
    etlTstamp: Instant
  ): WriterT[F, List[Failure.SchemaViolation], Option[Contexts]] =
    maybeContexts match {
      case Some(contextsStr) =>
        val field = "contexts"
        val criterion = SchemaCriterion("com.snowplowanalytics.snowplow", "contexts", "jsonschema", 1, 0)

        WriterT {
          extractInputData(contextsStr, field, criterion, hollowEntities, client, registryLookup, maxJsonDepth, etlTstamp).value.flatMap {
            case Left(error) =>
              Sync[F].pure((List(error), Option.empty[Contexts]))
            case Right(json) =>
              val jsons = json.asArray.get.toList // .get OK because SDJ wrapping the contexts is valid
              jsons
                .traverse(decodeAndValidateSDJ(_, client, registryLookup, field, etlTstamp).value)
                .map { results =>
                  val (errors, valids) = results.separate
                  (errors, valids.toNel.map(Contexts(_)))
                }
          }
        }
      case None =>
        WriterT(Sync[F].pure((List.empty[Failure.SchemaViolation], Option.empty[Contexts])))
    }

  /** Used to extract .data from input contexts and input unstruct event */
  private def extractInputData[F[_]: Sync](
    rawJson: String,
    field: String, // to put in the bad row
    expectedCriterion: SchemaCriterion,
    hollow: Json => Json,
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F],
    maxJsonDepth: Int,
    etlTstamp: Instant
  ): EitherT[F, Failure.SchemaViolation, Json] =
    for {
      // Parse JSON string with the SDJ
      json <- JsonUtils
                .extractJson(rawJson, maxJsonDepth)
                .leftMap(e =>
                  Failure.SchemaViolation(
                    schemaViolation = FailureDetails.SchemaViolation.NotJson(field, rawJson.some, e),
                    source = field,
                    data = rawJson.asJson,
                    etlTstamp = etlTstamp
                  )
                )
                .toEitherT[F]
      // Decode Json as SelfDescribingData[Json] (which contains the .data that we want)
      sdj <- SelfDescribingData
               .parse(json)
               .leftMap(e =>
                 Failure.SchemaViolation(
                   schemaViolation = FailureDetails.SchemaViolation.NotIglu(json, e),
                   source = field,
                   data = json,
                   etlTstamp = etlTstamp
                 )
               )
               .toEitherT[F]
      // Check that the schema of SelfDescribingData[Json] is the expected one
      _ <- if (validateCriterion(sdj, expectedCriterion))
             EitherT.rightT[F, Failure.SchemaViolation](sdj)
           else
             EitherT
               .leftT[F, SelfDescribingData[Json]](
                 Failure.SchemaViolation(
                   schemaViolation = FailureDetails.SchemaViolation.CriterionMismatch(sdj.schema, expectedCriterion),
                   source = field,
                   data = sdj.data,
                   etlTstamp = etlTstamp
                 )
               )
      // Check that the SDJ holding the .data is valid
      _ <- validateSDJ(client, sdj.copy(data = hollow(sdj.data)), registryLookup, field, etlTstamp)
             .leftMap(_.copy(data = sdj.data))
      // Extract .data of SelfDescribingData[Json]
      data <- EitherT.rightT[F, Failure.SchemaViolation](sdj.data)
    } yield data

  /**
   * The unstruct_event and contexts schemas leave each entity's `data` unconstrained, so it is
   * replaced with null before validating the envelope. The entity's data is validated against its
   * own schema afterwards; validating it here too would fail the whole envelope for a fault in a
   * single entity, e.g. a number outside the range of a 64-bit float.
   */
  private def hollowEntity(entity: Json): Json =
    entity.mapObject(obj => if (obj.contains("data")) obj.add("data", Json.Null) else obj)

  private def hollowEntities(entities: Json): Json =
    entities.mapArray(_.map(hollowEntity))

  /** Check that the schema of a SDJ matches the expected one */
  private def validateCriterion(sdj: SelfDescribingData[Json], criterion: SchemaCriterion): Boolean =
    criterion.matches(sdj.schema)

  /** Decode a Json as a SDJ and check that it's valid */
  private def decodeAndValidateSDJ[F[_]: Sync](
    json: Json,
    client: IgluCirceClient[F],
    registryLookup: RegistryLookup[F],
    field: String,
    etlTstamp: Instant
  ): EitherT[F, Failure.SchemaViolation, ValidSDJ] =
    for {
      sdj <- EitherT
               .fromEither[F](SelfDescribingData.parse(json))
               .leftMap(e =>
                 Failure.SchemaViolation(
                   schemaViolation = FailureDetails.SchemaViolation.NotIglu(json, e),
                   source = field,
                   data = json.asJson,
                   etlTstamp = etlTstamp
                 )
               )
      valid <- validateSDJ(client, sdj, registryLookup, field, etlTstamp)
    } yield valid

  /** Check that a SDJ is valid */
  private[enrich] def validateSDJ[F[_]: Sync](
    client: IgluCirceClient[F],
    sdj: SelfDescribingData[Json],
    registryLookup: RegistryLookup[F],
    field: String,
    etlTstamp: Instant
  ): EitherT[F, Failure.SchemaViolation, ValidSDJ] = {
    implicit val rl: RegistryLookup[F] = registryLookup
    client
      .check(sdj)
      .leftSemiflatMap {
        case re: ClientError.ResolutionError if client.resolver.isSystemError(re) =>
          val message = s"Could not reach Iglu Server for schema '${sdj.schema.toSchemaUri}'. " +
            s"Check resolver configuration and ensure registries are available. " +
            s"Resolution errors: ${re.getMessage}"
          Logger[F].error(message) >> Sync[F].raiseError[ClientError](IgluSystemError(message))
        case other =>
          Sync[F].pure(other)
      }
      .leftMap(clientError =>
        Failure.SchemaViolation(
          schemaViolation = FailureDetails.SchemaViolation.IgluError(sdj.schema, clientError),
          source = field,
          data = sdj.data,
          etlTstamp = etlTstamp
        )
      )
      .map { supersededBy =>
        val validationInfo = supersededBy.map(s => ValidationInfo(sdj.schema, s))
        ValidSDJ(
          replaceSchemaVersion(sdj, validationInfo),
          validationInfo
        )
      }
  }

  /**
   * Check that several SDJs are valid
   * @return `SchemaViolation`s accumulated in the log. Valid SDJs in the value.
   */
  private[common] def validateSDJs[F[_]: Sync](
    client: IgluCirceClient[F],
    sdjs: List[SelfDescribingData[Json]],
    registryLookup: RegistryLookup[F],
    field: String,
    etlTstamp: Instant
  ): WriterT[F, List[Failure.SchemaViolation], List[ValidSDJ]] =
    WriterT {
      sdjs
        .traverse { sdj =>
          validateSDJ(client, sdj, registryLookup, field, etlTstamp).value
        }
        .map(_.separate)
    }

  private def replaceSchemaVersion(
    sdj: SelfDescribingData[Json],
    validationInfo: Option[ValidationInfo]
  ): SelfDescribingData[Json] =
    validationInfo match {
      case None => sdj
      case Some(s) => sdj.copy(schema = sdj.schema.copy(version = s.validatedWith))
    }
}
