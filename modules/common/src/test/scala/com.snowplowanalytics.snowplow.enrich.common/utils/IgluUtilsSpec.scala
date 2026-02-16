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

import org.specs2.mutable.Specification
import org.specs2.matcher.ValidatedMatchers

import cats.effect.testing.specs2.CatsEffect

import io.circe.parser.parse
import io.circe.Json
import io.circe.syntax._

import cats.data.{Ior, NonEmptyList}

import com.snowplowanalytics.iglu.core.{ParseError, SchemaKey, SchemaVer}

import com.snowplowanalytics.iglu.client.ClientError.{ResolutionError, ValidationError}

import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.enrichments.Failure
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.IgluUtils.{Contexts, Unstruct, ValidSDJ}

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers

class IgluUtilsSpec extends Specification with ValidatedMatchers with CatsEffect {
  import IgluUtilsSpec._

  val processor = Processor("unit tests SCE", "v42")
  val enriched = new EnrichedEvent()

  val unstructFieldName = "unstruct"
  val contextsFieldName = "contexts"
  val derivedContextsFieldName = "derived_contexts"

  val notJson = "foo"
  val jsonNotJson = notJson.asJson // Just jsonized version of the string
  val notIglu = """{"foo":"bar"}"""
  val emailSentSchema =
    SchemaKey(
      "com.acme",
      "email_sent",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )
  val supersedingExampleSchema100 =
    SchemaKey(
      "com.acme",
      "superseding_example",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )
  val supersedingExampleSchema101 = supersedingExampleSchema100.copy(version = SchemaVer.Full(1, 0, 1))
  val clientSessionSchema =
    SchemaKey(
      "com.snowplowanalytics.snowplow",
      "client_session",
      "jsonschema",
      SchemaVer.Full(1, 0, 1)
    )
  val emailSent1 = s"""{
    "schema": "${emailSentSchema.toSchemaUri}",
    "data": {
      "emailAddress": "hello@world.com",
      "emailAddress2": "foo@bar.org"
    }
  }"""
  val emailSent2 = s"""{
    "schema": "${emailSentSchema.toSchemaUri}",
    "data": {
      "emailAddress": "hello2@world.com",
      "emailAddress2": "foo2@bar.org"
    }
  }"""
  val invalidEmailSentData = s"""{
      "emailAddress": "hello@world.com"
  }"""
  val invalidEmailSent = s"""{
    "schema": "${emailSentSchema.toSchemaUri}",
    "data": $invalidEmailSentData
  }"""
  val supersedingExample1 =
    s"""{
    "schema": "${supersedingExampleSchema100.toSchemaUri}",
    "data": {
      "field_a": "value_a",
      "field_b": "value_b"
    }
  }"""
  val supersedingExample2 =
    s"""{
    "schema": "${supersedingExampleSchema100.toSchemaUri}",
    "data": {
      "field_a": "value_a",
      "field_b": "value_b",
      "field_d": "value_d"
    }
  }"""
  val clientSession = s"""{
    "schema": "${clientSessionSchema.toSchemaUri}",
    "data": {
      "sessionIndex": 1,
      "storageMechanism": "LOCAL_STORAGE",
      "firstEventId": "5c33fccf-6be5-4ce6-afb1-e34026a3ca75",
      "sessionId": "21c2a0dd-892d-42d1-b156-3a9d4e147eef",
      "previousSessionId": null,
      "userId": "20d631b8-7837-49df-a73e-6da73154e6fd"
    }
  }"""
  val noSchemaData = "{}"
  val noSchema =
    s"""{"schema":"iglu:com.snowplowanalytics.snowplow/foo/jsonschema/1-0-0", "data": $noSchemaData}"""

  val deepJson = s"""{
    "schema": "${emailSentSchema.toSchemaUri}",
    "data": {"d1":{"d2":{"d3":{"d4":{"d5":{"d6":6}}}}}}
  }"""

  "parseAndValidateUnstruct" should {
    "return None if unstruct_event field is empty" >> {
      IgluUtils
        .parseAndValidateUnstruct(None,
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Right(None) => ok
          case other => ko(s"[$other] is not a success with None")
        }
    }

    "return a FailureDetails.SchemaViolation.NotJson if unstruct_event does not contain a properly formatted JSON string" >> {
      IgluUtils
        .parseAndValidateUnstruct(Some(notJson),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(_: FailureDetails.SchemaViolation.NotJson, `unstructFieldName`, `jsonNotJson`, _),
                  _
                ),
                None
              ) =>
            ok
          case other => ko(s"[$other] is not an error with NotJson")
        }
    }

    "return a FailureDetails.SchemaViolation.NotIglu if unstruct_event contains a properly formatted JSON string that is not self-describing" >> {
      val json = notIglu.toJson

      IgluUtils
        .parseAndValidateUnstruct(Some(notIglu),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(Failure.SchemaViolation(_: FailureDetails.SchemaViolation.NotIglu, `unstructFieldName`, `json`, _), _),
                None
              ) =>
            ok
          case other => ko(s"[$other] is not an error with NotIglu")
        }
    }

    "return a FailureDetails.SchemaViolation.CriterionMismatch if unstruct_event contains a self-describing JSON but not with the expected schema for unstructured events" >> {
      val json = noSchemaData.toJson

      IgluUtils
        .parseAndValidateUnstruct(Some(noSchema),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(_: FailureDetails.SchemaViolation.CriterionMismatch, `unstructFieldName`, `json`, _),
                  _
                ),
                None
              ) =>
            ok
          case other => ko(s"[$other] is not an error with CriterionMismatch")
        }
    }

    "return a FailureDetails.SchemaViolation.NotJson if the JSON in .data is not a JSON" >> {
      val ue = buildUnstruct(notJson)
      val ueJson = ue.asJson

      IgluUtils
        .parseAndValidateUnstruct(Some(ue),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(NonEmptyList(
                          Failure.SchemaViolation(_: FailureDetails.SchemaViolation.NotJson, `unstructFieldName`, `ueJson`, _),
                          _
                        ),
                        None
              ) =>
            ok
          case other => ko(s"[$other] is not an error with NotJson")
        }
    }

    "return a FailureDetails.SchemaViolation.IgluError containing a ValidationError if the JSON in .data is not self-describing" >> {
      val ue = buildUnstruct(notIglu)
      val ueJson = notIglu.toJson

      IgluUtils
        .parseAndValidateUnstruct(Some(ue),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(NonEmptyList(Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError),
                                                             `unstructFieldName`,
                                                             `ueJson`,
                                                             _
                                     ),
                                     _
                        ),
                        None
              ) =>
            ok
          case other => ko(s"[$other] is not expected one")
        }
    }

    "return a FailureDetails.SchemaViolation.IgluError containing a ValidationError if the JSON in .data is not a valid SDJ" >> {
      val json = invalidEmailSentData.toJson

      IgluUtils
        .parseAndValidateUnstruct(Some(buildUnstruct(invalidEmailSent)),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError), `unstructFieldName`, `json`, _),
                  _
                ),
                None
              ) =>
            ok
          case other => ko(s"[$other] is not expected one")
        }
    }

    "return a FailureDetails.SchemaViolation.IgluError containing a ResolutionError if the schema of the SDJ in .data can't be resolved" >> {
      val json = noSchemaData.toJson

      IgluUtils
        .parseAndValidateUnstruct(Some(buildUnstruct(noSchema)),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ResolutionError), `unstructFieldName`, `json`, _),
                  _
                ),
                None
              ) =>
            ok
          case other => ko(s"[$other] is not expected one")
        }
    }

    "return the extracted unstructured event if .data is a valid SDJ" >> {
      IgluUtils
        .parseAndValidateUnstruct(Some(buildUnstruct(emailSent1)),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Right(Some(Unstruct(ValidSDJ(sdj, None)))) if sdj.schema == emailSentSchema => ok
          case Ior.Right(Some(s)) =>
            ko(
              s"unstructured event's schema [${s.unstruct.sdj.schema}] does not match expected schema [${emailSentSchema}]"
            )
          case other => ko(s"no unstructured event was extracted [$other]")
        }
    }

    "return the extracted unstructured event when schema is superseded by another schema" >> {
      val expectedValidationInfo = IgluUtils.ValidationInfo(supersedingExampleSchema100, supersedingExampleSchema101.version)

      IgluUtils
        .parseAndValidateUnstruct(Some(buildUnstruct(supersedingExample1)),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Right(Some(Unstruct(ValidSDJ(sdj, Some(`expectedValidationInfo`))))) if sdj.schema == supersedingExampleSchema101 =>
            ok
          case Ior.Right(Some(s)) =>
            ko(
              s"unstructured event's schema [${s.unstruct.sdj.schema}] does not match expected schema [${supersedingExampleSchema101}]"
            )
          case other => ko(s"no unstructured event was extracted [$other]")
        }

      // input2 wouldn't be validated with 1-0-0. It would be validated with 1-0-1 only.
      IgluUtils
        .parseAndValidateUnstruct(Some(buildUnstruct(supersedingExample2)),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Right(Some(Unstruct(ValidSDJ(sdj, Some(`expectedValidationInfo`))))) if sdj.schema == supersedingExampleSchema101 =>
            ok
          case Ior.Right(Some(s)) =>
            ko(
              s"unstructured event's schema [${s.unstruct.sdj.schema}] does not match expected schema [${supersedingExampleSchema101}]"
            )
          case other => ko(s"no unstructured event was extracted [$other]")
        }
    }

    "return a FailureDetails.SchemaViolation.IgluError containing a ValidationError if the JSON in .data exceeds the max allowed JSON depth" >> {
      val igluScalaClient = SpecHelpers.client(5)
      val json = deepJson.toJson

      IgluUtils
        .parseAndValidateUnstruct(Some(buildUnstruct(deepJson)),
                                  igluScalaClient,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError), `unstructFieldName`, `json`, _),
                  _
                ),
                None
              ) =>
            ok
          case other => ko(s"[$other] is not expected one")
        }
    }
  }

  "parseAndValidateContexts" should {
    "return None if contexts field is empty" >> {
      IgluUtils
        .parseAndValidateContexts(None,
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Right(None) => ok
          case other => ko(s"[$other] is not a success with an empty list")
        }
    }

    "return a FailureDetails.SchemaViolation.NotJson if .contexts does not contain a properly formatted JSON string" >> {
      IgluUtils
        .parseAndValidateContexts(Some(notJson),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(Failure.SchemaViolation(_: FailureDetails.SchemaViolation.NotJson, `contextsFieldName`, `jsonNotJson`, _),
                             Nil
                ),
                None
              ) =>
            ok
          case other => ko(s"[$other] is not an error with NotJson")
        }
    }

    "return a FailureDetails.SchemaViolation.NotIglu if .contexts contains a properly formatted JSON string that is not self-describing" >> {
      val json = notIglu.toJson

      IgluUtils
        .parseAndValidateContexts(Some(notIglu),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(Failure.SchemaViolation(_: FailureDetails.SchemaViolation.NotIglu, `contextsFieldName`, `json`, _), Nil),
                None
              ) =>
            ok
          case other => ko(s"[$other] is not an error with NotIglu")
        }
    }

    "return a FailureDetails.SchemaViolation.CriterionMismatch if .contexts contains a self-describing JSON but not with the right schema" >> {
      val json = noSchemaData.toJson

      IgluUtils
        .parseAndValidateContexts(Some(noSchema),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(NonEmptyList(
                          Failure.SchemaViolation(_: FailureDetails.SchemaViolation.CriterionMismatch, `contextsFieldName`, `json`, _),
                          Nil
                        ),
                        None
              ) =>
            ok
          case other => ko(s"[$other] is not an error with CriterionMismatch")
        }
    }

    "return a FailureDetails.SchemaViolation.IgluError containing a ValidationError if .data does not contain an array of JSON objects" >> {
      val notArrayContexts =
        s"""{"schema": "${inputContextsSchema.toSchemaUri}", "data": ${emailSent1}}"""
      val json = emailSent1.toJson

      IgluUtils
        .parseAndValidateContexts(Some(notArrayContexts),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError), `contextsFieldName`, `json`, _),
                  Nil
                ),
                None
              ) =>
            ok
          case other => ko(s"[$other] is not expected one")
        }
    }

    "return a FailureDetails.SchemaViolation.IgluError containing a ValidationError if .data contains one invalid context" >> {
      val json = invalidEmailSentData.toJson

      IgluUtils
        .parseAndValidateContexts(Some(buildInputContexts(List(invalidEmailSent))),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError), `contextsFieldName`, `json`, _),
                  Nil
                ),
                None
              ) =>
            ok
          case other => ko(s"[$other] is not expected one")
        }
    }

    "return a FailureDetails.SchemaViolation.IgluError containing a ResolutionError if .data contains one context whose schema can't be resolved" >> {
      val json = noSchemaData.toJson

      IgluUtils
        .parseAndValidateContexts(Some(buildInputContexts(List(noSchema))),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ResolutionError), `contextsFieldName`, `json`, _),
                  Nil
                ),
                None
              ) =>
            ok
          case other => ko(s"[$other] is not expected one")
        }
    }

    "return 2 expected failures for 2 invalid contexts" >> {
      val invalidEmailSentJson = invalidEmailSentData.toJson
      val noSchemaJson = noSchemaData.toJson

      IgluUtils
        .parseAndValidateContexts(
          Some(buildInputContexts(List(invalidEmailSent, noSchema))),
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth,
          SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(NonEmptyList(
                          Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError),
                                                  `contextsFieldName`,
                                                  `invalidEmailSentJson`,
                                                  _
                          ),
                          List(
                            Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ResolutionError),
                                                    `contextsFieldName`,
                                                    `noSchemaJson`,
                                                    _
                            )
                          )
                        ),
                        None
              ) =>
            ok
          case other => ko(s"[$other] is not one ValidationError and one ResolutionError")
        }
    }

    "return an expected failure and an expected SDJ if one context is invalid and one is valid" >> {
      val noSchemaJson = noSchemaData.toJson

      IgluUtils
        .parseAndValidateContexts(
          Some(buildInputContexts(List(emailSent1, noSchema))),
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth,
          SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(NonEmptyList(
                          Failure.SchemaViolation(_: FailureDetails.SchemaViolation.IgluError, `contextsFieldName`, `noSchemaJson`, _),
                          Nil
                        ),
                        Some(Contexts(contexts))
              ) if contexts.head.sdj.schema == emailSentSchema =>
            ok
          case other => ko(s"[$other] is not one IgluError and one SDJ with schema $emailSentSchema")
        }
    }

    "return the extracted SDJs for 2 valid input contexts" >> {
      IgluUtils
        .parseAndValidateContexts(
          Some(buildInputContexts(List(emailSent1, emailSent2))),
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth,
          SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Right(Some(Contexts(contexts)))
              if contexts.size == 2 && contexts.forall(c => c.sdj.schema == emailSentSchema && c.validationInfo.isEmpty) =>
            ok
          case other =>
            ko(s"[$other] is not 2 SDJs with expected schema [${emailSentSchema.toSchemaUri}]")
        }
    }

    "return the extracted SDJ for an input that has a required property set to null if the schema explicitly allows it" >> {
      IgluUtils
        .parseAndValidateContexts(Some(buildInputContexts(List(clientSession))),
                                  SpecHelpers.client,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Right(Some(Contexts(contexts))) if contexts.size == 1 && contexts.forall(_.sdj.schema == clientSessionSchema) =>
            ok
          case other =>
            ko(s"[$other] is not 1 SDJ with expected schema [${clientSessionSchema.toSchemaUri}]")
        }
    }

    "return the extracted context when schema is superseded by another schema" >> {
      IgluUtils
        .parseAndValidateContexts(
          Some(buildInputContexts(List(supersedingExample1, supersedingExample2))),
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth,
          SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Right(Some(Contexts(contexts))) if contexts.size == 2 && contexts.forall(_.sdj.schema == supersedingExampleSchema101) =>
            ok
          case other =>
            ko(s"[$other] is not 2 SDJs with expected schema [${supersedingExampleSchema101.toSchemaUri}]")
        }
    }

    "return a FailureDetails.SchemaViolation.IgluError containing a ValidationError if .data contains context that exceeds the max allowed JSON depth" >> {
      val igluScalaClient = SpecHelpers.client(5)
      val json = s"[$deepJson]".toJson

      IgluUtils
        .parseAndValidateContexts(Some(buildInputContexts(List(deepJson))),
                                  igluScalaClient,
                                  SpecHelpers.registryLookup,
                                  SpecHelpers.DefaultMaxJsonDepth,
                                  SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError), `contextsFieldName`, `json`, _),
                  Nil
                ),
                None
              ) =>
            ok
          case other => ko(s"[$other] is not expected one")
        }
    }
  }

  "validateSDJs" should {
    "return one expected SchemaViolation for one invalid context" >> {
      val json = invalidEmailSentData.toJson
      val contexts = List(
        SpecHelpers.jsonStringToSDJ(invalidEmailSent).right.get
      )

      IgluUtils
        .validateSDJs(SpecHelpers.client, contexts, SpecHelpers.registryLookup, derivedContextsFieldName, SpecHelpers.etlTstamp)
        .value
        .map {
          case Ior.Both(
                NonEmptyList(Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError),
                                                     `derivedContextsFieldName`,
                                                     `json`,
                                                     _
                             ),
                             Nil
                ),
                Nil
              ) =>
            ok
          case other => ko(s"[$other] is not one ValidationError")
        }
    }

    "return two expected SchemaViolation for two invalid contexts" >> {
      val invalidEmailSentJson = invalidEmailSentData.toJson
      val noSchemaJson = noSchemaData.toJson
      val contexts = List(
        SpecHelpers.jsonStringToSDJ(invalidEmailSent).right.get,
        SpecHelpers.jsonStringToSDJ(noSchema).right.get
      )

      IgluUtils
        .validateSDJs(SpecHelpers.client, contexts, SpecHelpers.registryLookup, derivedContextsFieldName, SpecHelpers.etlTstamp)
        .value
        .map {
          case Ior.Both(NonEmptyList(
                          Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError),
                                                  `derivedContextsFieldName`,
                                                  `invalidEmailSentJson`,
                                                  _
                          ),
                          List(
                            Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ResolutionError),
                                                    `derivedContextsFieldName`,
                                                    `noSchemaJson`,
                                                    _
                            )
                          )
                        ),
                        Nil
              ) =>
            ok
          case other => ko(s"[$other] is not one ValidationError and one ResolutionError")
        }
    }

    "return one expected SchemaViolation for one invalid context and one valid" >> {
      val invalidEmailSentJson = invalidEmailSentData.toJson
      val contexts = List(
        SpecHelpers.jsonStringToSDJ(invalidEmailSent).right.get,
        SpecHelpers.jsonStringToSDJ(emailSent1).right.get
      )

      IgluUtils
        .validateSDJs(SpecHelpers.client, contexts, SpecHelpers.registryLookup, derivedContextsFieldName, SpecHelpers.etlTstamp)
        .value
        .map {
          case Ior.Both(NonEmptyList(
                          Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError),
                                                  `derivedContextsFieldName`,
                                                  `invalidEmailSentJson`,
                                                  _
                          ),
                          Nil
                        ),
                        List(valid)
              ) if valid.sdj.schema == emailSentSchema =>
            ok
          case other => ko(s"[$other] is not one ValidationError and one SDJ with schema $emailSentSchema")
        }
    }

    "return 2 valid contexts" >> {
      val contexts = List(
        SpecHelpers.jsonStringToSDJ(emailSent1).right.get,
        SpecHelpers.jsonStringToSDJ(emailSent2).right.get
      )

      IgluUtils
        .validateSDJs(SpecHelpers.client, contexts, SpecHelpers.registryLookup, derivedContextsFieldName, SpecHelpers.etlTstamp)
        .value
        .map {
          case Ior.Right(List(valid1, valid2)) if valid1.sdj.schema == emailSentSchema && valid2.sdj.schema == emailSentSchema => ok
          case other => ko(s"[$other] doesn't contain 2 valid contexts with schema $emailSentSchema")
        }
    }
  }

  "parseAndValidateInput" should {
    "return one SchemaViolation if the input event contains an invalid unstructured event" >> {
      val input = IgluUtils.EventExtractInput(unstructEvent = Some(buildUnstruct(invalidEmailSent)), contexts = None)

      IgluUtils
        .parseAndValidateInput(
          input,
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth,
          SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  _: Failure.SchemaViolation,
                  Nil
                ),
                (None, None)
              ) =>
            ok
          case other => ko(s"[$other] isn't an error with SchemaViolation")
        }
    }

    "return one SchemaViolation if the input event contains one invalid context" >> {
      val input = IgluUtils.EventExtractInput(unstructEvent = None, contexts = Some(buildInputContexts(List(invalidEmailSent))))

      IgluUtils
        .parseAndValidateInput(
          input,
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth,
          SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  _: Failure.SchemaViolation,
                  Nil
                ),
                (None, None)
              ) =>
            ok
          case other => ko(s"[$other] isn't an error with SchemaViolation")
        }
    }

    "return two SchemaViolation if the input event contains an invalid unstructured event and one invalid context" >> {
      val input =
        IgluUtils.EventExtractInput(unstructEvent = Some(invalidEmailSent), contexts = Some(buildInputContexts(List(invalidEmailSent))))

      IgluUtils
        .parseAndValidateInput(
          input,
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth,
          SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  _: Failure.SchemaViolation,
                  List(_: Failure.SchemaViolation)
                ),
                (None, None)
              ) =>
            ok
          case other => ko(s"[$other] isn't 2 errors with SchemaViolation")
        }
    }

    "return the extracted unstructured event and the extracted input contexts if they are all valid" >> {
      val input =
        IgluUtils.EventExtractInput(unstructEvent = Some(buildUnstruct(emailSent1)),
                                    contexts = Some(buildInputContexts(List(emailSent1, emailSent2)))
        )

      IgluUtils
        .parseAndValidateInput(
          input,
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth,
          SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Right((Some(Unstruct(unstruct)), Some(Contexts(contexts))))
              if contexts.size == 2
                && (List(unstruct.sdj) ++ contexts.toList.map(_.sdj)).forall(_.schema == emailSentSchema) =>
            ok
          case other =>
            ko(
              s"[$other] doesn't contain the two contexts and the unstructured event"
            )
        }
    }

    "return the SchemaViolation of the invalid context in the Left and the extracted unstructured event in the Right" >> {
      val invalidEmailSentJson = invalidEmailSentData.toJson
      val input = IgluUtils.EventExtractInput(unstructEvent = Some(buildUnstruct(emailSent1)),
                                              contexts = Some(buildInputContexts(List(invalidEmailSent)))
      )

      IgluUtils
        .parseAndValidateInput(
          input,
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth,
          SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError),
                                                     `contextsFieldName`,
                                                     `invalidEmailSentJson`,
                                                     _
                             ),
                             _
                ),
                (Some(Unstruct(unstruct)), None)
              ) if unstruct.sdj.schema == emailSentSchema =>
            ok
          case other =>
            ko(
              s"[$other] isn't one ValidationError and an unstructured event with schema $emailSentSchema"
            )
        }
    }

    "return the SchemaViolation of the invalid unstructured event in the Left and the valid context in the Right" >> {
      val invalidEmailSentJson = invalidEmailSentData.toJson
      val input = IgluUtils.EventExtractInput(unstructEvent = Some(buildUnstruct(invalidEmailSent)),
                                              contexts = Some(buildInputContexts(List(emailSent1)))
      )

      IgluUtils
        .parseAndValidateInput(
          input,
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth,
          SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError),
                                                     `unstructFieldName`,
                                                     `invalidEmailSentJson`,
                                                     _
                             ),
                             _
                ),
                (None, Some(Contexts(contexts)))
              ) if contexts.size == 1 && contexts.head.sdj.schema == emailSentSchema =>
            ok
          case other =>
            ko(
              s"[$other] isn't one ValidationError and one context with schema $emailSentSchema"
            )
        }
    }

    "return a NotIglu error if a context has a schema vendor exceeding 128 characters" >> {
      val longVendor = "a" * 129
      val longVendorContext = s"""{
        "schema": "iglu:$longVendor/email_sent/jsonschema/1-0-0",
        "data": {"emailAddress": "hello@world.com", "emailAddress2": "foo@bar.org"}
      }"""

      IgluUtils
        .parseAndValidateContexts(
          Some(buildInputContexts(List(longVendorContext))),
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth,
          SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(
                    FailureDetails.SchemaViolation.NotIglu(_, ParseError.InvalidIgluUri),
                    `contextsFieldName`,
                    _,
                    _
                  ),
                  Nil
                ),
                None
              ) =>
            ok
          case other => ko(s"[$other] is not a NotIglu error with InvalidIgluUri")
        }
    }

    "return a NotIglu error if a context has a schema name exceeding 128 characters" >> {
      val longName = "a" * 129
      val longNameContext = s"""{
        "schema": "iglu:com.acme/$longName/jsonschema/1-0-0",
        "data": {"emailAddress": "hello@world.com", "emailAddress2": "foo@bar.org"}
      }"""

      IgluUtils
        .parseAndValidateContexts(
          Some(buildInputContexts(List(longNameContext))),
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth,
          SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(
                    FailureDetails.SchemaViolation.NotIglu(_, ParseError.InvalidIgluUri),
                    `contextsFieldName`,
                    _,
                    _
                  ),
                  Nil
                ),
                None
              ) =>
            ok
          case other => ko(s"[$other] is not a NotIglu error with InvalidIgluUri")
        }
    }

    "return the extracted unstructured event and the extracted input contexts when schema is superseded by another schema" >> {
      val input = IgluUtils.EventExtractInput(unstructEvent = Some(buildUnstruct(supersedingExample1)),
                                              contexts = Some(buildInputContexts(List(supersedingExample1, supersedingExample2)))
      )

      val expectedValidationInfo = IgluUtils.ValidationInfo(
        originalSchema = SchemaKey("com.acme", "superseding_example", "jsonschema", SchemaVer.Full(1, 0, 0)),
        validatedWith = SchemaVer.Full(1, 0, 1)
      )

      IgluUtils
        .parseAndValidateInput(
          input,
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth,
          SpecHelpers.etlTstamp
        )
        .value
        .map {
          case Ior.Right((Some(Unstruct(unstruct)), Some(Contexts(contexts))))
              if contexts.size == 2
                && contexts.toList.count(_.sdj.schema == supersedingExampleSchema101) == 2
                && contexts.toList.map(_.validationInfo.get).forall(_ == expectedValidationInfo)
                && unstruct.sdj.schema == supersedingExampleSchema101
                && unstruct.validationInfo == Some(expectedValidationInfo) =>
            ok
          case other =>
            ko(
              s"[$other] doesn't contain the two contexts and the unstructured event with the superseded schema"
            )
        }
    }
  }
}

object IgluUtilsSpec {
  val unstructSchema =
    SchemaKey(
      "com.snowplowanalytics.snowplow",
      "unstruct_event",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )
  val inputContextsSchema =
    SchemaKey(
      "com.snowplowanalytics.snowplow",
      "contexts",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )

  def buildUnstruct(sdj: String) =
    s"""{"schema": "${unstructSchema.toSchemaUri}", "data": $sdj}"""

  def buildInputContexts(sdjs: List[String] = List.empty[String]) =
    s"""{"schema": "${inputContextsSchema.toSchemaUri}", "data": [${sdjs.mkString(",")}]}"""

  implicit class StringToJson(str: String) {
    def toJson: Json = parse(str).toOption.get
  }
}
