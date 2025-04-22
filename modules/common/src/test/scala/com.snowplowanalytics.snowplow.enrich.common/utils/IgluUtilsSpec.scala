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

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.iglu.client.ClientError.{ResolutionError, ValidationError}

import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.enrichments.Failure
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
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

  "extractAndValidateUnstructEvent" should {
    "return None if unstruct_event field is empty" >> {
      IgluUtils
        .extractAndValidateUnstructEvent(new EnrichedEvent, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Right(None) => ok
          case other => ko(s"[$other] is not a success with None")
        }
    }

    "return a FailureDetails.SchemaViolation.NotJson if unstruct_event does not contain a properly formatted JSON string" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(notJson)

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
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
      val input = new EnrichedEvent
      input.setUnstruct_event(notIglu)

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
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
      val input = new EnrichedEvent
      input.setUnstruct_event(noSchema)

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
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
      val input = new EnrichedEvent
      val ue = buildUnstruct(notJson)
      val ueJson = ue.asJson
      input.setUnstruct_event(ue)

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
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
      val input = new EnrichedEvent
      val ue = buildUnstruct(notIglu)
      val ueJson = notIglu.toJson
      input.setUnstruct_event(ue)

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
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
      val input = new EnrichedEvent
      val json = invalidEmailSentData.toJson
      input.setUnstruct_event(buildUnstruct(invalidEmailSent))

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
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
      val input = new EnrichedEvent
      val json = noSchemaData.toJson
      input.setUnstruct_event(buildUnstruct(noSchema))

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
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
      val input = new EnrichedEvent
      input.setUnstruct_event(buildUnstruct(emailSent1))

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Right(Some(IgluUtils.SdjExtractResult(sdj, None))) if sdj.schema == emailSentSchema => ok
          case Ior.Right(Some(s)) =>
            ko(
              s"unstructured event's schema [${s.sdj.schema}] does not match expected schema [${emailSentSchema}]"
            )
          case other => ko(s"no unstructured event was extracted [$other]")
        }
    }

    "return the extracted unstructured event when schema is superseded by another schema" >> {
      val input1 = new EnrichedEvent
      input1.setUnstruct_event(buildUnstruct(supersedingExample1))

      val input2 = new EnrichedEvent
      input2.setUnstruct_event(buildUnstruct(supersedingExample2))

      val expectedValidationInfo = IgluUtils.ValidationInfo(supersedingExampleSchema100, supersedingExampleSchema101.version)

      IgluUtils
        .extractAndValidateUnstructEvent(input1, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Right(Some(IgluUtils.SdjExtractResult(sdj, Some(`expectedValidationInfo`))))
              if sdj.schema == supersedingExampleSchema101 =>
            ok
          case Ior.Right(Some(s)) =>
            ko(
              s"unstructured event's schema [${s.sdj.schema}] does not match expected schema [${supersedingExampleSchema101}]"
            )
          case other => ko(s"no unstructured event was extracted [$other]")
        }

      // input2 wouldn't be validated with 1-0-0. It would be validated with 1-0-1 only.
      IgluUtils
        .extractAndValidateUnstructEvent(input2, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Right(Some(IgluUtils.SdjExtractResult(sdj, Some(`expectedValidationInfo`))))
              if sdj.schema == supersedingExampleSchema101 =>
            ok
          case Ior.Right(Some(s)) =>
            ko(
              s"unstructured event's schema [${s.sdj.schema}] does not match expected schema [${supersedingExampleSchema101}]"
            )
          case other => ko(s"no unstructured event was extracted [$other]")
        }
    }

    "return a FailureDetails.SchemaViolation.IgluError containing a ValidationError if the JSON in .data exceeds the max allowed JSON depth" >> {
      val igluScalaClient = SpecHelpers.client(5)
      val input = new EnrichedEvent
      val json = deepJson.toJson
      input.setUnstruct_event(buildUnstruct(deepJson))

      IgluUtils
        .extractAndValidateUnstructEvent(input, igluScalaClient, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
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

  "extractAndValidateInputContexts" should {
    "return Nil if contexts field is empty" >> {
      IgluUtils
        .extractAndValidateInputContexts(new EnrichedEvent, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Right(Nil) => ok
          case other => ko(s"[$other] is not a success with an empty list")
        }
    }

    "return a FailureDetails.SchemaViolation.NotJson if .contexts does not contain a properly formatted JSON string" >> {
      val input = new EnrichedEvent
      input.setContexts(notJson)

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Both(
                NonEmptyList(Failure.SchemaViolation(_: FailureDetails.SchemaViolation.NotJson, `contextsFieldName`, `jsonNotJson`, _),
                             Nil
                ),
                Nil
              ) =>
            ok
          case other => ko(s"[$other] is not an error with NotJson")
        }
    }

    "return a FailureDetails.SchemaViolation.NotIglu if .contexts contains a properly formatted JSON string that is not self-describing" >> {
      val input = new EnrichedEvent
      val json = notIglu.toJson
      input.setContexts(notIglu)

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Both(
                NonEmptyList(Failure.SchemaViolation(_: FailureDetails.SchemaViolation.NotIglu, `contextsFieldName`, `json`, _), Nil),
                Nil
              ) =>
            ok
          case other => ko(s"[$other] is not an error with NotIglu")
        }
    }

    "return a FailureDetails.SchemaViolation.CriterionMismatch if .contexts contains a self-describing JSON but not with the right schema" >> {
      val input = new EnrichedEvent
      val json = noSchemaData.toJson
      input.setContexts(noSchema)

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Both(NonEmptyList(
                          Failure.SchemaViolation(_: FailureDetails.SchemaViolation.CriterionMismatch, `contextsFieldName`, `json`, _),
                          Nil
                        ),
                        Nil
              ) =>
            ok
          case other => ko(s"[$other] is not an error with CriterionMismatch")
        }
    }

    "return a FailureDetails.SchemaViolation.IgluError containing a ValidationError if .data does not contain an array of JSON objects" >> {
      val input = new EnrichedEvent
      val notArrayContexts =
        s"""{"schema": "${inputContextsSchema.toSchemaUri}", "data": ${emailSent1}}"""
      val json = emailSent1.toJson
      input.setContexts(notArrayContexts)

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError), `contextsFieldName`, `json`, _),
                  Nil
                ),
                Nil
              ) =>
            ok
          case other => ko(s"[$other] is not expected one")
        }
    }

    "return a FailureDetails.SchemaViolation.IgluError containing a ValidationError if .data contains one invalid context" >> {
      val input = new EnrichedEvent
      val json = invalidEmailSentData.toJson
      input.setContexts(buildInputContexts(List(invalidEmailSent)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError), `contextsFieldName`, `json`, _),
                  Nil
                ),
                Nil
              ) =>
            ok
          case other => ko(s"[$other] is not expected one")
        }
    }

    "return a FailureDetails.SchemaViolation.IgluError containing a ResolutionError if .data contains one context whose schema can't be resolved" >> {
      val input = new EnrichedEvent
      val json = noSchemaData.toJson
      input.setContexts(buildInputContexts(List(noSchema)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ResolutionError), `contextsFieldName`, `json`, _),
                  Nil
                ),
                Nil
              ) =>
            ok
          case other => ko(s"[$other] is not expected one")
        }
    }

    "return 2 expected failures for 2 invalid contexts" >> {
      val input = new EnrichedEvent
      val invalidEmailSentJson = invalidEmailSentData.toJson
      val noSchemaJson = noSchemaData.toJson
      input.setContexts(buildInputContexts(List(invalidEmailSent, noSchema)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
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
                        Nil
              ) =>
            ok
          case other => ko(s"[$other] is not one ValidationError and one ResolutionError")
        }
    }

    "return an expected failure and an expected SDJ if one context is invalid and one is valid" >> {
      val input = new EnrichedEvent
      val noSchemaJson = noSchemaData.toJson
      input.setContexts(buildInputContexts(List(emailSent1, noSchema)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Both(NonEmptyList(
                          Failure.SchemaViolation(_: FailureDetails.SchemaViolation.IgluError, `contextsFieldName`, `noSchemaJson`, _),
                          Nil
                        ),
                        List(extract)
              ) if extract.sdj.schema == emailSentSchema =>
            ok
          case other => ko(s"[$other] is not one IgluError and one SDJ with schema $emailSentSchema")
        }
    }

    "return the extracted SDJs for 2 valid input contexts" >> {
      val input = new EnrichedEvent
      input.setContexts(buildInputContexts(List(emailSent1, emailSent2)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Right(sdjs) if sdjs.size == 2 && sdjs.forall(i => i.sdj.schema == emailSentSchema && i.validationInfo.isEmpty) =>
            ok
          case other =>
            ko(s"[$other] is not 2 SDJs with expected schema [${emailSentSchema.toSchemaUri}]")
        }
    }

    "return the extracted SDJ for an input that has a required property set to null if the schema explicitly allows it" >> {
      val input = new EnrichedEvent
      input.setContexts(buildInputContexts(List(clientSession)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Right(sdjs) if sdjs.size == 1 && sdjs.forall(_.sdj.schema == clientSessionSchema) =>
            ok
          case other =>
            ko(s"[$other] is not 1 SDJ with expected schema [${clientSessionSchema.toSchemaUri}]")
        }
    }

    "return the extracted context when schema is superseded by another schema" >> {
      val input = new EnrichedEvent
      input.setContexts(buildInputContexts(List(supersedingExample1, supersedingExample2)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Right(sdjs) if sdjs.size == 2 && sdjs.forall(_.sdj.schema == supersedingExampleSchema101) =>
            ok
          case other =>
            ko(s"[$other] is not 2 SDJs with expected schema [${supersedingExampleSchema101.toSchemaUri}]")
        }
    }

    "return a FailureDetails.SchemaViolation.IgluError containing a ValidationError if .data contains context that exceeds the max allowed JSON depth" >> {
      val igluScalaClient = SpecHelpers.client(5)
      val input = new EnrichedEvent
      val json = s"[$deepJson]".toJson
      input.setContexts(buildInputContexts(List(deepJson)))

      IgluUtils
        .extractAndValidateInputContexts(input, igluScalaClient, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  Failure.SchemaViolation(FailureDetails.SchemaViolation.IgluError(_, _: ValidationError), `contextsFieldName`, `json`, _),
                  Nil
                ),
                Nil
              ) =>
            ok
          case other => ko(s"[$other] is not expected one")
        }
    }
  }

  "validateEnrichmentsContexts" should {
    "return one expected SchemaViolation for one invalid context" >> {
      val json = invalidEmailSentData.toJson
      val contexts = List(
        SpecHelpers.jsonStringToSDJ(invalidEmailSent).right.get
      )

      IgluUtils
        .validateEnrichmentsContexts(SpecHelpers.client, contexts, SpecHelpers.registryLookup)
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
        .validateEnrichmentsContexts(SpecHelpers.client, contexts, SpecHelpers.registryLookup)
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
        .validateEnrichmentsContexts(SpecHelpers.client, contexts, SpecHelpers.registryLookup)
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
                        List(sdj)
              ) if sdj.schema == emailSentSchema =>
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
        .validateEnrichmentsContexts(SpecHelpers.client, contexts, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Right(List(sdj1, sdj2)) if sdj1.schema == emailSentSchema && sdj2.schema == emailSentSchema => ok
          case other => ko(s"[$other] doesn't contain 2 valid contexts with schema $emailSentSchema")
        }
    }
  }

  "extractAndValidateInputJsons" should {
    "return one SchemaViolation if the input event contains an invalid unstructured event" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(buildUnstruct(invalidEmailSent))

      IgluUtils
        .extractAndValidateInputJsons(
          input,
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  _: Failure.SchemaViolation,
                  Nil
                ),
                IgluUtils.EventExtractResult(Nil, None, Nil)
              ) =>
            ok
          case other => ko(s"[$other] isn't an error with SchemaViolation")
        }
    }

    "return one SchemaViolation if the input event contains one invalid context" >> {
      val input = new EnrichedEvent
      input.setContexts(buildInputContexts(List(invalidEmailSent)))

      IgluUtils
        .extractAndValidateInputJsons(
          input,
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  _: Failure.SchemaViolation,
                  Nil
                ),
                IgluUtils.EventExtractResult(Nil, None, Nil)
              ) =>
            ok
          case other => ko(s"[$other] isn't an error with SchemaViolation")
        }
    }

    "return two SchemaViolation if the input event contains an invalid unstructured event and one invalid context" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(invalidEmailSent)
      input.setContexts(buildInputContexts(List(invalidEmailSent)))

      IgluUtils
        .extractAndValidateInputJsons(
          input,
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth
        )
        .value
        .map {
          case Ior.Both(
                NonEmptyList(
                  _: Failure.SchemaViolation,
                  List(_: Failure.SchemaViolation)
                ),
                IgluUtils.EventExtractResult(Nil, None, Nil)
              ) =>
            ok
          case other => ko(s"[$other] isn't 2 errors with SchemaViolation")
        }
    }

    "return the extracted unstructured event and the extracted input contexts if they are all valid" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(buildUnstruct(emailSent1))
      input.setContexts(buildInputContexts(List(emailSent1, emailSent2)))

      IgluUtils
        .extractAndValidateInputJsons(
          input,
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth
        )
        .value
        .map {
          case Ior.Right(IgluUtils.EventExtractResult(contexts, Some(unstructEvent), validationInfos))
              if contexts.size == 2
                && validationInfos.isEmpty
                && (unstructEvent :: contexts).forall(_.schema == emailSentSchema) =>
            ok
          case other =>
            ko(
              s"[$other] doesn't contain the two contexts and the unstructured event"
            )
        }
    }

    "return the SchemaViolation of the invalid context in the Left and the extracted unstructured event in the Right" >> {
      val input = new EnrichedEvent
      val invalidEmailSentJson = invalidEmailSentData.toJson
      input.setUnstruct_event(buildUnstruct(emailSent1))
      input.setContexts(buildInputContexts(List(invalidEmailSent)))

      IgluUtils
        .extractAndValidateInputJsons(
          input,
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth
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
                extract
              ) if extract.contexts.isEmpty && extract.unstructEvent.isDefined && extract.unstructEvent.get.schema == emailSentSchema =>
            ok
          case other =>
            ko(
              s"[$other] isn't one ValidationError and an unstructured event with schema $emailSentSchema"
            )
        }
    }

    "return the SchemaViolation of the invalid unstructured event in the Left and the valid context in the Right" >> {
      val input = new EnrichedEvent
      val invalidEmailSentJson = invalidEmailSentData.toJson
      input.setUnstruct_event(buildUnstruct(invalidEmailSent))
      input.setContexts(buildInputContexts(List(emailSent1)))

      IgluUtils
        .extractAndValidateInputJsons(
          input,
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth
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
                extract
              ) if extract.contexts.size == 1 && extract.contexts.head.schema == emailSentSchema && extract.unstructEvent.isEmpty =>
            ok
          case other =>
            ko(
              s"[$other] isn't one ValidationError and one context with schema $emailSentSchema"
            )
        }
    }

    "return the extracted unstructured event and the extracted input contexts when schema is superseded by another schema" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(buildUnstruct(supersedingExample1))
      input.setContexts(buildInputContexts(List(supersedingExample1, supersedingExample2)))

      val expectedValidationInfoContext =
        """ {
          | "originalSchema" : "iglu:com.acme/superseding_example/jsonschema/1-0-0",
          | "validatedWith" : "1-0-1"
          |}""".stripMargin.toJson

      IgluUtils
        .extractAndValidateInputJsons(
          input,
          SpecHelpers.client,
          SpecHelpers.registryLookup,
          SpecHelpers.DefaultMaxJsonDepth
        )
        .value
        .map {
          case Ior.Right(IgluUtils.EventExtractResult(contexts, Some(unstructEvent), List(validationInfo)))
              if contexts.size == 2
                && unstructEvent.schema == supersedingExampleSchema101
                && contexts.count(_.schema == supersedingExampleSchema101) == 2
                && validationInfo.schema == IgluUtils.ValidationInfo.schemaKey
                && validationInfo.data == expectedValidationInfoContext =>
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
