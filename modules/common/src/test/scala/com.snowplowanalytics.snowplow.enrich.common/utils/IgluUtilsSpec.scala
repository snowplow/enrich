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

import org.specs2.mutable.Specification
import org.specs2.matcher.ValidatedMatchers

import cats.effect.testing.specs2.CatsEffect

import io.circe.parser.parse

import cats.data.{Ior, NonEmptyList}

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.iglu.client.ClientError.{ResolutionError, ValidationError}

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

class IgluUtilsSpec extends Specification with ValidatedMatchers with CatsEffect {

  val raw = RawEvent(
    CollectorPayload.Api("vendor", "version"),
    Map.empty[String, Option[String]],
    None,
    CollectorPayload.Source("source", "enc", None),
    CollectorPayload.Context(None, None, None, None, Nil, None)
  )
  val processor = Processor("unit tests SCE", "v42")
  val enriched = new EnrichedEvent()

  val notJson = "foo"
  val notIglu = """{"foo":"bar"}"""
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
  val invalidEmailSent = s"""{
    "schema": "${emailSentSchema.toSchemaUri}",
    "data": {
      "emailAddress": "hello@world.com"
    }
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
  val noSchema =
    """{"schema":"iglu:com.snowplowanalytics.snowplow/foo/jsonschema/1-0-0", "data": {}}"""

  "extractAndValidateUnstructEvent" should {
    "return None if unstruct_event field is empty" >> {
      IgluUtils
        .extractAndValidateUnstructEvent(new EnrichedEvent, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Right(None) => ok
          case other => ko(s"[$other] is not a success with None")
        }
    }

    "return a SchemaViolation.NotJson if unstruct_event does not contain a properly formatted JSON string" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(notJson)

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(_: FailureDetails.SchemaViolation.NotJson, _), None) => ok
          case other => ko(s"[$other] is not an error with NotJson")
        }
    }

    "return a SchemaViolation.NotIglu if unstruct_event contains a properly formatted JSON string that is not self-describing" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(notIglu)

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(_: FailureDetails.SchemaViolation.NotIglu, _), None) => ok
          case other => ko(s"[$other] is not an error with NotIglu")
        }
    }

    "return a SchemaViolation.CriterionMismatch if unstruct_event contains a self-describing JSON but not with the expected schema for unstructured events" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(noSchema)

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(_: FailureDetails.SchemaViolation.CriterionMismatch, _), None) => ok
          case other => ko(s"[$other] is not an error with CriterionMismatch")
        }
    }

    "return a SchemaViolation.NotJson if the JSON in .data is not a JSON" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(buildUnstruct(notJson))

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(_: FailureDetails.SchemaViolation.NotJson, _), None) => ok
          case other => ko(s"[$other] is not an error with NotJson")
        }
    }

    "return a SchemaViolation.IgluError containing a ValidationError if the JSON in .data is not self-describing" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(buildUnstruct(notIglu))

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(FailureDetails.SchemaViolation.IgluError(_, ValidationError(_, _)), _), None) => ok
          case Ior.Both(NonEmptyList(ie: FailureDetails.SchemaViolation.IgluError, _), None) =>
            ko(s"IgluError [$ie] is not ValidationError")
          case other => ko(s"[$other] is not an error with IgluError")
        }
    }

    "return a SchemaViolation.IgluError containing a ValidationError if the JSON in .data is not a valid SDJ" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(buildUnstruct(invalidEmailSent))

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(FailureDetails.SchemaViolation.IgluError(_, ValidationError(_, _)), _), None) => ok
          case Ior.Both(NonEmptyList(ie: FailureDetails.SchemaViolation.IgluError, _), None) =>
            ko(s"IgluError [$ie] is not ValidationError")
          case other => ko(s"[$other] is not an error with IgluError")
        }
    }

    "return a SchemaViolation.IgluError containing a ResolutionError if the schema of the SDJ in .data can't be resolved" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(buildUnstruct(noSchema))

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(FailureDetails.SchemaViolation.IgluError(_, ResolutionError(_)), _), None) => ok
          case Ior.Both(NonEmptyList(ie: FailureDetails.SchemaViolation.IgluError, _), None) =>
            ko(s"IgluError [$ie] is not a ResolutionError")
          case other => ko(s"[$other] is not an error with IgluError")
        }
    }

    "return the extracted unstructured event if .data is a valid SDJ" >> {
      val input = new EnrichedEvent
      input.setUnstruct_event(buildUnstruct(emailSent1))

      IgluUtils
        .extractAndValidateUnstructEvent(input, SpecHelpers.client, SpecHelpers.registryLookup)
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
        .extractAndValidateUnstructEvent(input1, SpecHelpers.client, SpecHelpers.registryLookup)
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
        .extractAndValidateUnstructEvent(input2, SpecHelpers.client, SpecHelpers.registryLookup)
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
  }

  "extractAndValidateInputContexts" should {
    "return Nil if contexts field is empty" >> {
      IgluUtils
        .extractAndValidateInputContexts(new EnrichedEvent, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Right(Nil) => ok
          case other => ko(s"[$other] is not a success with an empty list")
        }
    }

    "return a SchemaViolation.NotJson if .contexts does not contain a properly formatted JSON string" >> {
      val input = new EnrichedEvent
      input.setContexts(notJson)

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(_: FailureDetails.SchemaViolation.NotJson, Nil), Nil) => ok
          case other => ko(s"[$other] is not an error with NotJson")
        }
    }

    "return a SchemaViolation.NotIglu if .contexts contains a properly formatted JSON string that is not self-describing" >> {
      val input = new EnrichedEvent
      input.setContexts(notIglu)

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(_: FailureDetails.SchemaViolation.NotIglu, Nil), Nil) => ok
          case other => ko(s"[$other] is not an error with NotIglu")
        }
    }

    "return a SchemaViolation.CriterionMismatch if .contexts contains a self-describing JSON but not with the right schema" >> {
      val input = new EnrichedEvent
      input.setContexts(noSchema)

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(_: FailureDetails.SchemaViolation.CriterionMismatch, Nil), Nil) => ok
          case other => ko(s"[$other] is not an error with CriterionMismatch")
        }
    }

    "return a SchemaViolation.IgluError containing a ValidationError if .data does not contain an array of JSON objects" >> {
      val input = new EnrichedEvent
      val notArrayContexts =
        s"""{"schema": "${inputContextsSchema.toSchemaUri}", "data": ${emailSent1}}"""
      input.setContexts(notArrayContexts)

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(FailureDetails.SchemaViolation.IgluError(_, ValidationError(_, _)), Nil), Nil) =>
            ok
          case Ior.Both(NonEmptyList(ie: FailureDetails.SchemaViolation.IgluError, Nil), Nil) =>
            ko(s"IgluError [$ie] is not ValidationError")
          case other => ko(s"[$other] is not an error with IgluError")
        }
    }

    "return a SchemaViolation.IgluError containing a ValidationError if .data contains one invalid context" >> {
      val input = new EnrichedEvent
      input.setContexts(buildInputContexts(List(invalidEmailSent)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(FailureDetails.SchemaViolation.IgluError(_, ValidationError(_, _)), Nil), Nil) =>
            ok
          case Ior.Both(NonEmptyList(ie: FailureDetails.SchemaViolation.IgluError, Nil), Nil) =>
            ko(s"IgluError [$ie] is not ValidationError")
          case other => ko(s"[$other] is not an error with IgluError")
        }
    }

    "return a SchemaViolation.IgluError containing a ResolutionError if .data contains one context whose schema can't be resolved" >> {
      val input = new EnrichedEvent
      input.setContexts(buildInputContexts(List(noSchema)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(FailureDetails.SchemaViolation.IgluError(_, ResolutionError(_)), Nil), Nil) =>
            ok
          case Ior.Both(NonEmptyList(ie: FailureDetails.SchemaViolation.IgluError, Nil), Nil) =>
            ko(s"IgluError [$ie] is not ResolutionError")
          case other => ko(s"[$other] is not an error with IgluError")
        }
    }

    "return 2 expected failures for 2 invalid contexts" >> {
      val input = new EnrichedEvent
      input.setContexts(buildInputContexts(List(invalidEmailSent, noSchema)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(
                          FailureDetails.SchemaViolation.IgluError(_, ValidationError(_, _)),
                          List(FailureDetails.SchemaViolation.IgluError(_, ResolutionError(_)))
                        ),
                        Nil
              ) =>
            ok
          case other => ko(s"[$other] is not one ValidationError and one ResolutionError")
        }
    }

    // TODO: check good schema
    "return an expected failure if one context is valid and the other invalid" >> {
      val input = new EnrichedEvent
      input.setContexts(buildInputContexts(List(emailSent1, noSchema)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(_: FailureDetails.SchemaViolation.IgluError, Nil), List(_)) => ok
          case other => ko(s"[$other] is not one IgluError and one valid SDJ")
        }
    }

    "return the extracted SDJs for 2 valid input contexts" >> {
      val input = new EnrichedEvent
      input.setContexts(buildInputContexts(List(emailSent1, emailSent2)))

      IgluUtils
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup)
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
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup)
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
        .extractAndValidateInputContexts(input, SpecHelpers.client, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Right(sdjs) if sdjs.size == 2 && sdjs.forall(_.sdj.schema == supersedingExampleSchema101) =>
            ok
          case other =>
            ko(s"[$other] is not 2 SDJs with expected schema [${supersedingExampleSchema101.toSchemaUri}]")
        }
    }
  }

  "validateEnrichmentsContexts" should {
    "return one expected failure for one invalid context" >> {
      val contexts = List(
        SpecHelpers.jsonStringToSDJ(invalidEmailSent).right.get
      )

      IgluUtils
        .validateEnrichmentsContexts(SpecHelpers.client, contexts, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(
                          FailureDetails.EnrichmentFailure(
                            _,
                            FailureDetails.EnrichmentFailureMessage.IgluError(_, ValidationError(_, _))
                          ),
                          _
                        ),
                        Nil
              ) =>
            ok
          case other => ko(s"[$other] is not one EnrichmentFailure with ValidationError")
        }
    }

    "return 2 expected failures for 2 invalid contexts" >> {
      val contexts = List(
        SpecHelpers.jsonStringToSDJ(invalidEmailSent).right.get,
        SpecHelpers.jsonStringToSDJ(noSchema).right.get
      )

      IgluUtils
        .validateEnrichmentsContexts(SpecHelpers.client, contexts, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(
                          FailureDetails.EnrichmentFailure(
                            _,
                            FailureDetails.EnrichmentFailureMessage.IgluError(_, ValidationError(_, _))
                          ),
                          List(
                            FailureDetails.EnrichmentFailure(
                              _,
                              FailureDetails.EnrichmentFailureMessage.IgluError(_, ResolutionError(_))
                            )
                          )
                        ),
                        Nil
              ) =>
            ok
          case other => ko(s"[$other] is not one is not one ValidationError and one ResolutionError")
        }
    }

    // TODO: check schema in the Right
    "return a failure and a SDJ for one valid context and one invalid" >> {
      val contexts = List(
        SpecHelpers.jsonStringToSDJ(invalidEmailSent).right.get,
        SpecHelpers.jsonStringToSDJ(emailSent1).right.get
      )

      IgluUtils
        .validateEnrichmentsContexts(SpecHelpers.client, contexts, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Both(NonEmptyList(
                          FailureDetails.EnrichmentFailure(
                            _,
                            FailureDetails.EnrichmentFailureMessage.IgluError(_, ValidationError(_, _))
                          ),
                          Nil
                        ),
                        List(_)
              ) =>
            ok
          case other => ko(s"[$other] is not one error with a ValidationError and one valid SDJ")
        }
    }

    // TODO: check the schemas
    "return 2 valid contexts" >> {
      val contexts = List(
        SpecHelpers.jsonStringToSDJ(emailSent1).right.get,
        SpecHelpers.jsonStringToSDJ(emailSent2).right.get
      )

      IgluUtils
        .validateEnrichmentsContexts(SpecHelpers.client, contexts, SpecHelpers.registryLookup)
        .value
        .map {
          case Ior.Right(List(_, _)) => ok
          case other => ko(s"[$other] doesn't contain the 2 valid contexts")
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
            SpecHelpers.registryLookup
          )
          .value
          .map {
            case Ior.Both(
                  NonEmptyList(
                    _: FailureDetails.SchemaViolation,
                    Nil
                  ),
                  IgluUtils.EventExtractResult(Nil, None, Nil)
                ) =>
              ok
            case other => ko(s"[$other] isn't an error with SchemaViolation")
          }
      }

      "return one SchemaViolation if the input event contains an invalid context" >> {
        val input = new EnrichedEvent
        input.setContexts(buildInputContexts(List(invalidEmailSent)))

        IgluUtils
          .extractAndValidateInputJsons(
            input,
            SpecHelpers.client,
            SpecHelpers.registryLookup
          )
          .value
          .map {
            case Ior.Both(
                  NonEmptyList(
                    _: FailureDetails.SchemaViolation,
                    Nil
                  ),
                  IgluUtils.EventExtractResult(Nil, None, Nil)
                ) =>
              ok
            case other => ko(s"[$other] isn't an error with SchemaViolation")
          }
      }

      "return 2 SchemaViolation if the input event contains an invalid unstructured event and 1 invalid context" >> {
        val input = new EnrichedEvent
        input.setUnstruct_event(invalidEmailSent)
        input.setContexts(buildInputContexts(List(invalidEmailSent)))

        IgluUtils
          .extractAndValidateInputJsons(
            input,
            SpecHelpers.client,
            SpecHelpers.registryLookup
          )
          .value
          .map {
            case Ior.Both(
                  NonEmptyList(
                    _: FailureDetails.SchemaViolation,
                    List(_: FailureDetails.SchemaViolation)
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
            SpecHelpers.registryLookup
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
                s"[$other] doesn't contain the 2 contexts and the unstructured event"
              )
          }
      }

      "return the extracted unstructured event and the extracted input contexts when schema is superseded by another schema" >> {
        val input = new EnrichedEvent
        input.setUnstruct_event(buildUnstruct(supersedingExample1))
        input.setContexts(buildInputContexts(List(supersedingExample1, supersedingExample2)))

        val expectedValidationInfoContext = parse(
          """ {
          | "originalSchema" : "iglu:com.acme/superseding_example/jsonschema/1-0-0",
          | "validatedWith" : "1-0-1"
          |}""".stripMargin
        ).toOption.get

        IgluUtils
          .extractAndValidateInputJsons(
            input,
            SpecHelpers.client,
            SpecHelpers.registryLookup
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
                s"[$other] doesn't contain the 2 contexts and the unstructured event with the superseded schema"
              )
          }
      }
    }
  }

  def buildUnstruct(sdj: String) =
    s"""{"schema": "${unstructSchema.toSchemaUri}", "data": $sdj}"""

  def buildInputContexts(sdjs: List[String] = List.empty[String]) =
    s"""{"schema": "${inputContextsSchema.toSchemaUri}", "data": [${sdjs.mkString(",")}]}"""
}
