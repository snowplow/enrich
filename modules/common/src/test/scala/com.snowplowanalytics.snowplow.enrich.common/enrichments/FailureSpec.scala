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
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import java.time.Instant

import scala.collection.immutable.SortedMap

import cats.effect.testing.specs2.CatsEffect
import cats.effect.unsafe.implicits.global
import cats.effect.IO

import cats.data.NonEmptyList
import cats.syntax.option._

import io.circe.syntax._
import io.circe.Json

import org.specs2.mutable.Specification
import org.specs2.matcher.ValidatedMatchers
import org.specs2.ScalaCheck

import org.scalacheck.{Gen, Prop}

import com.snowplowanalytics.snowplow.badrows.{FailureDetails, Processor}

import com.snowplowanalytics.iglu.core.{ParseError, SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.iglu.client.ClientError
import com.snowplowanalytics.iglu.client.validator.{ValidatorError, ValidatorReport}
import com.snowplowanalytics.iglu.client.resolver.LookupHistory
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import com.snowplowanalytics.snowplow.enrich.common.utils.AtomicError

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers

class FailureSpec extends Specification with ValidatedMatchers with CatsEffect with ScalaCheck {

  val timestamp = Instant.now()
  val processor = Processor("unit tests SCE", "v42")
  val schemaKey = SchemaKey("com.snowplowanalytics", "test", "jsonschema", SchemaVer.Full(1, 0, 0))
  val schemaCriterion = SchemaCriterion.apply("com.snowplowanalytics", "test", "jsonschema", 1)

  "FailureEntityContext should be valid against its schema" >> {
    implicit val registryLookup: RegistryLookup[IO] = SpecHelpers.registryLookup

    val genFeContext = for {
      failureType <- Gen.alphaNumStr
      jsonGen = Gen.oneOf(
                  Json.obj(),
                  Json.obj("test1" := "value1"),
                  Json.obj("test1" := "value1", "test2" := "value2"),
                  Json.obj("test1" := "value1", "test2" := "value2", "test3" := "value3")
                )
      errors <- Gen.listOf(jsonGen)
      data <- Gen.option(jsonGen)
      schema <- Gen.option(Gen.const(schemaKey))
    } yield Failure.FailureContext(
      failureType = failureType,
      errors = errors,
      schema = schema,
      data = data,
      timestamp = timestamp,
      componentName = processor.artifact,
      componentVersion = processor.version
    )

    Prop.forAll(genFeContext) { feContext: Failure.FailureContext =>
      val sdj = SelfDescribingData(schema = Failure.failureSchemaKey, data = feContext.asJson)
      SpecHelpers.client
        .check(sdj)
        .value
        .map(_ must beRight)
        .unsafeRunSync()
    }
  }

  "fromEnrichmentFailure" should {
    "convert InputData correctly" >> {
      val ef = Failure.EnrichmentFailure(
        enrichmentFailure = FailureDetails.EnrichmentFailure(
          enrichment = FailureDetails
            .EnrichmentInformation(
              schemaKey = schemaKey,
              identifier = "enrichmentId"
            )
            .some,
          message = FailureDetails.EnrichmentFailureMessage.InputData(
            field = "testField",
            value = "testValue".some,
            expectation = "testExpectation"
          )
        )
      )
      val result = Failure.fromEnrichmentFailure(ef, timestamp, processor)
      val expected = Failure.FailureContext(
        failureType = "EnrichmentError: enrichmentId",
        errors = List(
          Json.obj(
            "message" := "testField - testExpectation",
            "source" := "testField"
          )
        ),
        schema = schemaKey.some,
        data = Json.obj("testField" := "testValue").some,
        timestamp = timestamp,
        componentName = processor.artifact,
        componentVersion = processor.version
      )
      result must beEqualTo(expected)
    }

    "convert Simple correctly" >> {
      val ef = Failure.EnrichmentFailure(
        enrichmentFailure = FailureDetails.EnrichmentFailure(
          enrichment = FailureDetails
            .EnrichmentInformation(
              schemaKey = schemaKey,
              identifier = "enrichmentId"
            )
            .some,
          message = FailureDetails.EnrichmentFailureMessage.Simple(error = "testError")
        )
      )
      val result = Failure.fromEnrichmentFailure(ef, timestamp, processor)
      val expected = Failure.FailureContext(
        failureType = "EnrichmentError: enrichmentId",
        errors = List(Json.obj("message" := "testError")),
        schema = schemaKey.some,
        data = None,
        timestamp = timestamp,
        componentName = processor.artifact,
        componentVersion = processor.version
      )
      result must beEqualTo(expected)
    }
  }

  "fromSchemaViolation" should {
    "convert NotJson correctly" >> {
      val sv = Failure.SchemaViolation(
        schemaViolation = FailureDetails.SchemaViolation.NotJson(
          field = "testField",
          value = "testValue".some,
          error = "testError"
        ),
        source = "testSource",
        data = "testData".asJson
      )
      val fe = Failure.fromSchemaViolation(sv, timestamp, processor)
      val expected = Failure.FailureContext(
        failureType = "NotJSON",
        errors = List(
          Json.obj(
            "message" := "testError",
            "source" := "testSource"
          )
        ),
        schema = None,
        data = Json.obj("testSource" := "testData").some,
        timestamp = timestamp,
        componentName = processor.artifact,
        componentVersion = processor.version
      )
      fe must beEqualTo(expected)
    }

    "convert NotIglu correctly" >> {
      val sv = Failure.SchemaViolation(
        schemaViolation = FailureDetails.SchemaViolation.NotIglu(
          json = Json.Null,
          error = ParseError.InvalidSchema
        ),
        source = "testSource",
        data = "testData".asJson
      )
      val fe = Failure.fromSchemaViolation(sv, timestamp, processor)
      val expected = Failure.FailureContext(
        failureType = "NotIglu",
        errors = List(
          Json.obj(
            "message" := "Invalid schema",
            "source" := "testSource"
          )
        ),
        schema = None,
        data = "testData".asJson.some,
        timestamp = timestamp,
        componentName = processor.artifact,
        componentVersion = processor.version
      )
      fe must beEqualTo(expected)
    }

    "convert CriterionMismatch correctly" >> {
      val sv = Failure.SchemaViolation(
        schemaViolation = FailureDetails.SchemaViolation.CriterionMismatch(
          schemaKey = schemaKey,
          schemaCriterion = schemaCriterion
        ),
        source = "testSource",
        data = "testData".asJson
      )
      val fe = Failure.fromSchemaViolation(sv, timestamp, processor)
      val expected = Failure.FailureContext(
        failureType = "CriterionMismatch",
        errors = List(
          Json.obj(
            "message" := "Unexpected schema: iglu:com.snowplowanalytics/test/jsonschema/1-0-0 does not match the criterion",
            "source" := "testSource",
            "criterion" := "iglu:com.snowplowanalytics/test/jsonschema/1-*-*"
          )
        ),
        schema = schemaKey.some,
        data = "testData".asJson.some,
        timestamp = timestamp,
        componentName = processor.artifact,
        componentVersion = processor.version
      )
      fe must beEqualTo(expected)
    }

    "convert ResolutionError correctly" >> {
      val sv = Failure.SchemaViolation(
        schemaViolation = FailureDetails.SchemaViolation.IgluError(
          schemaKey = schemaKey,
          error = ClientError.ResolutionError(
            value = SortedMap(
              "repo1" -> LookupHistory(
                errors = Set.empty,
                attempts = 1,
                lastAttempt = timestamp
              ),
              "repo2" -> LookupHistory(
                errors = Set.empty,
                attempts = 2,
                lastAttempt = timestamp
              )
            )
          )
        ),
        source = "testSource",
        data = "testData".asJson
      )
      val fe = Failure.fromSchemaViolation(sv, timestamp, processor)
      val expected = Failure.FailureContext(
        failureType = "ResolutionError",
        errors = List(
          Json.obj(
            "message" := "Resolution error: schema iglu:com.snowplowanalytics/test/jsonschema/1-0-0 not found",
            "source" := "testSource",
            "lookupHistory" := Json.arr(
              Json.obj("repository" := "repo1", "errors" := List.empty[String], "attempts" := 1, "lastAttempt" := timestamp),
              Json.obj("repository" := "repo2", "errors" := List.empty[String], "attempts" := 2, "lastAttempt" := timestamp)
            )
          )
        ),
        schema = schemaKey.some,
        data = "testData".asJson.some,
        timestamp = timestamp,
        componentName = processor.artifact,
        componentVersion = processor.version
      )
      fe must beEqualTo(expected)
    }

    "convert InvalidData correctly" >> {
      def createSv(schemaKey: SchemaKey) = {
        val (source, targets1, targets2, keyword1, keyword2) =
          if (schemaKey == AtomicFields.atomicSchema)
            (AtomicError.source, Nil, Nil, AtomicError.keywordParse, AtomicError.keywordLength)
          else
            ("testSource", List("testTarget1"), List("testTarget2"), "testKeyword1", "testKeyword2")
        Failure.SchemaViolation(
          schemaViolation = FailureDetails.SchemaViolation.IgluError(
            schemaKey = schemaKey,
            error = ClientError.ValidationError(
              error = ValidatorError.InvalidData(
                messages = NonEmptyList.of(
                  ValidatorReport(message = "message1", path = "path1".some, targets = targets1, keyword = keyword1.some),
                  ValidatorReport(message = "message2", path = "path2".some, targets = targets2, keyword = keyword2.some)
                )
              ),
              supersededBy = None
            )
          ),
          source = source,
          data = "testData".asJson
        )
      }

      val svWithAtomicSchema = createSv(AtomicFields.atomicSchema)
      val svWithOrdinarySchema = createSv(schemaKey)
      val feWithAtomicSchema = Failure.fromSchemaViolation(svWithAtomicSchema, timestamp, processor)
      val feWithOrdinarySchema = Failure.fromSchemaViolation(svWithOrdinarySchema, timestamp, processor)
      val expectedWithAtomicSchema = Failure.FailureContext(
        failureType = "ValidationError",
        errors = List(
          Json.obj("message" := "message1",
                   "source" := AtomicError.source,
                   "path" := "path1",
                   "keyword" := AtomicError.keywordParse,
                   "targets" := List.empty[String]
          ),
          Json.obj("message" := "message2",
                   "source" := AtomicError.source,
                   "path" := "path2",
                   "keyword" := AtomicError.keywordLength,
                   "targets" := List.empty[String]
          )
        ),
        schema = AtomicFields.atomicSchema.some,
        data = "testData".asJson.some,
        timestamp = timestamp,
        componentName = processor.artifact,
        componentVersion = processor.version
      )
      val expectedWithOrdinarySchema = Failure.FailureContext(
        failureType = "ValidationError",
        errors = List(
          Json.obj("message" := "message1",
                   "source" := "testSource",
                   "path" := "path1",
                   "keyword" := "testKeyword1",
                   "targets" := List("testTarget1")
          ),
          Json.obj("message" := "message2",
                   "source" := "testSource",
                   "path" := "path2",
                   "keyword" := "testKeyword2",
                   "targets" := List("testTarget2")
          )
        ),
        schema = schemaKey.some,
        data = "testData".asJson.some,
        timestamp = timestamp,
        componentName = processor.artifact,
        componentVersion = processor.version
      )

      feWithAtomicSchema must beEqualTo(expectedWithAtomicSchema)
      feWithOrdinarySchema must beEqualTo(expectedWithOrdinarySchema)
    }

    "convert InvalidSchema correctly" >> {
      val sv = Failure.SchemaViolation(
        schemaViolation = FailureDetails.SchemaViolation.IgluError(
          schemaKey = schemaKey,
          error = ClientError.ValidationError(
            error = ValidatorError.InvalidSchema(
              issues = NonEmptyList.of(
                ValidatorError.SchemaIssue(path = "testPath1", message = "testMessage1"),
                ValidatorError.SchemaIssue(path = "testPath2", message = "testMessage2")
              )
            ),
            supersededBy = None
          )
        ),
        source = "testSource",
        data = "testData".asJson
      )
      val fe = Failure.fromSchemaViolation(sv, timestamp, processor)
      val expected = Failure.FailureContext(
        failureType = "ValidationError",
        errors = List(
          Json.obj(
            "message" := "Invalid schema: iglu:com.snowplowanalytics/test/jsonschema/1-0-0 - testMessage1",
            "source" := "testSource",
            "path" := "testPath1"
          ),
          Json.obj(
            "message" := "Invalid schema: iglu:com.snowplowanalytics/test/jsonschema/1-0-0 - testMessage2",
            "source" := "testSource",
            "path" := "testPath2"
          )
        ),
        schema = schemaKey.some,
        data = "testData".asJson.some,
        timestamp = timestamp,
        componentName = processor.artifact,
        componentVersion = processor.version
      )
      fe must beEqualTo(expected)
    }
  }
}
