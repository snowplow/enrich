/*
 * Copyright (c) 2024-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import java.time.Instant
import java.util.UUID
import java.nio.ByteBuffer

import org.joda.time.DateTime

import org.specs2.mutable.{Specification => MutSpecification}

import io.circe.Json

import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder

import com.snowplowanalytics.snowplow.enrich.common.utils.ConversionUtils
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

class FailureEntitySpec extends MutSpecification {
  val enriched = new EnrichedEvent
  enriched.app_id = "demo"
  enriched.platform = "web"
  enriched.name_tracker = "scala"
  enriched.v_collector = "scala-stream-collector"
  enriched.v_tracker = "0.0.0"
  enriched.v_etl = "4.3.0"
  enriched.user_id = UUID.randomUUID().toString()
  enriched.event_id = UUID.randomUUID().toString()
  enriched.collector_tstamp =
    EventEnrichments.formatCollectorTstamp(Some(DateTime.now())).getOrElse(throw new RuntimeException("Bad timestamp"))
  enriched.etl_tstamp = enriched.collector_tstamp
  // TODO: set type unstruct

  val timestamp = Instant.now()
  val componentName = "enrich-kinesis"
  val componentVersion = "4.3.0"

  val region = "eu-central-1"
  val streamName = "qa-lnuwhb8s-enriched-stream"
  val kinesisClient = AmazonKinesisClientBuilder.standard.withRegion(region).build()

  "FailureEntity" should {
    "be encoded successfully" in {
      // NotJson
      val failure1 = FailureEntity(
        "SchemaViolation",
        List(
          Json.obj(
            "message" -> Json.fromString("invalid json: expected \" got '{\n    ...' (line 2, column 2)"),
            "source" -> Json.fromString("ue_properties")
          )
        ),
        None,
        Some(Json.obj("ue_properties" -> Json.fromString("\n{{\n        "))),
        Instant.now(),
        componentName,
        componentVersion
      )

      // NotIglu
      val failure2 = FailureEntity(
        "NotIglu",
        List(
          Json.obj(
            "message" -> Json.fromString("Invalid schema found"),
            "source" -> Json.fromString("ue_properties")
          )
        ),
        None,
        Some(Json.obj("foo" -> Json.fromString("bar"))),
        Instant.now(),
        componentName,
        componentVersion
      )

      // CriterionMismatch
      val failure3 = FailureEntity(
        "CriterionMismatch",
        List(
          Json.obj(
            "message" -> Json.fromString("Criterion Mismatch - iglu:com.snowplowanalytics.snowplow/my_unstruct_event/jsonschema/1-0-0"),
            "source" -> Json.fromString("ue_properties"),
            "criterion" -> Json.fromString("iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-*")
          )
        ),
        Some("iglu:com.snowplowanalytics.snowplow/my_unstruct_event/jsonschema/1-0-0"),
        Some(
          Json.obj(
            "schema" -> Json.fromString("com.snowplowanalytics.snowplow/my_unstruct_event/jsonschema/1-0-0"),
            "data" -> Json.obj(
              "foo" -> Json.fromString("bar")
            )
          )
        ),
        Instant.now(),
        componentName,
        componentVersion
      )

      // ResolutionError
      val failure4 = FailureEntity(
        "ResolutionError",
        List(
          Json.obj(
            "message" -> Json.fromString("Resolution Error - iglu:com.acme/email_sent_foo/jsonschema/1-0-0"),
            "source" -> Json.fromString("ue_properties"),
            "lookupHistory" -> Json.arr(
              Json.obj(
                "repository" -> Json.fromString("Iglu Central"),
                "errors" -> Json.arr(
                  Json.obj("error" -> Json.fromString("NotFound"))
                ),
                "attempts" -> Json.fromInt(1),
                "lastAttempt" -> Json.fromString("2024-03-05T14:33:10.095Z")
              ),
              Json.obj(
                "repository" -> Json.fromString("Iglu Client Embedded"),
                "errors" -> Json.arr(
                  Json.obj("error" -> Json.fromString("NotFound"))
                ),
                "attempts" -> Json.fromInt(1),
                "lastAttempt" -> Json.fromString("2024-03-05T14:33:10.095Z")
              )
            )
          )
        ),
        Some("iglu:com.acme/email_sent_foo/jsonschema/1-0-0"),
        Some(
          Json.obj(
            "schema" -> Json.fromString("iglu:com.acme/email_sent_foo/jsonschema/1-0-0"),
            "data" -> Json.obj(
              "emailAddress" -> Json.fromString("hello@world.com"),
              "emailAddress2" -> Json.fromString("foo@bar.org")
            )
          )
        ),
        Instant.now(),
        componentName,
        componentVersion
      )

      // InvalidData
      val failure5 = FailureEntity(
        "ValidationError",
        List(
          Json.obj(
            "message" -> Json.fromString("$.schema: is missing but it is required"),
            "source" -> Json.fromString("ue_properties"),
            "path" -> Json.fromString("$"),
            "keyword" -> Json.fromString("required"),
            "targets" -> Json.arr(Json.fromString("schema"))
          ),
          Json.obj(
            "message" -> Json.fromString("$.data: is missing but it is required"),
            "source" -> Json.fromString("ue_properties"),
            "path" -> Json.fromString("$"),
            "keyword" -> Json.fromString("required"),
            "targets" -> Json.arr(Json.fromString("data"))
          ),
          Json.obj(
            "message" -> Json.fromString("$.foo: is not defined in the schema and the schema does not allow additional properties"),
            "source" -> Json.fromString("ue_properties"),
            "path" -> Json.fromString("$"),
            "keyword" -> Json.fromString("additionalProperties"),
            "targets" -> Json.arr(Json.fromString("foo"))
          )
        ),
        Some("iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0"),
        Some(Json.obj("foo" -> Json.fromString("bar"))),
        Instant.now(),
        componentName,
        componentVersion
      )

      // InvalidSchema
      val failure6 = FailureEntity(
        "ValidationError",
        List(
          Json.obj(
            "message" -> Json.fromString(
              "Invalid schema: iglu:com.acme/malformed_schema/jsonschema/1-0-0 - $.required: there must be a minimum of 1 items in the array"
            ),
            "source" -> Json.fromString("ue_properties"),
            "path" -> Json.fromString("$.required")
          )
        ),
        Some("iglu:com.acme/malformed_schema/jsonschema/1-0-0"),
        Some(
          Json.obj(
            "schema" -> Json.fromString("iglu:com.acme/malformed_schema/jsonschema/1-0-0"),
            "data" -> Json.obj(
              "emailAddress" -> Json.fromString("hello@world.com"),
              "emailAddress2" -> Json.fromString("foo@bar.org")
            )
          )
        ),
        Instant.now(),
        componentName,
        componentVersion
      )

      // Two wrong input atomic fields
      val failure7 = FailureEntity(
        "ValidationError",
        List(
          Json.obj(
            "message" -> Json.fromString(
              "Cannot be converted to java.math.BigDecimal. Error : Character h is neither a decimal digit number, decimal point, nor \"e\" notation exponential mark."
            ),
            "source" -> Json.fromString("tr_tt"),
            "path" -> Json.fromString("tr_tt"),
            "keyword" -> Json.fromString("hello"),
            "targets" -> Json.arr()
          ),
          Json.obj(
            "message" -> Json.fromString(
              "Cannot be converted to java.math.BigDecimal. Error : Character w is neither a decimal digit number, decimal point, nor \"e\" notation exponential mark."
            ),
            "source" -> Json.fromString("tr_tx"),
            "path" -> Json.fromString("tr_tx"),
            "keyword" -> Json.fromString("world"),
            "targets" -> Json.arr()
          )
        ),
        Some("iglu:com.snowplowanalytics.snowplow/atomic/jsonschema/1-0-0"),
        Some(
          Json.obj(
            "tr_tt" -> Json.fromString("hello"),
            "tr_tx" -> Json.fromString("world")
          )
        ),
        Instant.now(),
        componentName,
        componentVersion
      )

      // Atomic field too long
      val failure8 = FailureEntity(
        "ValidationError",
        List(
          Json.obj(
            "message" -> Json.fromString("Field is longer than maximum allowed size 100"),
            "source" -> Json.fromString("v_tracker"),
            "path" -> Json.fromString("v_tracker"),
            "keyword" -> Json.fromString(
              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            "targets" -> Json.arr()
          )
        ),
        Some("iglu:com.snowplowanalytics.snowplow/atomic/jsonschema/1-0-0"),
        Some(
          Json.obj(
            "v_tracker" -> Json.fromString(
              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            )
          )
        ),
        Instant.now(),
        componentName,
        componentVersion
      )

      // Enrichment failure - InputData
      val failure9 = FailureEntity(
        "EnrichmentError - cross-navigation",
        List(
          Json.obj(
            "message" -> Json.fromString("sp_dtm not in the expected format: ms since epoch"),
            "source" -> Json.fromString("sp_dtm")
          )
        ),
        Some("iglu:com.snowplowanalytics.snowplow.enrichments/cross_navigation_config/jsonschema/1-0-0"),
        None,
        Instant.now(),
        componentName,
        componentVersion
      )

      // Enrichment failure - Simple
      val failure10 = FailureEntity(
        "EnrichmentError - Javascript enrichment",
        List(
          Json.obj(
            "message" -> Json.fromString(
              "Error during execution of JavaScript function: [Javascript exception in <eval> at line number 3 at column number 10]"
            )
          )
        ),
        Some("iglu:com.snowplowanalytics.snowplow/javascript_script_config/jsonschema/1-0-0"),
        None,
        Instant.now(),
        componentName,
        componentVersion
      )

      val failures = List(
        // Unstruct
        List(failure1),
        List(failure2),
        List(failure3),
        List(failure4),
        List(failure5),
        List(failure6),
        // Atomic
        List(failure7),
        List(failure8),
        // Enrichment
        List(failure9),
        List(failure10),
        // Mix of failures
        List(failure5, failure7), // unstruct + atomic
        List(failure3, failure10), // unstruct + enrichment
        List(failure8, failure9), // atomic + enrichment
        List(failure1, failure7, failure9) // all
      )

      failures.foreach { fs =>
        FailureEntity.addDerivedContexts(enriched, fs)
        val tsv = ConversionUtils.tabSeparatedEnrichedEvent(enriched)
        val bytes = tsv.getBytes()
        println(tsv)

        val put = new PutRecordRequest()
        put.setStreamName(streamName)
        put.setPartitionKey(enriched.user_id)
        put.setData(ByteBuffer.wrap(bytes))
        kinesisClient.putRecord(put)
      }

      1 must beEqualTo(1)
    }
  }
}
