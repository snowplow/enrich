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
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import org.specs2.mutable.Specification

import org.joda.time.DateTime

import io.circe.syntax._

import cats.implicits._
import cats.data.Ior

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect

import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.AcceptInvalid

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class IncompleteFailuresSpec extends Specification with CatsEffect {
  import IncompleteFailuresSpec._

  "schema violation types" should {
    // mapping error
    // collector timestamp

    "unstruct bad JSON" >> {
      val unstruct =
        """
        {{
        """
      enrich(unstruct).map {
        case sv: BadRow.SchemaViolations =>
          ko(sv.selfDescribingData.asJson.spaces2)
        case other => ko(s"[$other] is not a SchemaViolations bad row")
      }
    }

    "unstruct not SDJ" >> {
      val unstruct =
        """
        {"foo": "bar"}
        """
      enrich(unstruct).map {
        case sv: BadRow.SchemaViolations =>
          ko(sv.selfDescribingData.asJson.spaces2)
        case other => ko(s"[$other] is not a SchemaViolations bad row")
      }
    }

    "criterion mismatch" >> {
      val unstruct =
        """
        {
          "schema":"iglu:com.snowplowanalytics.snowplow/my_unstruct_event/jsonschema/1-0-0",
          "data":{
            "foo":"bar"
          }
        }
        """
      enrich(unstruct).map {
        case sv: BadRow.SchemaViolations =>
          ko(sv.selfDescribingData.asJson.spaces2)
        case other => ko(s"[$other] is not a SchemaViolations bad row")
      }
    }

    "resolution error - schema could not be found in the specified repositories, defined by ResolutionError in the Iglu Client" >> {
      val unstruct =
        """
        {
          "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
          "data": {
            "schema":"iglu:com.acme/email_sent_foo/jsonschema/1-0-0",
            "data": {
              "emailAddress": "hello@world.com",
              "emailAddress2": "foo@bar.org"
            }
          }
        }
        """
      enrich(unstruct).map {
        case sv: BadRow.SchemaViolations =>
          ko(sv.selfDescribingData.asJson.spaces2)
        case other => ko(s"[$other] is not a SchemaViolations bad row")
      }
    }

    "validation error - Data is invalid against resolved schema" >> {
      val unstruct =
        """
        {
          "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
          "data":{
            "foo":"bar"
          }
        }
        """
      enrich(unstruct).map {
        case sv: BadRow.SchemaViolations =>
          ko(sv.selfDescribingData.asJson.spaces2)
        case other => ko(s"[$other] is not a SchemaViolations bad row")
      }
    }

    "validation error - Schema is invalid (empty required list) and cannot be used to validate an instance" >> {
      val unstruct =
        """
        {
          "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
          "data":{
            "schema":"iglu:com.acme/malformed_schema/jsonschema/1-0-0",
            "data": {
              "foo": "hello@world.com",
              "emailAddress2": "foo@bar.org"
            }
          }
        }
        """
      enrich(unstruct).map {
        case sv: BadRow.SchemaViolations =>
          ko(sv.selfDescribingData.asJson.spaces2)
        case other => ko(s"[$other] is not a SchemaViolations bad row")
      }
    }
  }
}

object IncompleteFailuresSpec {

  val enrichmentReg = EnrichmentRegistry[IO]()
  val client = SpecHelpers.client
  val processor = Processor("enrich", "0.0.0")
  val timestamp = DateTime.now()
  val api = CollectorPayload.Api("com.snowplowanalytics.snowplow", "tp2")
  val source = CollectorPayload.Source("scala-tracker", "UTF-8", None)
  val context = CollectorPayload.Context(
    DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
    "37.157.33.123".some,
    None,
    None,
    Nil,
    None
  )

  val atomicFieldLimits = AtomicFields.from(Map("v_tracker" -> 100))

  val parameters = Map(
    "e" -> "pp",
    "tv" -> "js-0.13.1",
    "p" -> "web"
  ).toOpt

  def enrich(unstruct: String): IO[BadRow] = {
    val allParams = parameters ++ Map("ue_pr" -> unstruct.some)
    val rawEvent = RawEvent(api, allParams, None, source, context)
    val enriched = EnrichmentManager.enrichEvent[IO](
      enrichmentReg,
      client,
      processor,
      timestamp,
      rawEvent,
      AcceptInvalid.featureFlags,
      IO.unit,
      SpecHelpers.registryLookup,
      atomicFieldLimits,
      SpecHelpers.emitIncomplete
    )
    enriched.value.flatMap {
      case Ior.Left(br) => IO.pure(br)
      case Ior.Right(_) => IO.raiseError(new IllegalStateException("Expected bad row but got enriched event"))
      case Ior.Both(_, _) => IO.raiseError(new IllegalStateException("Expected bad row but got Both"))
    }
  }
}
