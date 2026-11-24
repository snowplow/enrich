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
package com.snowplowanalytics.snowplow.enrich.common.adapters

import cats.data.NonEmptyList
import cats.syntax.option._

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect

import org.joda.time.DateTime

import org.specs2.Specification

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers

class AdapterRegistrySpec extends Specification with CatsEffect {
  def is = s2"""
  toRawEvents should return an AdapterFailures bad row for an unsupported vendor/version $e1
  """

  val adapterRegistry = new AdapterRegistry[IO](SpecHelpers.adaptersSchemas)
  val processor = Processor("sce-test-suite", "1.0.0")

  def e1 = {
    val api = CollectorPayload.Api("com.acme", "v1")
    val source = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
    val context = CollectorPayload.Context(
      DateTime.parse("2013-08-29T00:18:48.000+00:00"),
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    )
    val payload = SpecHelpers.collectorPayload(api, Nil, None, None, source, context)

    val expected = BadRow.AdapterFailures(
      processor,
      Failure.AdapterFailures(
        SpecHelpers.etlTstamp,
        "com.acme",
        "v1",
        NonEmptyList.one(
          FailureDetails.AdapterFailure.InputData(
            "vendor/version",
            Some("com.acme/v1"),
            "vendor/version combination is not supported"
          )
        )
      ),
      payload.toBadRowPayload
    )

    adapterRegistry
      .toRawEvents(
        payload,
        SpecHelpers.client,
        processor,
        SpecHelpers.registryLookup,
        SpecHelpers.DefaultMaxJsonDepth,
        SpecHelpers.etlTstamp
      )
      .map(_.toEither must beLeft(expected))
  }
}
