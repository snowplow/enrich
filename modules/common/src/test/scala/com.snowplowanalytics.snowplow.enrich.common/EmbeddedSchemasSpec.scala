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
package com.snowplowanalytics.snowplow.enrich.common

import java.util.Base64

import cats.syntax.validated._

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect

import io.circe.Json
import io.circe.literal._

import org.joda.time.DateTime

import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.Registry

import com.snowplowanalytics.snowplow.badrows.Processor

import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.RemoteAdapter
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{AtomicFields, EnrichmentRegistry}
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.OptionIor

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class EmbeddedSchemasSpec extends Specification with ValidatedMatchers with CatsEffect {
  def is = s2"""
  An unstruct event with contexts should be enriched using only embedded schemas  $e1
  """

  val adapterRegistry = new AdapterRegistry[IO](
    Map.empty[(String, String), RemoteAdapter[IO]],
    adaptersSchemas = adaptersSchemas
  )
  val enrichmentReg = EnrichmentRegistry[IO]()
  val processor = Processor("sce-test-suite", "1.0.0")
  val dateTime = DateTime.now()

  def embeddedOnlyClient: IO[IgluCirceClient[IO]] =
    IgluCirceClient.fromResolver[IO](
      Resolver[IO](List(Registry.EmbeddedRegistry), None),
      cacheSize = 0,
      maxJsonDepth = 40
    )

  def e1 =
    for {
      client <- embeddedOnlyClient
      payload = EmbeddedSchemasSpec.buildPayload()
      output <- EtlPipeline
                  .processEvents[IO](
                    adapterRegistry,
                    enrichmentReg,
                    client,
                    processor,
                    dateTime,
                    payload.validNel,
                    AcceptInvalid.featureFlags,
                    IO.unit,
                    SpecHelpers.registryLookup,
                    AtomicFields.from(Map.empty),
                    emitFailed,
                    SpecHelpers.DefaultMaxJsonDepth
                  )
    } yield output must be like {
      case OptionIor.Right(_) :: Nil => ok
      case other => ko(s"Expected 1 enriched event but got: $other")
    }
}

object EmbeddedSchemasSpec {

  private implicit class JsonOps(val json: Json) extends AnyVal {
    def toBase64: String =
      Base64.getEncoder.encodeToString(json.noSpaces.getBytes("UTF-8"))
  }

  private val anonIpData = json"""{
    "vendor": "com.snowplowanalytics.snowplow",
    "name": "anon_ip",
    "enabled": true,
    "parameters": { "anonOctets": 1 }
  }"""

  private val unstructEventJson = json"""{
    "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
    "data": {
      "schema": "iglu:com.snowplowanalytics.snowplow/anon_ip/jsonschema/1-0-0",
      "data": $anonIpData
    }
  }"""

  private val contextsJson = json"""{
    "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
    "data": [{
      "schema": "iglu:com.snowplowanalytics.snowplow/anon_ip/jsonschema/1-0-0",
      "data": $anonIpData
    }]
  }"""

  /**
   * Builds a POST payload containing a single unstruct event with a context entity.
   * All schemas used here must be available in the embedded registry:
   * - iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4 (POST body wrapper)
   * - iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0 (unstruct event wrapper)
   * - iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0 (context wrapper)
   * - iglu:com.snowplowanalytics.snowplow/anon_ip/jsonschema/1-0-0 (unstruct event data and context entity)
   */
  def buildPayload(): CollectorPayload = {
    val collectorContext =
      CollectorPayload.Context(
        DateTime.parse("2017-07-14T03:39:39.000+00:00"),
        Some("127.0.0.1"),
        None,
        None,
        Nil,
        None
      )
    val source = CollectorPayload.Source("clj-tomcat", "UTF-8", None)

    val uePx = unstructEventJson.toBase64
    val cx = contextsJson.toBase64

    CollectorPayload(
      CollectorPayload.Api("com.snowplowanalytics.snowplow", "tp2"),
      Nil,
      Some("application/json"),
      Some(
        s"""{
           |"schema":"iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4",
           |"data":[
           |  {"e":"ue","ue_px":"$uePx","tv":"js-2.10.2","tna":"test","aid":"test-app","p":"web","eid":"1a950884-61d4-4179-a89a-43b67fb58a8f","dtm":"1581382581877","cx":"$cx","stm":"1581382583373"}
           |]}""".stripMargin
      ),
      source,
      collectorContext
    )
  }
}
