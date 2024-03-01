/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox

import scala.collection.JavaConverters._

import org.specs2.mutable.Specification

import cats.effect.IO
import cats.effect.kernel.Resource

import cats.effect.testing.specs2.CatsEffect

import cats.data.Validated
import cats.data.Validated.{Invalid, Valid}

import io.circe.Json
import io.circe.syntax._
import io.circe.parser.{parse => jparse}

import org.apache.thrift.TSerializer

import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.Registry

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.enrich.common.utils.{HttpClient, ShiftExecution}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry

import com.snowplowanalytics.snowplow.enrich.common.fs2.Enrich
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.FeatureFlags
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Clients

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.fs2.EnrichSpec
import com.snowplowanalytics.snowplow.enrich.common.fs2.test.TestEnvironment
import com.snowplowanalytics.snowplow.enrich.common.enrichments.AtomicFields

object BlackBoxTesting extends Specification with CatsEffect {

  private val serializer: TSerializer = new TSerializer()

  def buildCollectorPayload(
    body: Option[String] = None,
    contentType: Option[String] = None,
    headers: List[String] = Nil,
    ipAddress: String = "",
    networkUserId: String = java.util.UUID.randomUUID().toString,
    path: String = "",
    querystring: Option[String] = None,
    refererUri: Option[String] = None,
    timestamp: Long = 0L,
    userAgent: Option[String] = None
  ): Array[Byte] = {
    val cp = new CollectorPayload(
      "iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0",
      ipAddress,
      timestamp,
      "UTF-8",
      "ssc"
    )
    cp.body = body.orNull
    cp.contentType = contentType.orNull
    cp.hostname = "hostname"
    cp.headers = headers.asJava
    cp.ipAddress = ipAddress
    cp.networkUserId = networkUserId
    cp.path = path
    cp.querystring = querystring.orNull
    cp.refererUri = refererUri.orNull
    cp.userAgent = userAgent.orNull
    serializer.serialize(cp)
  }

  def runTest(
    input: Array[Byte],
    expected: Map[String, String],
    enrichmentConfig: Option[Json] = None
  ) =
    SpecHelpers
      .createIgluClient(List(Registry.EmbeddedRegistry))
      .flatMap { igluClient =>
        getEnrichmentRegistry(enrichmentConfig, igluClient).use { registry =>
          Enrich
            .enrichWith(
              IO.pure(registry),
              TestEnvironment.adapterRegistry,
              igluClient,
              None,
              EnrichSpec.processor,
              featureFlags,
              IO.unit,
              SpecHelpers.registryLookup,
              AtomicFields.from(valueLimits = Map.empty)
            )(
              input
            )
            .map {
              case (List(Validated.Valid(enriched)), _) => checkEnriched(enriched, expected)
              case other => ko(s"there should be one enriched event but got $other")
            }
        }
      }

  private def checkEnriched(enriched: EnrichedEvent, expectedFields: Map[String, String]) = {
    val asMap = getMap(enriched)
    val r = expectedFields.map {
      case (k, v) if k == "unstruct_event" || k == "contexts" || k == "derived_contexts" =>
        compareJsons(asMap.getOrElse(k, ""), v) must beTrue
      case (k, v) =>
        asMap.get(k) must beSome(v)
    }
    r.toList.reduce(_ and _)
  }

  private def compareJsons(j1: String, j2: String): Boolean =
    j1 == j2 || jparse(j1).toOption.get == jparse(j2).toOption.get

  private val enrichedFields = classOf[EnrichedEvent].getDeclaredFields()
  enrichedFields.foreach(_.setAccessible(true))

  private def getMap(enriched: EnrichedEvent): Map[String, String] =
    enrichedFields.map(f => (f.getName(), Option(f.get(enriched)).map(_.toString).getOrElse(""))).toMap

  private def getEnrichmentRegistry(enrichmentConfig: Option[Json], igluClient: IgluCirceClient[IO]): Resource[IO, EnrichmentRegistry[IO]] =
    for {
      shift <- ShiftExecution.ofSingleThread[IO]
      http4s <- Clients.mkHttp[IO]()
      http = HttpClient.fromHttp4sClient[IO](http4s)
      registry = enrichmentConfig match {
                   case None =>
                     IO.pure(EnrichmentRegistry[IO]())
                   case Some(json) =>
                     val enrichmentsSchemaKey =
                       SchemaKey("com.snowplowanalytics.snowplow", "enrichments", "jsonschema", SchemaVer.Full(1, 0, 0))
                     val enrichmentsJson = SelfDescribingData(enrichmentsSchemaKey, Json.arr(json)).asJson
                     for {
                       parsed <- EnrichmentRegistry.parse[IO](enrichmentsJson, igluClient, true, SpecHelpers.registryLookup)
                       confs <- parsed match {
                                  case Invalid(e) => IO.raiseError(new IllegalArgumentException(s"can't parse enrichmentsJson: $e"))
                                  case Valid(list) => IO.pure(list)
                                }
                       built <- EnrichmentRegistry.build[IO](confs, shift, http, SpecHelpers.blockingEC).value
                       registry <- built match {
                                     case Left(e) => IO.raiseError(new IllegalArgumentException(s"can't build EnrichmentRegistry: $e"))
                                     case Right(r) => IO.pure(r)
                                   }
                     } yield registry
                 }
      resource <- Resource.eval(registry)
    } yield resource

  private val featureFlags = FeatureFlags(acceptInvalid = false, legacyEnrichmentOrder = false, tryBase64Decoding = false)
}
