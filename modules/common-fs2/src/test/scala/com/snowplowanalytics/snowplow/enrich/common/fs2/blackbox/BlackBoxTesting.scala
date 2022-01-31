/*
 * Copyright (c) 2022-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox

import scala.collection.JavaConverters._

import org.specs2.mutable.Specification

import cats.effect.IO

import cats.effect.testing.specs2.CatsIO

import cats.data.Validated
import cats.data.Validated.{Invalid, Valid}

import io.circe.Json
import io.circe.literal._
import io.circe.syntax._

import org.apache.thrift.TSerializer

import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload

import com.snowplowanalytics.iglu.client.{CirceValidator, Client, Resolver}
import com.snowplowanalytics.iglu.client.resolver.registries.Registry

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.enrich.common.utils.BlockerF
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry

import com.snowplowanalytics.snowplow.enrich.common.fs2.Enrich

import com.snowplowanalytics.snowplow.enrich.common.fs2.EnrichSpec

object BlackBoxTesting extends Specification with CatsIO {

  val igluClient: Client[IO, Json] =
    Client[IO, Json](Resolver(List(Registry.EmbeddedRegistry), None), CirceValidator)

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
    Enrich
      .enrichWith(getEnrichmentRegistry(enrichmentConfig), igluClient, None, EnrichSpec.processor, false, IO.unit)(
        input
      )
      .map {
        case (List(Validated.Valid(enriched)), _) => checkEnriched(enriched, expected)
        case other => ko(s"there should be one enriched event but got $other")
      }

  private def checkEnriched(enriched: EnrichedEvent, expectedFields: Map[String, String]) = {
    val asMap = getMap(enriched)
    val r = expectedFields.map { case (k, v) => asMap.get(k) must beSome(v) }
    r.toList.reduce(_ and _)
  }

  private val enrichedFields = classOf[EnrichedEvent].getDeclaredFields()
  enrichedFields.foreach(_.setAccessible(true))

  private def getMap(enriched: EnrichedEvent): Map[String, String] =
    enrichedFields.map(f => (f.getName(), Option(f.get(enriched)).map(_.toString).getOrElse(""))).toMap

  private def getEnrichmentRegistry(enrichmentConfig: Option[Json]): IO[EnrichmentRegistry[IO]] =
    enrichmentConfig match {
      case None =>
        IO.pure(EnrichmentRegistry[IO]())
      case Some(json) =>
        val enrichmentsSchemaKey =
          SchemaKey("com.snowplowanalytics.snowplow", "enrichments", "jsonschema", SchemaVer.Full(1, 0, 0))
        val enrichmentsJson = SelfDescribingData(enrichmentsSchemaKey, Json.arr(json)).asJson

        for {
          parsed <- EnrichmentRegistry.parse[IO](enrichmentsJson, igluClient, true)
          confs <- parsed match {
                     case Invalid(e) => IO.raiseError(new IllegalArgumentException(s"can't parse enrichmentsJson: $e"))
                     case Valid(list) => IO.pure(list)
                   }
          built <- EnrichmentRegistry.build[IO](confs, BlockerF.noop).value
          registry <- built match {
                        case Left(e) => IO.raiseError(new IllegalArgumentException(s"can't build EnrichmentRegistry: $e"))
                        case Right(r) => IO.pure(r)
                      }
        } yield registry
    }
}
