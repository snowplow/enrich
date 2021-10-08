/*
 * Copyright (c) 2021-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.kinesis

import cats.effect.{Blocker, ContextShift, Sync}

import cats.implicits._

import io.circe.parser._
import io.circe.syntax._
import io.circe.Json

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.collection.JavaConverters._

import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.{Base64Json, CliConfig, EncodedOrPath}

object DynamoDbConfig {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  private val dynamoDbRegex = "^dynamodb:([^/]*)/([^/]*)/([^/]*)$".r

  private val enrichmentSchema = SchemaKey(
    "com.snowplowanalytics.snowplow",
    "enrichments",
    "jsonschema",
    SchemaVer.Full(1, 0, 0)
  )

  /**
   * Retrieves JSONs from DynamoDB if cli arguments start with dynamodb:
   *  and passes them as base64 encoded JSONs
   */
  def updateCliConfig[F[_]: ContextShift: Sync](blocker: Blocker, original: CliConfig): F[CliConfig] =
    for {
      resolver <- updateArg[F](blocker, original.resolver, getResolver[F])
      enrichments <- updateArg[F](blocker, original.enrichments, getEnrichments[F])
      updated = original.copy(resolver = resolver, enrichments = enrichments)
    } yield updated

  private def updateArg[F[_]: ContextShift: Sync](
    blocker: Blocker,
    orig: EncodedOrPath,
    getConfig: (Blocker, String, String, String) => F[Base64Json]
  ): F[EncodedOrPath] =
    orig match {
      case Left(encoded) => Sync[F].pure(Left(encoded))
      case Right(path) =>
        path.toString match {
          case dynamoDbRegex(region, table, key) => getConfig(blocker, region, table, key).map(Left(_))
          case _ => Sync[F].pure(Right(path))
        }
    }

  private def getResolver[F[_]: ContextShift: Sync](
    blocker: Blocker,
    region: String,
    table: String,
    key: String
  ): F[Base64Json] = {
    val dynamoDBClient = mkClient(region)
    val dynamoDB = new DynamoDB(dynamoDBClient)
    for {
      _ <- unsafeLogger.info(s"Retrieving resolver in DynamoDB $region/$table/$key")
      item <- blocker.blockOn(Sync[F].delay(dynamoDB.getTable(table).getItem("id", key)))
      jsonStr <- Option(item).flatMap(i => Option(i.getString("json"))) match {
                   case Some(content) =>
                     Sync[F].pure(content)
                   case None =>
                     Sync[F].raiseError[String](new RuntimeException(s"Can't retrieve resolver in DynamoDB at $region/$table/$key"))
                 }
      json <- parse(jsonStr) match {
                case Right(parsed) =>
                  Sync[F].pure(parsed)
                case Left(err) =>
                  Sync[F].raiseError[Json](new RuntimeException(s"Can't parse resolver. Error: $err"))
              }
      encoded = Base64Json(json)
    } yield encoded
  }

  private def getEnrichments[F[_]: ContextShift: Sync](
    blocker: Blocker,
    region: String,
    table: String,
    prefix: String
  ): F[Base64Json] = {
    val dynamoDBClient = mkClient(region)
    // Assumes that the table is small and contains only the config
    val scanRequest = new ScanRequest().withTableName(table)
    for {
      _ <- unsafeLogger.info(s"Retrieving enrichments in DynamoDB $region/$table/$prefix*")
      scanned <- blocker.blockOn(Sync[F].delay(dynamoDBClient.scan(scanRequest)))
      values = scanned.getItems().asScala.collect {
                 case map if Option(map.get("id")).exists(_.getS.startsWith(prefix)) && map.containsKey("json") =>
                   map.get("json").getS()
               }
      jsons <- values.map(parse).toList.sequence match {
                 case Left(decodingFailure) =>
                   val msg = s"An error occured while parsing an enrichment config: ${decodingFailure.message}"
                   Sync[F].raiseError(new RuntimeException(msg))
                 case Right(parsed) =>
                   Sync[F].pure(parsed)
               }
      sdj = SelfDescribingData[Json](enrichmentSchema, Json.arr(jsons: _*))
      encoded = Base64Json(sdj.asJson)
    } yield encoded
  }

  private def mkClient(region: String) =
    AmazonDynamoDBClientBuilder
      .standard()
      .withRegion(region)
      .build()
}
