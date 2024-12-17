/*
 * Copyright (c) 2021-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.kinesis

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationLong
import scala.util.control.NonFatal

import cats.effect.kernel.{Async, Resource, Sync}

import cats.Applicative
import cats.implicits._

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}

import retry.{RetryDetails, RetryPolicies, RetryPolicy, retryingOnSomeErrors}

import com.amazonaws.AmazonClientException
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.{Base64Hocon, CliConfig, EncodedHoconOrPath}

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
  def updateCliConfig[F[_]: Async](original: CliConfig): F[CliConfig] =
    for {
      resolver <- updateArg[F](original.resolver, getResolver[F])
      enrichments <- updateArg[F](original.enrichments, getEnrichments[F])
      updated = original.copy(resolver = resolver, enrichments = enrichments)
    } yield updated

  private def updateArg[F[_]: Sync](
    orig: EncodedHoconOrPath,
    getConfig: (String, String, String) => F[Base64Hocon]
  ): F[EncodedHoconOrPath] =
    orig match {
      case Left(encoded) => Sync[F].pure(Left(encoded))
      case Right(path) =>
        path.toString match {
          case dynamoDbRegex(region, table, key) => getConfig(region, table, key).map(Left(_))
          case _ => Sync[F].pure(Right(path))
        }
    }

  private def getResolver[F[_]: Async](
    region: String,
    table: String,
    key: String
  ): F[Base64Hocon] = {
    val dynamoDBResource = for {
      client <- mkClient[F](region)
      api <- Resource.make(Sync[F].delay(new DynamoDB(client)))(a => Sync[F].delay(a.shutdown()))
    } yield api
    dynamoDBResource.use { dynamoDB =>
      for {
        _ <- unsafeLogger.info(s"Retrieving resolver in DynamoDB $region/$table/$key")
        item <- withRetry(Sync[F].blocking(dynamoDB.getTable(table).getItem("id", key)))
        jsonStr <- Option(item).flatMap(i => Option(i.getString("json"))) match {
                     case Some(content) =>
                       Sync[F].pure(content)
                     case None =>
                       Sync[F].raiseError[String](new RuntimeException(s"Can't retrieve resolver in DynamoDB at $region/$table/$key"))
                   }
        tsConfig <- Sync[F].delay(ConfigFactory.parseString(jsonStr)).adaptError {
                      case e => new RuntimeException("Cannot parse resolver from dynamodb", e)
                    }
      } yield Base64Hocon(tsConfig)
    }
  }

  private def getEnrichments[F[_]: Async](
    region: String,
    table: String,
    prefix: String
  ): F[Base64Hocon] =
    mkClient[F](region).use { dynamoDBClient =>
      // Assumes that the table is small and contains only the config
      val scanRequest = new ScanRequest().withTableName(table)
      for {
        _ <- unsafeLogger.info(s"Retrieving enrichments in DynamoDB $region/$table/$prefix*")
        scanned <- withRetry(Sync[F].blocking(dynamoDBClient.scan(scanRequest)))
        values = scanned.getItems().asScala.collect {
                   case map if Option(map.get("id")).exists(_.getS.startsWith(prefix)) && map.containsKey("json") =>
                     map.get("json").getS()
                 }
        hocons <- values.toList
                    .traverse { jsonStr =>
                      Sync[F].delay(ConfigFactory.parseString(jsonStr))
                    }
                    .adaptError {
                      case e => new RuntimeException("Cannot parse enrichment config from dynamodb", e)
                    }
      } yield {
        val tsConfig = ConfigFactory
          .empty()
          .withValue("schema", ConfigValueFactory.fromAnyRef(enrichmentSchema.toSchemaUri))
          .withValue("data", ConfigValueFactory.fromIterable(hocons.map(_.root).asJava))
        Base64Hocon(tsConfig)
      }
    }

  private def mkClient[F[_]: Sync](region: String): Resource[F, AmazonDynamoDB] =
    Resource.make(Sync[F].delay {
      AmazonDynamoDBClientBuilder
        .standard()
        .withRegion(region)
        .build()
    })(c => Sync[F].delay(c.shutdown))

  private def withRetry[F[_]: Async, A](f: F[A]): F[A] =
    retryingOnSomeErrors[A](retryPolicy[F], worthRetrying[F], onError[F])(f)

  private def retryPolicy[F[_]: Applicative]: RetryPolicy[F] =
    RetryPolicies.fullJitter[F](1500.milliseconds).join(RetryPolicies.limitRetries[F](5))

  private def worthRetrying[F[_]: Applicative](e: Throwable): F[Boolean] =
    e match {
      case ace: AmazonClientException if ace.isRetryable => Applicative[F].pure(true)
      case NonFatal(_) => Applicative[F].pure(false)
    }

  private def onError[F[_]: Sync](error: Throwable, details: RetryDetails): F[Unit] =
    if (details.givingUp)
      Logger[F].error(show"Failed to query dynamodb after ${details.retriesSoFar} retries. ${error.getMessage}. Aborting the job")
    else
      Logger[F].warn(
        show"Failed to query dynamodb with ${details.retriesSoFar} retries so far, " +
          show"waiting for ${details.cumulativeDelay.toMillis} ms. ${error.getMessage}. " +
          show"Keep retrying"
      )
}
