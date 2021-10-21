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
package com.snowplowanalytics.snowplow.enrich.kinesis.it

import scala.concurrent.duration._

import java.nio.file.{Files, Path, Paths}
import java.io.File

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisClient
import software.amazon.awssdk.services.kinesis.model.{DeleteStreamRequest, CreateStreamRequest, CreateStreamResponse}
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest

import org.apache.commons.io.FileUtils

import cats.implicits._

import cats.effect.{Resource, Sync, Timer}

object Resources {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  val region = "eu-central-1"

  val collectorPayloadsStream = "it-enrich-kinesis-collector-payloads"
  val enrichedStream = "it-enrich-kinesis-enriched"
  val badRowsStream = "it-enrich-kinesis-bad"
  val streams = List(collectorPayloadsStream, enrichedStream, badRowsStream)

  val dynamoDbTableEnrich = "it-enrich-kinesis"
  val dynamoDbTableGood = "it-enrich-kinesis-good"
  val dynamoDbTableBad = "it-enrich-kinesis-bad"
  val tables = List(dynamoDbTableEnrich, dynamoDbTableGood, dynamoDbTableBad)

  val configPath = Files.createTempDirectory("enrich-kinesis-it")
  val configFile = "config.hocon"
  val configFilePath = Paths.get(configPath.toString, configFile)
  val resolverFile = "iglu_resolver.json"
  val resolverFilePath = Paths.get(configPath.toString, resolverFile)
  val enrichmentsDir = "enrichments"
  val enrichmentsPath = Paths.get(configPath.toString, enrichmentsDir)

  def init[F[_]: Sync: Timer]: Resource[F, Unit] = {

    def acquire(kinesisClient: KinesisClient, dynamoDbClient: DynamoDbClient): F[Unit] =
      for {
        _ <- Logger[F].info("Initializing AWS resources")
        _ <- deleteStreams(kinesisClient, streams)
        _ <- deleteTables(dynamoDbClient, tables)
        _ <- Timer[F].sleep(10.seconds)   // Let Kinesis settle
        _ <- createStreams(kinesisClient, streams)
        _ <- Timer[F].sleep(10.seconds)   // Let Kinesis settle
        _ <- writeFilesToDisk
      } yield ()

    def release(kinesisClient: KinesisClient, dynamoDbClient: DynamoDbClient): F[Unit] =
      deleteStreams(kinesisClient, streams) >>
        deleteTables(dynamoDbClient, tables)
        deleteFiles

    def mkResource(kinesisClient: KinesisClient, dynamoDbClient: DynamoDbClient) =
      Resource.make(acquire(kinesisClient, dynamoDbClient))(_ => release(kinesisClient, dynamoDbClient))

    val mkKinesisClient = Resource.fromAutoCloseable(Sync[F].pure(KinesisClient.builder.region(Region.of(region)).build()))
    val mkDynamoDbClient = Resource.fromAutoCloseable(Sync[F].pure(DynamoDbClient.builder.region(Region.of(region)).build()))
    (mkKinesisClient, mkDynamoDbClient).tupled.flatMap { case (kinesis, dynamoDb) => mkResource(kinesis, dynamoDb) }
  }

  def deleteStreams[F[_]: Sync](kinesisClient: KinesisClient, streams: List[String]): F[Unit] =
    streams.traverse_(deleteStream(kinesisClient, _))

  def deleteStream[F[_]: Sync](kinesisClient: KinesisClient, streamName: String): F[Unit] = {
    val deleteRequest = DeleteStreamRequest.builder.streamName(streamName).enforceConsumerDeletion(true).build
    val delete = Sync[F].delay(kinesisClient.deleteStream(deleteRequest)).void

    delete.recoverWith {
      case _: software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException =>
        Sync[F].unit
    }
  }

  def deleteTables[F[_]: Sync](dynamoDbClient: DynamoDbClient, tables: List[String]): F[Unit] =
    tables.traverse_(deleteTable(dynamoDbClient, _))

  def deleteTable[F[_]: Sync](dynamoDbClient: DynamoDbClient, tableName: String): F[Unit] = {
    val deleteRequest = DeleteTableRequest.builder.tableName(tableName).build
    val delete = Sync[F].delay(dynamoDbClient.deleteTable(deleteRequest)).void

    delete.recoverWith {
      case _: software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException =>
        Sync[F].unit
    }
  }

  def deleteFiles[F[_]: Sync]: F[Unit] =
    Sync[F].delay(FileUtils.forceDelete(new File(configPath.toString))) 

  def deleteFileFromDisk[F[_]: Sync](path: Path): F[Unit] =
    Sync[F].delay(Files.deleteIfExists(path)).void

  def createStreams[F[_]: Sync](kinesisClient: KinesisClient, streams: List[String]): F[Unit] =
    streams.traverse_(createStream(kinesisClient, _))

  def createStream[F[_]: Sync](kinesisClient: KinesisClient, streamName: String): F[CreateStreamResponse] = {
    val createRequest = CreateStreamRequest.builder.streamName(streamName).shardCount(1).build
    Sync[F].delay(kinesisClient.createStream(createRequest))
  }

  def writeFilesToDisk[F[_]: Sync]: F[Unit] =
    writeFileToDisk(configFile, configFilePath) *>
      writeFileToDisk(resolverFile, resolverFilePath) *>
      Sync[F].delay(new File(enrichmentsPath.toString).mkdirs()).void *>
      writeEnrichmentsJson

  def writeFileToDisk[F[_]: Sync](resourceName: String, dest: Path): F[Unit] = {
    val inputStream = getClass.getResourceAsStream("/" + resourceName)
    Sync[F].delay(Files.copy(inputStream, dest)).void
  }

  def writeEnrichmentsJson[F[_]: Sync]: F[Unit] =
    List(
      "yauaa_enrichment_config.json"
    )
      .map(f => writeFileToDisk(s"$enrichmentsDir/$f", Paths.get(enrichmentsPath.toString, f)))
      .sequence_
}
