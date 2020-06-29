/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.stream

import java.io.File
import java.net.URI

import scala.jdk.CollectionConverters._
import scala.io.Source
import cats.Id
import cats.implicits._
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeValue, GetItemRequest, ScanRequest}
import software.amazon.awssdk.regions.Region
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonUtils
import com.snowplowanalytics.snowplow.scalatracker.Tracker
import io.circe.Json
import io.circe.syntax._
import config._
import model.{Credentials, DualCloudCredentialsPair, Kinesis, NoCredentials, SentryConfig, StreamsConfig}
import sources.KinesisSource
import utils.getAwsCredentialsProvider

/** The main entry point for Stream Enrich for Kinesis. */
object KinesisEnrich extends Enrich {

  val DynamoDBRegex = "^dynamodb:([^/]*)/([^/]*)/([^/]*)$".r
  private val regexMsg = "'file:[filename]' or 'dynamodb:[region/table/key]'"

  def main(args: Array[String]): Unit = {
    val trackerSource = for {
      config <- parseConfig(args)
      (enrichConfig, resolverArg, enrichmentsArg, forceDownload) = config
      credsWithRegion <- enrichConfig.streams.sourceSink match {
                           case k: Kinesis =>
                             (
                               DualCloudCredentialsPair(k.aws, k.gcp.fold[Credentials](NoCredentials)(identity)),
                               Region.of(k.region)
                             ).asRight
                           case _ => "Configured source/sink is not Kinesis".asLeft
                         }
      (credentials, awsRegion) = credsWithRegion
      client <- parseClient(resolverArg)(credsWithRegion._1.aws)
      enrichmentsConf <- parseEnrichmentRegistry(enrichmentsArg, client)(credsWithRegion._1.aws)
      _ <- cacheFiles(
             enrichmentsConf,
             forceDownload,
             credentials.aws,
             credentials.gcp,
             Option(awsRegion)
           )
      enrichmentRegistry <- EnrichmentRegistry.build[Id](enrichmentsConf).value
      tracker = enrichConfig.monitoring.map(c => SnowplowTracking.initializeTracker(c.snowplow))
      adapterRegistry = new AdapterRegistry(prepareRemoteAdapters(enrichConfig.remoteAdapters))
      processor = Processor(generated.BuildInfo.name, generated.BuildInfo.version)
      source <- getSource(
                  enrichConfig.streams,
                  enrichConfig.sentry,
                  client,
                  adapterRegistry,
                  enrichmentRegistry,
                  tracker,
                  processor
                )
    } yield (tracker, source)

    trackerSource match {
      case Left(e) =>
        System.err.println(e)
        System.exit(1)
      case Right((tracker, source)) =>
        tracker.foreach(SnowplowTracking.initializeSnowplowTracking)
        source.run()
    }
  }

  override def getSource(
    streamsConfig: StreamsConfig,
    sentryConfig: Option[SentryConfig],
    client: Client[Id, Json],
    adapterRegistry: AdapterRegistry,
    enrichmentRegistry: EnrichmentRegistry[Id],
    tracker: Option[Tracker[Id]],
    processor: Processor
  ): Either[String, sources.Source] =
    KinesisSource.createAndInitialize(
      streamsConfig,
      sentryConfig,
      client,
      adapterRegistry,
      enrichmentRegistry,
      tracker,
      processor
    )

  override lazy val parser: scopt.OptionParser[FileConfig] =
    new scopt.OptionParser[FileConfig](generated.BuildInfo.name) with FileConfigOptions {
      head(generated.BuildInfo.name, generated.BuildInfo.version)
      help("help")
      version("version")
      configOption()
      opt[String]("resolver")
        .required()
        .valueName("<resolver uri>")
        .text(s"Iglu resolver file, $regexMsg")
        .action((r: String, c: FileConfig) => c.copy(resolver = r))
        .validate(_ match {
          case FilepathRegex(_) | DynamoDBRegex(_, _, _) => success
          case _ => failure(s"Resolver doesn't match accepted uris: $regexMsg")
        })
      opt[String]("enrichments")
        .optional()
        .valueName("<enrichment directory uri>")
        .text(s"Directory of enrichment configuration JSONs, $regexMsg")
        .action((e: String, c: FileConfig) => c.copy(enrichmentsDir = Some(e)))
        .validate(_ match {
          case FilepathRegex(_) | DynamoDBRegex(_, _, _) => success
          case _ => failure(s"Enrichments directory doesn't match accepted uris: $regexMsg")
        })
      forceCachedFilesDownloadOption()
    }

  override def extractResolver(resolverArgument: String)(implicit creds: Credentials): Either[String, String] =
    resolverArgument match {
      case FilepathRegex(filepath) =>
        val file = new File(filepath)
        if (file.exists) Source.fromFile(file).mkString.asRight
        else "Iglu resolver configuration file \"%s\" does not exist".format(filepath).asLeft
      case DynamoDBRegex(region, table, key) =>
        for {
          provider <- getAwsCredentialsProvider(creds)
          resolver <- lookupDynamoDBResolver(provider, Region.of(region), table, key)
        } yield resolver
      case _ => s"Resolver argument [$resolverArgument] must match $regexMsg".asLeft
    }

  /**
   * Fetch configuration from DynamoDB, assumes the primary key is "id" and the configuration key is
   * "json"
   * @param provider aws credentials provider
   * @param region DynamoDB region, e.g. "eu-west-1"
   * @param table DynamoDB table containing the resolver
   * @param key The value of the primary key for the configuration
   * @return The JSON stored in DynamoDB
   */
  private def lookupDynamoDBResolver(
    provider: AwsCredentialsProvider,
    region: Region,
    table: String,
    key: String
  ): Either[String, String] = {
    val dynamoDBClient = DynamoDbClient.builder
      .credentialsProvider(provider)
      .endpointOverride(getDynamodbEndpoint(region))
      .region(region)
      .build
    val request = GetItemRequest.builder.tableName(table).key(Map("id" -> AttributeValue.builder.s(key).build).asJava).build
    for {
      response <- Either.catchNonFatal(dynamoDBClient.getItem(request)).leftMap(_.getMessage)
      field <- response.item.asScala.get("json").toRight(s"""Field "json" not found at key $key in DynamoDB table $table""")
    } yield field.s
  }

  override def extractEnrichmentConfigs(enrichmentArg: Option[String])(implicit creds: Credentials): Either[String, Json] = {
    val jsons: Either[String, List[String]] = enrichmentArg
      .map {
        case FilepathRegex(dir) =>
          new File(dir).listFiles
            .filter(_.getName.endsWith(".json"))
            .map(scala.io.Source.fromFile(_).mkString)
            .toList
            .asRight
        case DynamoDBRegex(region, table, keyNamePrefix) =>
          for {
            provider <- getAwsCredentialsProvider(creds)
            enrichmentList = lookupDynamoDBEnrichments(provider, Region.of(region), table, keyNamePrefix)
            enrichments <- enrichmentList match {
                             case Nil => s"No enrichments found with prefix $keyNamePrefix".asLeft
                             case js => js.asRight
                           }
          } yield enrichments
        case other => s"Enrichments argument [$other] must match $regexMsg".asLeft
      }
      .getOrElse(Nil.asRight)

    val schemaKey = SchemaKey(
      "com.snowplowanalytics.snowplow",
      "enrichments",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )

    jsons
      .flatMap(_.map(JsonUtils.extractJson).sequence[EitherS, Json])
      .map(jsons => SelfDescribingData[Json](schemaKey, Json.fromValues(jsons)).asJson)
  }

  /**
   * Get a list of enrichment JSONs from DynamoDB
   * @param provider aws credentials provider
   * @param region DynamoDB region, e.g. "eu-west-1"
   * @param table
   * @param keyNamePrefix Primary key prefix, e.g. "enrichments-"
   * @return List of JSONs
   */
  private def lookupDynamoDBEnrichments(
    provider: AwsCredentialsProvider,
    region: Region,
    table: String,
    keyNamePrefix: String
  ): List[String] = {
    val dynamoDBClient = DynamoDbClient.builder
      .credentialsProvider(provider)
      .endpointOverride(getDynamodbEndpoint(region))
      .region(region)
      .build

    val scanRequest = ScanRequest.builder.tableName(table).build
    val allItems = dynamoDBClient
      .scanPaginator(scanRequest)
      .asScala
      .flatMap { response =>
        response.items.asScala.map(_.asScala.toMap.mapValues(_.s))
      }
      .toList

    allItems
      .filter { item =>
        item.get("id") match {
          case Some(value) if value.startsWith(keyNamePrefix) => true
          case _ => false
        }
      }
      .flatMap(_.get("json"))
  }

  private def getDynamodbEndpoint(region: Region): URI =
    region match {
      case cn @ Region.CN_NORTH_1 => new URI(s"https://dynamodb.$cn.amazonaws.com.cn")
      case _ => new URI(s"https://dynamodb.$region.amazonaws.com")
    }
}
