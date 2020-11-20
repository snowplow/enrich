/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics
package snowplow
package enrich
package beam

import java.io.File
import java.net.URI

import scala.io.Source

import cats.Id
import cats.data.ValidatedNel
import cats.implicits._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonUtils
import com.spotify.scio.Args
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax._

import utils._

object config {
  private type EitherS[A] = Either[String, A]
  private type ValidatedNelS[A] = ValidatedNel[String, A]

  /** Case class holding the raw job configuration */
  final case class EnrichConfig(
    jobName: String,
    raw: String,
    enriched: String,
    bad: String,
    pii: Option[String],
    resolver: Json,
    enrichmentConfs: List[EnrichmentConf],
    labels: Map[String, String],
    sentryDSN: Option[URI],
    metrics: Boolean
  )
  object EnrichConfig {

    /** Smart constructor taking SCIO's [[Args]] */
    def apply(args: Args): Either[String, EnrichConfig] =
      for {
        _ <- if (args.optional("help").isDefined) helpString(configurations).asLeft else "".asRight
        l <- configurations
               .collect {
                 case Configuration.Required(key, _) =>
                   args.optional(key).toValidNel(s"Missing `$key` argument")
               }
               .sequence[ValidatedNelS, String]
               .leftMap(_.toList.mkString("\n"))
               .toEither
        List(jobName, raw, enriched, bad, resolver) = l
        pii = args.optional("pii")
        enrichments = args.optional("enrichments")
        labels = args.optional("labels")
        sentryDsn = args.asMap.get("sentry-dsn").map(_.mkString(",")) // see #391
        metrics = args.boolean("metrics", true)
        resolverJson <- parseResolver(resolver)
        client <- Client.parseDefault[Id](resolverJson).leftMap(_.toString).value
        registryJson <- parseEnrichmentRegistry(enrichments, client)
        confs <- EnrichmentRegistry.parse(registryJson, client, false).leftMap(_.toString).toEither
        labels <- labels.map(parseLabels).getOrElse(Right(Map.empty[String, String]))
        sentryDSN <- parseSentryDsn(sentryDsn)
        _ <- if (emitPii(confs) && pii.isEmpty)
               "A pii topic needs to be used in order to use the pii enrichment".asLeft
             else
               ().asRight
      } yield EnrichConfig(
        jobName,
        raw,
        enriched,
        bad,
        pii,
        resolverJson,
        confs,
        labels,
        sentryDSN,
        metrics
      )

    private val configurations = List(
      Configuration.Required("job-name", "Name of the Dataflow job that will be launched"),
      Configuration.Required(
        "raw",
        "Name of the subscription to the raw topic projects/{project}/subscriptions/{subscription}"
      ),
      Configuration.Required(
        "enriched",
        "Name of the enriched topic projects/{project}/topics/{topic}"
      ),
      Configuration.Required("bad", "Name of the bad topic projects/{project}/topics/{topic}"),
      Configuration.Optional("pii", "Name of the pii topic projects/{project}/topics/{topic}"),
      Configuration.Required("resolver", "Path to the resolver file"),
      Configuration.Optional("enrichments", "Path to the directory containing the enrichment files"),
      Configuration.Optional("labels", "Dataflow labels to be set ie. env=qa1;region=eu"),
      Configuration.Optional("sentry-dsn", "Sentry DSN"),
      Configuration.Optional("metrics", "Enable ScioMetrics (default: true)")
    )

    /** Generates an help string from a list of conifugration */
    private def helpString(configs: List[Configuration]): String =
      "Possible configuration are:\n" +
        configs
          .map {
            case Configuration.Optional(key, desc) => s"--$key=VALUE, optional, $desc"
            case Configuration.Required(key, desc) => s"--$key=VALUE, required, $desc"
          }
          .mkString("\n") +
        "\n--help, Display this message" +
        "\nA full list of all the Beam CLI options can be found at: https://cloud.google.com/dataflow/pipelines/specifying-exec-params#setting-other-cloud-pipeline-options"
  }

  /** ADT for configuration parameters */
  sealed trait Configuration {
    def key: String
    def desc: String
  }
  object Configuration {
    final case class Optional(key: String, desc: String) extends Configuration
    final case class Required(key: String, desc: String) extends Configuration

  }

  /**
   * Parses a resolver at the specified path.
   * @param resolverPath path where the resolver is located
   * @return the parsed JValue if the parsing was successful
   */
  def parseResolver(resolverPath: String): Either[String, Json] =
    for {
      fileContent <- readResolverFile(resolverPath)
      json <- JsonUtils.extractJson(fileContent)
      _ <- Client.parseDefault[Id](json).leftMap(_.message).value
    } yield json

  /** Reads a resolver file at the specfied path. */
  private def readResolverFile(path: String): Either[String, String] = {
    val file = new File(path)
    if (file.exists) Source.fromFile(file).mkString.asRight
    else s"Iglu resolver configuration file `$path` does not exist".asLeft
  }

  /**
   * Parses an enrichment registry at the specified path.
   * @param enrichmentsPath path where the enrichment directory is located
   * @return the enrichment registry built from the enrichments found
   */
  def parseEnrichmentRegistry(enrichmentsPath: Option[String], client: Client[Id, Json]): Either[String, Json] =
    for {
      fileContents <- readEnrichmentFiles(enrichmentsPath)
      jsons <- fileContents.map(JsonUtils.extractJson(_)).sequence[EitherS, Json]
      schemaKey = SchemaKey(
                    "com.snowplowanalytics.snowplow",
                    "enrichments",
                    "jsonschema",
                    SchemaVer.Full(1, 0, 0)
                  )
      enrichmentsJson = SelfDescribingData[Json](schemaKey, Json.fromValues(jsons)).asJson
      _ <- EnrichmentRegistry
             .parse(enrichmentsJson, client, false)
             .leftMap(_.toList.mkString("\n"))
             .toEither
    } yield enrichmentsJson

  /** Reads all the enrichment files contained in a directory at the specified path. */
  private def readEnrichmentFiles(path: Option[String]): Either[String, List[String]] =
    path
      .map { p =>
        for {
          files <- Option(new File(p).listFiles)
                     .toRight(s"Enrichment directory `$p` does not exist")
          read = files
                   .filter(_.getName.endsWith(".json"))
                   .map(Source.fromFile(_).mkString)
                   .toList
        } yield read
      }
      .getOrElse(Nil.asRight)

  def parseLabels(input: String): Either[String, Map[String, String]] =
    decode[Map[String, String]](input)
      .leftMap(_ => s"Invalid `labels` format, expected json object, received: $input")

  /** Create a java.net.URI from string with Sentry DSN. */
  private def parseSentryDsn(maybeDSN: Option[String]): Either[String, Option[URI]] =
    maybeDSN match {
      case Some(uri) if uri.startsWith("http://") || uri.startsWith("https://") =>
        Either
          .catchNonFatal(URI.create(uri))
          .leftMap(e => s"Could not parse Sentry DSN as URI. Error: [${e.getMessage}]")
          .map(Some(_))
      case Some(uri) =>
        Left(s"Sentry DSN [$uri] doesn't start with http:// or https://")
      case _ => None.asRight
    }
}
