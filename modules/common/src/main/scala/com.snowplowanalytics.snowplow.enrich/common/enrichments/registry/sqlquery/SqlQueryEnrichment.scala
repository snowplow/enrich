/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery

import scala.collection.immutable.IntMap

import java.sql.Connection
import javax.sql.DataSource

import org.slf4j.LoggerFactory

import cats.Monad
import cats.data.{EitherT, NonEmptyList, ValidatedNel}
import cats.implicits._

import io.circe._
import io.circe.generic.semiauto._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingData}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.{BlockerF, CirceUtils, ResourceF}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.SqlQueryConf
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.{Enrichment, ParseableEnrichment}

/** Lets us create an SqlQueryConf from a Json */
object SqlQueryEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "sql_query_enrichment_config",
      "jsonschema",
      1,
      0,
      0
    )

  /**
   * Creates an SqlQueryEnrichment instance from a Json.
   * @param c The enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return a SqlQueryEnrichment configuration
   */
  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, SqlQueryConf] =
    isParseable(c, schemaKey).toEitherNel.flatMap { _ =>
      // input ctor throws exception
      val inputs: ValidatedNel[String, List[Input]] = Either.catchNonFatal(
        CirceUtils.extract[List[Input]](c, "parameters", "inputs").toValidatedNel
      ) match {
        case Left(e) => e.getMessage.invalidNel
        case Right(r) => r
      }
      // output ctor throws exception
      val output: ValidatedNel[String, Output] = Either.catchNonFatal(
        CirceUtils.extract[Output](c, "parameters", "output").toValidatedNel
      ) match {
        case Left(e) => e.getMessage.invalidNel
        case Right(r) => r
      }
      (
        inputs,
        CirceUtils.extract[Rdbms](c, "parameters", "database").toValidatedNel,
        CirceUtils.extract[Query](c, "parameters", "query").toValidatedNel,
        output,
        CirceUtils.extract[Cache](c, "parameters", "cache").toValidatedNel
      ).mapN(SqlQueryConf(schemaKey, _, _, _, _, _)).toEither
    }.toValidated

  def apply[F[_]: CreateSqlQueryEnrichment](conf: SqlQueryConf, blocker: BlockerF[F]): F[SqlQueryEnrichment[F]] =
    CreateSqlQueryEnrichment[F].create(conf, blocker)

  /** Just a string with SQL, not escaped */
  final case class Query(sql: String) extends AnyVal

  /** Cache configuration */
  final case class Cache(size: Int, ttl: Int)

  implicit val queryCirceDecoder: Decoder[Query] =
    deriveDecoder[Query]

  implicit val cacheCirceDecoder: Decoder[Cache] =
    deriveDecoder[Cache]
}

/**
 * @param schemaKey configuration schema
 * @param inputs list of inputs, extracted from an original event
 * @param db source DB configuration
 * @param query string representation of prepared SQL statement
 * @param output configuration of output context
 * @param ttl cache TTL in milliseconds
 * @param cache actual mutable LRU cache
 * @param blocker Allows running blocking enrichments on a dedicated thread pool
 */
final case class SqlQueryEnrichment[F[_]: Monad: DbExecutor: ResourceF](
  schemaKey: SchemaKey,
  inputs: List[Input],
  db: Rdbms,
  query: SqlQueryEnrichment.Query,
  output: Output,
  ttl: Int,
  cache: SqlCache[F],
  blocker: BlockerF[F],
  dataSource: DataSource
) extends Enrichment {
  private val enrichmentInfo =
    FailureDetails.EnrichmentInformation(schemaKey, "sql-query").some

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Primary function of the enrichment. Failure means connection failure, failed unexpected
   * JSON-value, etc. Successful Nil skipped lookup (unfilled placeholder for eg, empty response)
   * @param event currently enriching event
   * @param derivedContexts derived contexts as list of JSON objects
   * @param customContexts custom contexts as SelfDescribingData
   * @param unstructEvent unstructured (self-describing) event as empty or single element
   * SelfDescribingData
   * @return Nil if some inputs were missing, validated JSON contexts if lookup performed
   */
  def lookup(
    event: EnrichedEvent,
    derivedContexts: List[SelfDescribingData[Json]],
    customContexts: List[SelfDescribingData[Json]],
    unstructEvent: Option[SelfDescribingData[Json]]
  ): F[ValidatedNel[FailureDetails.EnrichmentFailure, List[SelfDescribingData[Json]]]] = {
    val contexts = for {
      connection <- EitherT(DbExecutor.getConnection[F](dataSource))
                      .leftMap(t => NonEmptyList.of("Error while getting the connection from the data source", t.toString))
      result <- EitherT(ResourceF[F].use(connection)(closeConnection)(lookup(event, derivedContexts, customContexts, unstructEvent, _)))
    } yield result

    contexts.leftMap(failureDetails).value.map(_.toValidated)
  }

  private def lookup(
    event: EnrichedEvent,
    derivedContexts: List[SelfDescribingData[Json]],
    customContexts: List[SelfDescribingData[Json]],
    unstructEvent: Option[SelfDescribingData[Json]],
    connection: Connection
  ): F[Either[NonEmptyList[String], List[SelfDescribingData[Json]]]] =
    (for {
      placeholders <- buildPlaceHolderMap(inputs, event, derivedContexts, customContexts, unstructEvent)
      filledPlaceholders <- fillPlaceholders(connection, query.sql, placeholders)
      result <- getResult(filledPlaceholders, connection)
    } yield result).value

  private def buildPlaceHolderMap(
    inputs: List[Input],
    event: EnrichedEvent,
    derivedContexts: List[SelfDescribingData[Json]],
    customContexts: List[SelfDescribingData[Json]],
    unstructEvent: Option[SelfDescribingData[Json]]
  ): EitherT[F, NonEmptyList[String], Input.PlaceholderMap] =
    Input
      .buildPlaceholderMap(inputs, event, derivedContexts, customContexts, unstructEvent)
      .toEitherT[F]
      .leftMap { t =>
        NonEmptyList.of("Error while building the map of placeholders", t.toString)
      }

  private def fillPlaceholders(
    connection: Connection,
    sql: String,
    placeholders: Input.PlaceholderMap
  ): EitherT[F, NonEmptyList[String], Input.PlaceholderMap] =
    EitherT(blocker.blockOn(Monad[F].pure(DbExecutor.allPlaceholdersFilled(connection, sql, placeholders))))
      .leftMap { t =>
        NonEmptyList.of("Error while filling the placeholders", t.toString)
      }

  private def getResult(
    placeholders: Input.PlaceholderMap,
    connection: Connection
  ): EitherT[F, NonEmptyList[String], List[SelfDescribingData[Json]]] =
    placeholders match {
      case Some(m) =>
        EitherT(get(connection, m))
          .leftMap { t =>
            NonEmptyList.of("Error while executing the query/getting the results", t.toString)
          }
      case None =>
        EitherT.rightT[F, NonEmptyList[String]](List.empty[SelfDescribingData[Json]])
    }

  /**
   * Get contexts from cache or perform query if nothing found and put result into cache
   * @param intMap IntMap of extracted values
   * @return validated list of Self-describing contexts
   */
  def get(connection: Connection, intMap: IntMap[Input.ExtractedValue]): F[Either[Throwable, List[SelfDescribingData[Json]]]] =
    for {
      gotten <- cache.get(intMap)
      res <- gotten match {
               case Some(response) =>
                 if (System.currentTimeMillis() / 1000 - response._2 < ttl) Monad[F].pure(response._1)
                 else put(connection, blocker, intMap)
               case None => put(connection, blocker, intMap)
             }
    } yield res

  private def put(
    connection: Connection,
    blocker: BlockerF[F],
    intMap: IntMap[Input.ExtractedValue]
  ): F[Either[Throwable, List[SelfDescribingData[Json]]]] =
    for {
      res <- blocker.blockOn(query(connection, intMap).value)
      _ <- cache.put(intMap, (res, System.currentTimeMillis() / 1000))
    } yield res

  /**
   * Perform SQL query and convert result to JSON object
   * @param intMap map with values extracted from inputs and ready to be set placeholders in
   * prepared statement
   * @return validated list of Self-describing contexts
   */
  def query(connection: Connection, intMap: IntMap[Input.ExtractedValue]): EitherT[F, Throwable, List[SelfDescribingData[Json]]] =
    for {
      sqlQuery <- DbExecutor.createStatement(connection, query.sql, intMap).toEitherT[F]
      resultSet <- DbExecutor[F].execute(sqlQuery)
      context <- DbExecutor[F].convert(resultSet, output.json.propertyNames)
      result <- output.envelope(context).toEitherT[F]
    } yield result

  private def failureDetails(errors: NonEmptyList[String]) =
    errors.map { error =>
      val message = FailureDetails.EnrichmentFailureMessage.Simple(error)
      FailureDetails.EnrichmentFailure(enrichmentInfo, message)
    }

  private def closeConnection(connection: Connection): Unit =
    Either.catchNonFatal(connection.close()) match {
      case Left(err) =>
        logger.error("Can't close the connection", err)
      case _ =>
        ()
    }
}
