/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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

import java.sql.Connection
import javax.sql.DataSource

import scala.collection.immutable.IntMap

import com.zaxxer.hikari.HikariDataSource

import io.circe._
import io.circe.generic.semiauto._

import cats.data.{EitherT, NonEmptyList, Validated, ValidatedNel}
import cats.implicits._

import cats.effect.{Async, Blocker, Clock, ContextShift}

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingData}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.SqlQueryConf
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.{CachingEvaluator, Enrichment, ParseableEnrichment}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.{CirceUtils, ShiftExecution}

/** Lets us create an SqlQueryConf from a Json */
object SqlQueryEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "sql_query_enrichment_config",
      "jsonschema",
      1,
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
        CirceUtils.extract[Cache](c, "parameters", "cache").toValidatedNel,
        CirceUtils
          .extract[Option[Boolean]](c, "parameters", "ignoreOnError")
          .map {
            case Some(value) => value
            case None => false
          }
          .toValidatedNel
      ).mapN(SqlQueryConf(schemaKey, _, _, _, _, _, _)).toEither
    }.toValidated

  /** Just a string with SQL, not escaped */
  final case class Query(sql: String) extends AnyVal

  /** Cache configuration */
  final case class Cache(size: Int, ttl: Int)

  implicit val queryCirceDecoder: Decoder[Query] =
    deriveDecoder[Query]

  implicit val cacheCirceDecoder: Decoder[Cache] =
    deriveDecoder[Cache]

  def create[F[_]: Async: Clock: ContextShift](
    schemaKey: SchemaKey,
    inputs: List[Input],
    db: Rdbms,
    query: Query,
    output: Output,
    cache: Cache,
    ignoreOnError: Boolean,
    blocker: Blocker,
    shifter: ShiftExecution[F]
  ): F[SqlQueryEnrichment[F]] = {
    val cacheConfig = CachingEvaluator.Config(
      size = cache.size,
      successTtl = cache.ttl,
      errorTtl = cache.ttl / 10
    )

    val executor: DbExecutor[F] = DbExecutor.async[F]

    CachingEvaluator
      .create[F, IntMap[Input.ExtractedValue], List[SelfDescribingData[Json]]](cacheConfig)
      .map { evaluator =>
        SqlQueryEnrichment(
          schemaKey,
          inputs,
          db,
          query,
          output,
          evaluator,
          executor,
          blocker,
          shifter,
          getDataSource(db),
          ignoreOnError
        )
      }
  }

  private def getDataSource(rdbms: Rdbms): HikariDataSource = {
    val source = new HikariDataSource()
    source.setJdbcUrl(rdbms.connectionString)
    source.setMaximumPoolSize(1) // see https://github.com/snowplow/enrich/issues/549
    source
  }
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
final case class SqlQueryEnrichment[F[_]: Async: Clock](
  schemaKey: SchemaKey,
  inputs: List[Input],
  db: Rdbms,
  query: SqlQueryEnrichment.Query,
  output: Output,
  sqlQueryEvaluator: SqlQueryEvaluator[F],
  dbExecutor: DbExecutor[F],
  blocker: Blocker,
  shifter: ShiftExecution[F],
  dataSource: DataSource,
  ignoreOnError: Boolean
) extends Enrichment {
  private val enrichmentInfo =
    FailureDetails.EnrichmentInformation(schemaKey, "sql-query").some

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
      placeholders <- buildPlaceHolderMap(inputs, event, derivedContexts, customContexts, unstructEvent)
      result <- maybeLookup(placeholders)
    } yield result

    contexts.leftMap(failureDetails).toValidated.map {
      case Validated.Invalid(_) if ignoreOnError => Validated.Valid(List.empty)
      case other => other
    }
  }

  private def maybeLookup(placeholders: Input.PlaceholderMap): EitherT[F, NonEmptyList[String], List[SelfDescribingData[Json]]] =
    placeholders match {
      case Some(intMap) =>
        EitherT {
          sqlQueryEvaluator.evaluateForKey(
            intMap,
            getResult = () => runLookup(intMap)
          )
        }.leftMap { t =>
          NonEmptyList.of("Error while executing the sql lookup", t.toString)
        }
      case None =>
        EitherT.rightT(Nil)
    }

  private def runLookup(intMap: Input.ExtractedValueMap): F[Either[Throwable, List[SelfDescribingData[Json]]]] =
    dbExecutor
      .getConnection(dataSource, blocker)
      .use { connection =>
        maybeRunWithConnection(connection, intMap)
      }
      .handleError(Left(_))

  // We now have a connection.  But time has passed since we last checked the cache, and another
  // fiber might have run a query while we were waiting for the connection. So we check the cache
  // one last time before hitting the database.
  private def maybeRunWithConnection(
    connection: Connection,
    intMap: Input.ExtractedValueMap
  ): F[Either[Throwable, List[SelfDescribingData[Json]]]] =
    sqlQueryEvaluator.evaluateForKey(
      intMap,
      getResult = () => runWithConnection(connection, intMap)
    )

  private def runWithConnection(
    connection: Connection,
    intMap: Input.ExtractedValueMap
  ): F[Either[Throwable, List[SelfDescribingData[Json]]]] = {
    val eitherT = DbExecutor.allPlaceholdersAreFilled(connection, query.sql, intMap).toEitherT[F].flatMap {
      case false =>
        // Not enough values were extracted from the event
        EitherT.rightT[F, Throwable](List.empty[SelfDescribingData[Json]])
      case true =>
        for {
          sqlQuery <- DbExecutor.createStatement(connection, query.sql, intMap).toEitherT[F]
          resultSet <- dbExecutor.execute(sqlQuery)
          context <- dbExecutor.convert(resultSet, output.json.propertyNames)
          result <- output.envelope(context).toEitherT[F]
        } yield result
    }

    shifter.shift(eitherT.value)
  }

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

  private def failureDetails(errors: NonEmptyList[String]) =
    errors.map { error =>
      val message = FailureDetails.EnrichmentFailureMessage.Simple(error)
      FailureDetails.EnrichmentFailure(enrichmentInfo, message)
    }
}
