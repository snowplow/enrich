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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery

import scala.collection.immutable.IntMap

import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData}
import javax.sql.DataSource

import io.circe.Json

import cats.Monad
import cats.data.EitherT
import cats.implicits._

import cats.effect.kernel.{Async, MonadCancel, Resource, Sync}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery.Input.ExtractedValue

// DbExecutor must have much smaller interface, ideally without any JDBC types
/** Side-effecting ability to connect to database */
trait DbExecutor[F[_]] {

  /** Get a connection from the Hikari data source */
  def getConnection(dataSource: DataSource): Resource[F, Connection]

  /** Execute a SQL query */
  def execute(query: PreparedStatement): EitherT[F, Throwable, ResultSet]

  /**
   * Convert list of rows fetched from DB into list (probably empty or single-element) of
   * Self-describing JSON objects (contexts). Primary function of class
   * @param resultSet rows fetched from DB
   * @return list of successful Self-describing JSON Objects or error
   */
  def convert(resultSet: ResultSet, names: JsonOutput.PropertyNameMode): EitherT[F, Throwable, List[Json]]

  /** Lift failing ResultSet#getMetaData into scalaz disjunction with Throwable as left-side */
  def getMetaData(rs: ResultSet): EitherT[F, Throwable, ResultSetMetaData]

  /** Lift failing ResultSetMetaData#getColumnCount into Either */
  def getColumnCount(rsMeta: ResultSetMetaData): EitherT[F, Throwable, Int]

  /** Lift failing ResultSetMetaData#getColumnLabel into Either */
  def getColumnLabel(column: Int, rsMeta: ResultSetMetaData): EitherT[F, Throwable, String]

  /** Lift failing ResultSetMetaData#getColumnClassName into Either */
  def getColumnType(column: Int, rsMeta: ResultSetMetaData): EitherT[F, Throwable, String]

  /**
   * Get value from ResultSet using column number
   *
   * @param datatype  stringified type representing real type
   * @param columnIdx column's number in table
   * @param rs        result set fetched from DB
   * @return JSON in case of success or Throwable in case of SQL error
   */
  def getColumnValue(
    datatype: String,
    columnIdx: Int,
    rs: ResultSet
  ): EitherT[F, Throwable, Json]
}

object DbExecutor {

  def apply[F[_]](implicit ev: DbExecutor[F]): DbExecutor[F] = ev

  def async[F[_]: Async]: DbExecutor[F] = sync[F]

  def sync[F[_]: Sync]: DbExecutor[F] =
    new DbExecutor[F] {
      def getConnection(dataSource: DataSource): Resource[F, Connection] =
        Resource.fromAutoCloseable(Sync[F].blocking(dataSource.getConnection()))

      def execute(query: PreparedStatement): EitherT[F, Throwable, ResultSet] =
        Sync[F].delay(query.executeQuery()).attemptT

      def convert(resultSet: ResultSet, names: JsonOutput.PropertyNameMode): EitherT[F, Throwable, List[Json]] =
        EitherT(MonadCancel[F, Throwable].bracket(Sync[F].pure(resultSet)) { set =>
          val hasNext = Sync[F].delay(set.next()).attemptT
          val convert = transform(set, names)(this, Monad[F])
          convert.whileM[List](hasNext).value
        } { set =>
          Sync[F].delay(set.close())
        })

      def getMetaData(rs: ResultSet): EitherT[F, Throwable, ResultSetMetaData] =
        Sync[F].delay(rs.getMetaData).attemptT

      def getColumnCount(rsMeta: ResultSetMetaData): EitherT[F, Throwable, Int] =
        Sync[F].delay(rsMeta.getColumnCount).attemptT

      def getColumnLabel(column: Int, rsMeta: ResultSetMetaData): EitherT[F, Throwable, String] =
        Sync[F].delay(rsMeta.getColumnLabel(column)).attemptT

      def getColumnType(column: Int, rsMeta: ResultSetMetaData): EitherT[F, Throwable, String] =
        Sync[F].delay(rsMeta.getColumnClassName(column)).attemptT

      def getColumnValue(
        datatype: String,
        columnIdx: Int,
        rs: ResultSet
      ): EitherT[F, Throwable, Json] =
        Sync[F]
          .delay(rs.getObject(columnIdx))
          .attemptT
          .map(Option.apply)
          .map {
            case Some(any) => JsonOutput.getValue(any, datatype)
            case None => Json.Null
          }

    }

  /**
   * Transform fetched from DB row (as ResultSet) into JSON object
   * All column names are mapped to object keys using propertyNames
   *
   * @param resultSet column fetched from DB
   * @return JSON object as right disjunction in case of success or throwable as left disjunction in
   *         case of any error
   */
  def transform[F[_]: DbExecutor: Monad](resultSet: ResultSet, propertyNames: JsonOutput.PropertyNameMode): EitherT[F, Throwable, Json] =
    for {
      rsMeta <- DbExecutor[F].getMetaData(resultSet)
      columnNumbers <- DbExecutor[F].getColumnCount(rsMeta).map((x: Int) => (1 to x).toList)
      keyValues <- columnNumbers.traverse { idx =>
                     for {
                       colLabel <- DbExecutor[F].getColumnLabel(idx, rsMeta)
                       colType <- DbExecutor[F].getColumnType(idx, rsMeta)
                       value <- DbExecutor[F].getColumnValue(colType, idx, resultSet)
                     } yield propertyNames.transform(colLabel) -> value
                   }
    } yield Json.obj(keyValues: _*)

  /** Get amount of placeholders (?-signs) in Prepared Statement */
  def getPlaceholderCount(
    connection: Connection,
    sql: String
  ): Either[Throwable, Int] =
    createEmptyStatement(connection, sql)
      .flatMap(statement => Either.catchNonFatal(statement.getParameterMetaData.getParameterCount))

  /**
   * Create PreparedStatement and fill all its placeholders. This function expects `placeholderMap`
   * contains exact same amount of placeholders as `sql`, otherwise it will result in error
   * downstream
   * @param sql prepared SQL statement with some unfilled placeholders (?-signs)
   * @param placeholderMap IntMap with input values
   * @return filled placeholder or error (unlikely)
   */
  def createStatement(
    connection: Connection,
    sql: String,
    placeholderMap: IntMap[ExtractedValue]
  ): Either[Throwable, PreparedStatement] =
    createEmptyStatement(connection, sql).map { preparedStatement =>
      placeholderMap.foreach {
        case (index, value) =>
          value.set(preparedStatement, index)
      }
      preparedStatement
    }

  /** Transform SQL-string with placeholders (?-signs) into PreparedStatement */
  private def createEmptyStatement(
    connection: Connection,
    sql: String
  ): Either[Throwable, PreparedStatement] =
    Either.catchNonFatal(connection.prepareStatement(sql))

  /**
   * Transform [[Input.PlaceholderMap]] to None if not enough input values were extracted
   * This prevents db from start building a statement while not failing event enrichment
   * @param intMap The extracted values from the event
   * @return Whether all placeholders were filled
   */
  def allPlaceholdersAreFilled(
    connection: Connection,
    sql: String,
    intMap: Input.ExtractedValueMap
  ): Either[Throwable, Boolean] =
    getPlaceholderCount(connection, sql).map { placeholderCount =>
      if (intMap.keys.size == placeholderCount) true else false
    }

  def getConnection[F[_]: Monad: DbExecutor](dataSource: DataSource): Resource[F, Connection] =
    DbExecutor[F].getConnection(dataSource)
}
