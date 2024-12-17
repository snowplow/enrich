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

import cats.syntax.either._

import io.circe._
import io.circe.generic.semiauto._

/** Common trait for all supported databases, providing a JDBC connection URI */
trait Rdbms {

  /** Placeholder for database driver (not used) */
  def driver: Class[_]

  /** Correctly generated connection URI specific for database */
  def connectionString: String
}

object Rdbms {

  /** Class representing connection configuration for databases speaking PostgreSQL dialect */
  final case class PostgresqlDb(
    host: String,
    port: Int,
    sslMode: Boolean,
    username: String,
    password: String,
    database: String
  ) extends Rdbms {

    val driver: Class[_] = Class.forName("org.postgresql.Driver") // Load class

    val connectionString =
      s"jdbc:postgresql://$host:$port/$database?user=$username&password=$password" ++ (if (sslMode)
                                                                                         "&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"
                                                                                       else
                                                                                         "")
  }

  /** Class representing connection configuration for databases speaking MySQL dialect */
  final case class MysqlDb(
    host: String,
    port: Int,
    sslMode: Boolean,
    username: String,
    password: String,
    database: String
  ) extends Rdbms {

    val driver: Class[_] = Class.forName("org.mariadb.jdbc.Driver") // Load class

    val sslModeParam: String = if (sslMode) "&sslMode=trust" else "&sslMode=disable&allowPublicKeyRetrieval=true"

    val connectionString =
      s"jdbc:mysql://$host:$port/$database?permitMysqlScheme&user=$username&password=$password$sslModeParam"
  }

  val postgresqlDbDecoder: Decoder[PostgresqlDb] =
    deriveDecoder[PostgresqlDb]

  val mysqlDbDecoder: Decoder[MysqlDb] =
    deriveDecoder[MysqlDb]

  implicit val rdbmsCirceDecoder: Decoder[Rdbms] =
    Decoder.instance { cur =>
      cur.as[Map[String, Json]].flatMap { m =>
        m.get("postgresql") match {
          case Some(json) =>
            postgresqlDbDecoder.decodeJson(json)
          case None =>
            m.get("mysql") match {
              case Some(json) =>
                mysqlDbDecoder.decodeJson(json)
              case None =>
                DecodingFailure(
                  s"""No known DB present in ${cur.value.noSpaces}. "postgresql" and "mysql" are supported""",
                  cur.history
                ).asLeft[Rdbms]
            }
        }
      }
    }

}
