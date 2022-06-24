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
package com.snowplowanalytics.snowplow.enrich.common
package enrichments.registry.sqlquery

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

    val driver: Class[_] = Class.forName("com.mysql.cj.jdbc.Driver") // Load class

    val connectionString =
      s"jdbc:mysql://$host:$port/$database?user=$username&password=$password" ++ (if (sslMode)
                                                                                    "&sslMode=REQUIRED"
                                                                                  else
                                                                                    "&sslMode=PREFERRED")
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
