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

import org.specs2.Specification

class RdbmsSpec extends Specification {
  def is = s2"""
  generate correct PostgreSQL connection string without SSL $e1
  generate correct PostgreSQL connection string with SSL    $e2
  generate correct MySQL connection string without SSL      $e3
  generate correct MySQL connection string with SSL         $e4
  """

  val username = "secretuser"
  val password = "secretpass"

  def noCredentials(rdbms: Rdbms) =
    (rdbms.connectionString must not(contain(username))) and
      (rdbms.connectionString must not(contain(password))) and
      (rdbms.username must beEqualTo(username)) and
      (rdbms.password must beEqualTo(password))

  def e1 = {
    val db = Rdbms.PostgresqlDb("localhost", 5432, false, username, password, "sql_enrichment_test")
    (db.connectionString must beEqualTo("jdbc:postgresql://localhost:5432/sql_enrichment_test")) and noCredentials(db)
  }

  def e2 = {
    val db = Rdbms.PostgresqlDb("localhost", 5432, true, username, password, "sql_enrichment_test")
    (db.connectionString must beEqualTo(
      "jdbc:postgresql://localhost:5432/sql_enrichment_test?sslmode=verify-full&sslfactory=org.postgresql.ssl.DefaultJavaSSLFactory"
    )) and noCredentials(db)
  }

  def e3 = {
    val db = Rdbms.MysqlDb("localhost", 3306, false, username, password, "sql_enrichment_test")
    (db.connectionString must beEqualTo(
      "jdbc:mysql://localhost:3306/sql_enrichment_test?permitMysqlScheme&sslMode=disable&allowPublicKeyRetrieval=true"
    )) and noCredentials(db)
  }

  def e4 = {
    val db = Rdbms.MysqlDb("localhost", 3306, true, username, password, "sql_enrichment_test")
    (db.connectionString must beEqualTo(
      "jdbc:mysql://localhost:3306/sql_enrichment_test?permitMysqlScheme&sslMode=verify-full"
    )) and noCredentials(db)
  }
}
