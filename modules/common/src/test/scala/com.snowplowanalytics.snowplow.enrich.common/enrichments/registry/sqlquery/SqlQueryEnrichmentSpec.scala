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

import io.circe.parser._
import io.circe.literal._

import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.SqlQueryConf
import com.snowplowanalytics.snowplow.enrich.common.utils.CirceUtils

class SqlQueryEnrichmentSpec extends Specification with ValidatedMatchers {
  def is = s2"""
  extract correct configuration       $e1
  fail to parse invalid configuration $e2
  extract correct MySQL configuration $e3
  """

  val SCHEMA_KEY =
    SchemaKey(
      "com.snowplowanalytics.snowplow.enrichments",
      "sql_query_enrichment_config",
      "jsonschema",
      SchemaVer.Full(1, 0, 1)
    )

  def e1 = {
    val inputs = List(
      Input.Pojo(1, "user_id"),
      Input.Json(
        1,
        "contexts",
        SchemaCriterion("com.snowplowanalytics.snowplow", "client_session", "jsonschema", 1),
        "$.userId"
      ),
      Input.Pojo(2, "app_id")
    )
    val db =
      Rdbms.PostgresqlDb(
        "cluster01.redshift.acme.com",
        5439,
        sslMode = true,
        "snowplow_enrich_ro",
        "1asIkJed",
        "crm"
      )
    val output = JsonOutput(
      SchemaKey("com.acme", "user", "jsonschema", SchemaVer.Full(1, 0, 0)),
      Output.DescribeMode.AllRows,
      JsonOutput.CamelCase
    )
    val cache = SqlQueryEnrichment.Cache(3000, 60)
    val query = SqlQueryEnrichment.Query(
      "SELECT username, email_address, date_of_birth FROM tbl_users WHERE user = ? AND client = ? LIMIT 1"
    )
    val config =
      SqlQueryConf(SCHEMA_KEY, inputs, db, query, Output(output, Output.AtMostOne), cache, ignoreOnError = true)

    val configuration = parse(
      """
      {
        "vendor": "com.snowplowanalytics.snowplow.enrichments",
        "name": "sql_query_enrichment_config",
        "enabled": true,
        "parameters": {
          "inputs": [
            {
              "placeholder": 1,
              "pojo": {
                "field": "user_id"
              }
            },
            {
              "placeholder": 1,
              "json": {
                "field": "contexts",
                "schemaCriterion": "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-*-*",
                "jsonPath": "$.userId"
              }
            },
            {
              "placeholder": 2,
              "pojo": {
                "field": "app_id"
              }
            }
          ],
          "query": {
            "sql": "SELECT username, email_address, date_of_birth FROM tbl_users WHERE user = ? AND client = ? LIMIT 1"
            },
          "database": {
            "postgresql": {
              "host": "cluster01.redshift.acme.com",
              "port": 5439,
              "sslMode": true,
              "username": "snowplow_enrich_ro",
              "password": "1asIkJed",
              "database": "crm"
            }
          },
          "output": {
            "expectedRows": "AT_MOST_ONE",
            "json": {
              "schema": "iglu:com.acme/user/jsonschema/1-0-0",
              "describes": "ALL_ROWS",
              "propertyNames": "CAMEL_CASE"
            }
          },
          "cache": {
            "size": 3000,
            "ttl": 60
          },
          "ignoreOnError": true
        }
      }"""
    ).toOption.get

    SqlQueryEnrichment.parse(configuration, SCHEMA_KEY) must beValid(config)
  }

  def e2 = {
    // $.output.json.describes contains invalid value
    val configuration = parse(
      """
      {
        "vendor": "com.snowplowanalytics.snowplow.enrichments",
        "name": "sql_query_enrichment_config",
        "enabled": true,
        "parameters": {
          "inputs": [
            {
              "placeholder": 1,
              "pojo": {
                "field": "user_id"
              }
            },
            {
              "placeholder": 1,
              "json": {
                "field": "contexts",
                "schemaCriterion": "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-*-*",
                "jsonPath": "$.userId"
              }
            },
            {
              "placeholder": 2,
              "pojo": {
                "field": "app_id"
              }
            }
          ],
          "query": {
            "sql": "SELECT username, email_address, date_of_birth FROM tbl_users WHERE user = ? AND client = ? LIMIT 1"
          },
          "database": {
            "postgresql": {
              "host": "cluster01.redshift.acme.com",
              "port": 5439,
              "sslMode": true,
              "username": "snowplow_enrich_ro",
              "password": "1asIkJed",
              "database": "crm"
            }
          },
          "output": {
            "expectedRows": "AT_MOST_ONE",
            "json": {
              "schema": "iglu:com.acme/user/jsonschema/1-0-0",
              "describes": "INVALID",
              "propertyNames": "CAMEL_CASE"
            }
          },
          "cache": {
            "size": 3000,
            "ttl": 60
          }
        }
      }"""
    ).toOption.get

    SqlQueryEnrichment.parse(configuration, SCHEMA_KEY) must beInvalid
  }

  def e3 = {
    val configuration = parse(
      """
      {
        "vendor": "com.snowplowanalytics.snowplow.enrichments",
        "name": "sql_query_enrichment_config",
        "enabled": true,
        "parameters": {
          "inputs": [
            {
              "placeholder": 1,
              "pojo": {
                "field": "user_id"
              }
            },
            {
              "placeholder": 1,
              "json": {
                "field": "contexts",
                "schemaCriterion": "iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-*-*",
                "jsonPath": "$.userId"
              }
            },
            {
              "placeholder": 2,
              "pojo": {
                "field": "app_id"
              }
            }
          ],
          "query": {
            "sql": "SELECT username, email_address, date_of_birth FROM tbl_users WHERE user = ? AND client = ? LIMIT 1"
           },
          "database": {
            "mysql": {
              "host": "cluster01.redshift.acme.com",
              "port": 5439,
              "sslMode": true,
              "username": "snowplow_enrich_ro",
              "password": "1asIkJed",
              "database": "crm"
            }
          },
          "output": {
            "expectedRows": "AT_LEAST_ONE",
            "json": {
              "schema": "iglu:com.acme/user/jsonschema/1-0-0",
              "describes": "EVERY_ROW",
              "propertyNames": "CAMEL_CASE"
            }
          },
          "cache": {
            "size": 3000,
            "ttl": 60
          }
        }
      }"""
    ).toOption.get

    SqlQueryEnrichment.parse(configuration, SCHEMA_KEY) must beValid
  }

  def e4 = {
    val pojoInput = json"""{"input": {"placeholder": 1, "pojo": { "field": "user_id" }}}"""
    CirceUtils.extract[Input](pojoInput, "input") must beValid(Input.Pojo(1, "user_id"))
  }
}
