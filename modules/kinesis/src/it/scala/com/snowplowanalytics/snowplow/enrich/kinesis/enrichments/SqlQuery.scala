/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.kinesis.enrichments

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

case object SqlQuery extends Enrichment {
  val config = """
    {
      "schema": "iglu:com.snowplowanalytics.snowplow.enrichments/sql_query_enrichment_config/jsonschema/1-0-0",
      "data": {
        "vendor": "com.snowplowanalytics.snowplow.enrichments",
        "name": "sql_query_enrichment_config",
        "enabled": true,
        "parameters": {
          "inputs": [],
          "database": {
            "mysql": {
              "host": "mysql",
              "port": 3306,
              "sslMode": false,
              "database": "snowplow",
              "username": "enricher",
              "password": "supersecret1"
            }
          },
          "query": {
            "sql": "SELECT * FROM coordinates LIMIT 1"
          },
          "output": {
            "expectedRows": "AT_MOST_ONE",
            "json": {
              "schema": "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0",
              "describes": "ALL_ROWS",
              "propertyNames": "AS_IS"
            }
          },
          "cache": {
            "size": 3000,
            "ttl": 60
          }
        }
      }
    }
  """

  val outputSchema = SchemaKey(
    "com.snowplowanalytics.snowplow",
    "geolocation_context",
    "jsonschema",
    SchemaVer.Full(1, 1, 0)
  )
}