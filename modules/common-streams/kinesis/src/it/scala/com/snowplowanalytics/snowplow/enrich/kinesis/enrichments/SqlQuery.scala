/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.streams.kinesis.enrichments

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

case object SqlQuery extends Enrichment {
  val fileName = "sql_query_enrichment_config.json"

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
