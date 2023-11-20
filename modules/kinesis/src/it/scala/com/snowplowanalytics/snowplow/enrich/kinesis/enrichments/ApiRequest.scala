/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.kinesis.enrichments

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

case object ApiRequest extends Enrichment {
  val config = """
    {
      "schema": "iglu:com.snowplowanalytics.snowplow.enrichments/api_request_enrichment_config/jsonschema/1-0-0",
      "data": {
        "vendor": "com.snowplowanalytics.snowplow.enrichments",
        "name": "api_request_enrichment_config",
        "enabled": true,
        "parameters": {
          "inputs": [],
          "api": {
            "http": {
              "method": "GET",
              "uri": "http://api",
              "timeout": 2000,
              "authentication": {
                "httpBasic": {
                  "username": "xxxxx",
                  "password": "yyyyy"
                }
              }
            }
          },
          "outputs": [
            {
              "schema": "iglu:com.snowplowanalytics.snowplow/diagnostic_error/jsonschema/1-0-0",
              "json": {
                "jsonPath": "$"
              }
            }
          ],
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
    "diagnostic_error",
    "jsonschema",
    SchemaVer.Full(1, 0, 0)
  )
}
