/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
