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
import com.snowplowanalytics.snowplow.enrich.streams.common.Enrichment

case object ApiRequest extends Enrichment {
  val fileName = "api_request_enrichment_config.json"
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
