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
package com.snowplowanalytics.snowplow.enrich.kinesis.enrichments

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.enrich.core.Enrichment

import java.util.Base64

case object JavascriptDropEvent extends Enrichment {
  val fileName = "javascript_script_enrichment.json"

  private val base64Encoder = Base64.getEncoder()

  val script = """
    function process(event){
      var trackerId = event.getV_tracker()
      if (trackerId == "drop") {
        event.drop()
      } else {
        return [ ]
      }
    }
  """
  val config = s"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/javascript_script_config/jsonschema/1-0-0",
      "data": {
        "vendor": "com.snowplowanalytics.snowplow",
        "name": "javascript_script_config",
        "enabled": true,
        "parameters": {
          "script": "${new String(base64Encoder.encode(script.getBytes()))}"
        }
      }
    }
  """
  val outputSchema = SchemaKey(
    "com.snowplowanalytics.snowplow",
    "identify",
    "jsonschema",
    SchemaVer.Full(1, 0, 0)
  )
}
