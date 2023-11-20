/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.kinesis.enrichments

import java.util.Base64

object Enrichments {
  private val base64Encoder = Base64.getEncoder()

  def mkJson(enrichments: List[String]): String = {
    val raw = s"""
      {
        "schema": "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0",
        "data": [
          ${enrichments.mkString(",")}
        ]
      }
    """
    new String(base64Encoder.encode(raw.getBytes()))
  }
}
