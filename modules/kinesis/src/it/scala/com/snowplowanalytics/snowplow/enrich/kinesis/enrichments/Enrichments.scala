/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
