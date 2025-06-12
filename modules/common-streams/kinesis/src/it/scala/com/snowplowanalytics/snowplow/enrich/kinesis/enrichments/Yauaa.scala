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

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.YauaaEnrichment
import com.snowplowanalytics.snowplow.enrich.streams.common.Enrichment

case object Yauaa extends Enrichment {
  val fileName = "yauaa_enrichment_config.json"
  val config = """
    {
      "schema": "iglu:com.snowplowanalytics.snowplow.enrichments/yauaa_enrichment_config/jsonschema/1-0-0",
      "data": {
        "enabled": true,
        "vendor": "com.snowplowanalytics.snowplow.enrichments",
        "name": "yauaa_enrichment_config"
      }
    }
  """

  val outputSchema = YauaaEnrichment.outputSchema
}
