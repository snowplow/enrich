/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.kinesis.enrichments

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.YauaaEnrichment

case object Yauaa extends Enrichment {
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
