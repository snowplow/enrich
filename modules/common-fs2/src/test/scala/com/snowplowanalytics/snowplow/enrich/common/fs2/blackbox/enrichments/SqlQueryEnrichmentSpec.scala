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
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.enrichments

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsEffect

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class SqlQueryEnrichmentSpec extends Specification with CatsEffect {

  args(skipAll = !sys.env.get("CI").contains("true"))

  "enrichWith" should {
    "enrich with SqlQueryEnrichment" in {
      val contexts =
        """eyJkYXRhIjpbeyJkYXRhIjp7ImxvbmdpdHVkZSI6MTAsImJlYXJpbmciOjUwLCJzcGVlZCI6MjUuMCwiYWx0aXR1ZGUiOjIwLCJhbHRpdHVkZUFjY3VyYWN5IjowLjMsImxhdGl0dWRlTG9uZ2l0dWRlQWNjdXJhY3kiOjAuNSwibGF0aXR1ZGUiOjd9LCJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9nZW9sb2NhdGlvbl9jb250ZXh0L2pzb25zY2hlbWEvMS0wLTAifV0sInNjaGVtYSI6ImlnbHU6Y29tLnNub3dwbG93YW5hbHl0aWNzLnNub3dwbG93L2NvbnRleHRzL2pzb25zY2hlbWEvMS0wLTAifQo="""
      val unstructEvent =
        """%7B%22schema%22%3A%22iglu%3Acom.snowplowanalytics.snowplow%2Funstruct_event%2Fjsonschema%2F1-0-0%22%2C%22data%22%3A%7B%22schema%22%3A%22iglu%3Acom.snowplowanalytics.snowplow-website%2Fsignup_form_submitted%2Fjsonschema%2F1-0-0%22%2C%22data%22%3A%7B%22name%22%3A%22Bob%C2%AE%22%2C%22email%22%3A%22alex%2Btest%40snowplowanalytics.com%22%2C%22company%22%3A%22SP%22%2C%22eventsPerMonth%22%3A%22%3C%201%20million%22%2C%22serviceType%22%3A%22unsure%22%7D%7D%7D"""
      val input = BlackBoxTesting.buildCollectorPayload(
        querystring = s"e=ue&cx=$contexts&ue_pr=$unstructEvent".some,
        path = "/i",
        userAgent = Some("Milton")
      )
      val expected = Map(
        "derived_contexts" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.statusgator/status_change/jsonschema/1-0-0","data":{"serviceName":"sp-sql-request-enrichment","currentStatus":"OK","lastStatus":"OK","pk":1}}]}""".noSpaces
      )
      BlackBoxTesting.runTest(input, expected, Some(SqlQueryEnrichmentSpec.conf))
    }
  }
}

object SqlQueryEnrichmentSpec {
  val conf = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow.enrichments/sql_query_enrichment_config/jsonschema/1-0-0",
      "data": {
        "vendor": "com.snowplowanalytics.snowplow.enrichments",
        "name": "sql_query_enrichment_config",
        "enabled": true,
        "parameters": {
          "inputs": [],
          "database": {
            "postgresql": {
              "host": "localhost",
              "port": 5432,
              "sslMode": false,
              "username": "enricher",
              "password": "supersecret1",
              "database": "sql_enrichment_test"
            }
          },
          "query": {
            "sql": "SELECT service_name, current_status, last_status, pk FROM enrichment_test"
          },
          "output": {
            "expectedRows": "EXACTLY_ONE",
            "json": {
              "schema": "iglu:com.statusgator/status_change/jsonschema/1-0-0",
              "describes": "ALL_ROWS",
              "propertyNames": "CAMEL_CASE"
            }
          },
          "cache": {
            "size": 1000,
            "ttl": 60
          }
        }
      }
    }
    """
}
