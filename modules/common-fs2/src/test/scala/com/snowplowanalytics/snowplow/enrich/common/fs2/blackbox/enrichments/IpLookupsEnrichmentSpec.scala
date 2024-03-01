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
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.enrichments

import io.circe.literal._

import java.util.Base64

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsEffect

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class IpLookupsEnrichmentSpec extends Specification with CatsEffect {
  "enrichWith" should {
    "enrich with IpLookupsEnrichment" in {
      val base64Encoded =
        "CwBkAAAADTM3LjIyOC4yMjUuMzIKAMgAAAFjiJGp1QsA0gAAAAVVVEYtOAsA3AAAABJzc2MtMC4xMy4wLXN0ZG91dCQLASwAAAALY3VybC83LjUwLjMLAUAAAAAjL2NvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy90cDILAVQAAAFpeyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9wYXlsb2FkX2RhdGEvanNvbnNjaGVtYS8xLTAtNCIsImRhdGEiOlt7InR2IjoidHJhY2tlcl92ZXJzaW9uIiwiZSI6InVlIiwicCI6IndlYiIsInVlX3ByIjoie1wic2NoZW1hXCI6XCJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wXCIsXCJkYXRhXCI6e1wic2NoZW1hXCI6XCJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9zY3JlZW5fdmlldy9qc29uc2NoZW1hLzEtMC0wXCIsXCJkYXRhXCI6e1wibmFtZVwiOlwiaGVsbG8gZnJvbSBTbm93cGxvd1wifX19In1dfQ8BXgsAAAAFAAAAO0hvc3Q6IGVjMi0zNC0yNDUtMzItNDcuZXUtd2VzdC0xLmNvbXB1dGUuYW1hem9uYXdzLmNvbToxMjM0AAAAF1VzZXItQWdlbnQ6IGN1cmwvNy41MC4zAAAAC0FjY2VwdDogKi8qAAAAG1RpbWVvdXQtQWNjZXNzOiA8ZnVuY3Rpb24xPgAAABBhcHBsaWNhdGlvbi9qc29uCwFoAAAAEGFwcGxpY2F0aW9uL2pzb24LAZAAAAAwZWMyLTM0LTI0NS0zMi00Ny5ldS13ZXN0LTEuY29tcHV0ZS5hbWF6b25hd3MuY29tCwGaAAAAJDEwZDk2YmM3LWU0MDAtNGIyOS04YTQxLTY5MTFhZDAwZWU5OAt6aQAAAEFpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9Db2xsZWN0b3JQYXlsb2FkL3RocmlmdC8xLTAtMAA="
      val input = Base64.getDecoder.decode(base64Encoded)

      val expected = Map(
        "event_vendor" -> "com.snowplowanalytics.snowplow",
        "event_name" -> "screen_view",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "platform" -> "web",
        "v_tracker" -> "tracker_version",
        "user_ipaddress" -> "37.228.225.32",
        "network_userid" -> "10d96bc7-e400-4b29-8a41-6911ad00ee98",
        "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/screen_view/jsonschema/1-0-0","data":{"name":"hello from Snowplow"}}}""".noSpaces,
        "geo_latitude" -> "53.3331",
        "geo_longitude" -> "-6.2489",
        "geo_country" -> "IE",
        "geo_region" -> "L",
        "geo_zipcode" -> "D02",
        "geo_city" -> "Dublin",
        "geo_region_name" -> "Leinster"
      )
      BlackBoxTesting.runTest(input, expected, Some(IpLookupsEnrichmentSpec.conf))
    }
  }
}

object IpLookupsEnrichmentSpec {
  val conf = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0",
      "data": {
        "name": "ip_lookups",
        "vendor": "com.snowplowanalytics.snowplow",
        "enabled": true,
        "parameters": {
          "geo": {
            "database": "GeoLite2-City.mmdb",
            "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
          }
        }
      }
    }
  """
}
