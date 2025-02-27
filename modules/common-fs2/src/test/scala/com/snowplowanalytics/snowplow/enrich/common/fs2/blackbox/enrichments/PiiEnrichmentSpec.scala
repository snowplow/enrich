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

import java.util.Base64

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsEffect

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class PiiEnrichmentSpec extends Specification with CatsEffect {
  "enrichWith" should {
    "enrich with PiiEnrichment" in {
      val base64Encoded =
        "CwBkAAAADTM3LjIyOC4yMjUuMzIKAMgAAAFjiJGp1QsA0gAAAAVVVEYtOAsA3AAAABJzc2MtMC4xMy4wLXN0ZG91dCQLASwAAAALY3VybC83LjUwLjMLAUAAAAAjL2NvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy90cDILAVQAAAFpeyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9wYXlsb2FkX2RhdGEvanNvbnNjaGVtYS8xLTAtNCIsImRhdGEiOlt7InR2IjoidHJhY2tlcl92ZXJzaW9uIiwiZSI6InVlIiwicCI6IndlYiIsInVlX3ByIjoie1wic2NoZW1hXCI6XCJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wXCIsXCJkYXRhXCI6e1wic2NoZW1hXCI6XCJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9zY3JlZW5fdmlldy9qc29uc2NoZW1hLzEtMC0wXCIsXCJkYXRhXCI6e1wibmFtZVwiOlwiaGVsbG8gZnJvbSBTbm93cGxvd1wifX19In1dfQ8BXgsAAAAFAAAAO0hvc3Q6IGVjMi0zNC0yNDUtMzItNDcuZXUtd2VzdC0xLmNvbXB1dGUuYW1hem9uYXdzLmNvbToxMjM0AAAAF1VzZXItQWdlbnQ6IGN1cmwvNy41MC4zAAAAC0FjY2VwdDogKi8qAAAAG1RpbWVvdXQtQWNjZXNzOiA8ZnVuY3Rpb24xPgAAABBhcHBsaWNhdGlvbi9qc29uCwFoAAAAEGFwcGxpY2F0aW9uL2pzb24LAZAAAAAwZWMyLTM0LTI0NS0zMi00Ny5ldS13ZXN0LTEuY29tcHV0ZS5hbWF6b25hd3MuY29tCwGaAAAAJDEwZDk2YmM3LWU0MDAtNGIyOS04YTQxLTY5MTFhZDAwZWU5OAt6aQAAAEFpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9Db2xsZWN0b3JQYXlsb2FkL3RocmlmdC8xLTAtMAA="
      val input = Base64.getDecoder.decode(base64Encoded)
      val expected = Map(
        "pii" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/pii_transformation/jsonschema/1-0-0","data":{"pii":{"pojo":[{"fieldName":"user_ipaddress","originalValue":"37.228.225.32","modifiedValue":"9e78f7282f027de55ec5c67aac71ae58cd78e203"}]},"strategy":{"pseudonymize":{"hashFunction":"SHA-1"}}}}}""".noSpaces
      )
      BlackBoxTesting.runTest(input, expected, Some(PiiEnrichmentSpec.conf))
    }
  }
}

object PiiEnrichmentSpec {
  val conf = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow.enrichments/pii_enrichment_config/jsonschema/2-0-0",
      "data": {
        "vendor": "com.snowplowanalytics.snowplow.enrichments",
        "name": "pii_enrichment_config",
        "emitEvent": true,
        "enabled": true,
        "parameters": {
          "pii": [
            {
              "pojo": {
                "field": "user_id"
              }
            },
            {
              "pojo": {
                "field": "user_ipaddress"
              }
            }
          ],
          "strategy": {
            "pseudonymize": {
              "hashFunction": "SHA-1",
              "salt": "pepper123"
            }
          }
        }
      }
    }
  """
}
