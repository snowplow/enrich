/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import java.net.InetAddress

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import cats.effect.testing.specs2.CatsEffect

import io.circe.literal._

import inet.ipaddr.HostName
import org.specs2.Specification
import org.specs2.matcher.DataTables
import org.joda.time.DateTime

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

class IabEnrichmentSpec extends Specification with DataTables with CatsEffect {

  def is = s2"""
  performCheck should correctly perform IAB checks on valid input $e1
  getIabContext should return a valid JObject on valid input      $e2
  """

  // When testing, localMode is set to true, so the URIs are ignored and the databases are loaded from test/resources
  val validConfig = IabEnrichment
    .parse(
      json"""{
      "name": "iab_spiders_and_robots_enrichment",
      "vendor": "com.snowplowanalytics.snowplow.enrichments",
      "enabled": false,
      "parameters": {
        "ipFile": {
          "database": "ip_exclude_current_cidr.txt",
          "uri": "s3://my-private-bucket/iab"
        },
        "excludeUseragentFile": {
          "database": "exclude_current.txt",
          "uri": "s3://my-private-bucket/iab"
        },
        "includeUseragentFile": {
          "database": "include_current.txt",
          "uri": "s3://my-private-bucket/iab"
        }
      }
    }""",
      SchemaKey(
        "com.snowplowanalytics.snowplow.enrichments",
        "iab_spiders_and_robots_enrichment",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ),
      true
    )
    .toOption
    .get

  def e1 =
    "SPEC NAME" || "USER AGENT" | "IP ADDRESS" | "EXPECTED SPIDER OR ROBOT" | "EXPECTED CATEGORY" | "EXPECTED REASON" | "EXPECTED PRIMARY IMPACT" |
      "valid UA/IP" !! "Xdroid" ! "192.168.0.1".ip ! false ! "BROWSER" ! "PASSED_ALL" ! "NONE" |
      "valid UA, excluded IP" !! "Mozilla/5.0" ! "192.168.151.21".ip ! true ! "SPIDER_OR_ROBOT" ! "FAILED_IP_EXCLUDE" ! "UNKNOWN" |
      "invalid UA, excluded IP" !! "xonitor" ! "192.168.0.1".ip ! true ! "SPIDER_OR_ROBOT" ! "FAILED_UA_INCLUDE" ! "UNKNOWN" |> {
      (
        _,
        userAgent,
        ipAddress,
        expectedSpiderOrRobot,
        expectedCategory,
        expectedReason,
        expectedPrimaryImpact
      ) =>
        validConfig
          .enrichment[IO]
          .map { e =>
            e.performCheck(userAgent, ipAddress, DateTime.now()) must beRight.like {
              case check =>
                check.spiderOrRobot must_== expectedSpiderOrRobot and
                  (check.category must_== expectedCategory) and
                  (check.reason must_== expectedReason) and
                  (check.primaryImpact must_== expectedPrimaryImpact)
            }
          }
          .unsafeRunSync()
    }

  def e2 = {
    val responseJson =
      SelfDescribingData(
        SchemaKey("com.iab.snowplow", "spiders_and_robots", "jsonschema", SchemaVer.Full(1, 0, 0)),
        json"""{"spiderOrRobot": false, "category": "BROWSER", "reason": "PASSED_ALL", "primaryImpact": "NONE"}"""
      )
    validConfig
      .enrichment[IO]
      .map { e =>
        e.getIabContext("Xdroid", "192.168.0.1".ip, DateTime.now()) must beRight(responseJson)
      }
  }

  private implicit class IpOps(s: String) {
    def ip: InetAddress =
      new HostName(s).toInetAddress
  }
}
