/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import cats.implicits._

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import cats.effect.testing.specs2.CatsEffect

import io.circe.literal._

import org.specs2.Specification
import org.specs2.matcher.DataTables

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.maxmind.iplookups.model.IpLocation

class IpLookupsEnrichmentSpec extends Specification with DataTables with CatsEffect {

  def is = s2"""
  extractIpInformation should correctly extract location data from IP addresses where possible $e1
  extractIpInformation should correctly extract ISP data from IP addresses where possible      $e2
  extractIpInformation should return result with left's when IP address is invalid oversized string $e3
  """

  // When testing, localMode is set to true, so the URIs are ignored and the databases are loaded from test/resources
  val config = IpLookupsEnrichment
    .parse(
      json"""{
      "name": "ip_lookups",
      "vendor": "com.snowplowanalytics.snowplow",
      "enabled": true,
      "parameters": {
        "geo": {
          "database": "GeoIP2-City.mmdb",
          "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
        },
        "isp": {
          "database": "GeoIP2-ISP.mmdb",
          "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
        }
      }
    }""",
      SchemaKey(
        "com.snowplowanalytics.snowplow",
        "ip_lookups",
        "jsonschema",
        SchemaVer.Full(2, 0, 0)
      ),
      true
    )
    .toOption
    .get

  def e1 =
    "SPEC NAME" || "IP ADDRESS" | "EXPECTED LOCATION" |
      "blank IP address" !! "" ! "AddressNotFoundException".asLeft.some |
      "null IP address" !! null ! "AddressNotFoundException".asLeft.some |
      "invalid IP address #1" !! "localhost" ! "AddressNotFoundException".asLeft.some |
      "invalid IP address #2" !! "hello" ! "UnknownHostException".asLeft.some |
      "invalid IP address #3" !! ":::::::::::::::" ! "UnknownHostException".asLeft.some |
      "valid IP address" !! "175.16.199.0" !
        IpLocation( // Taken from scala-maxmind-geoip. See that test suite for other valid IP addresses
          countryCode = "CN",
          countryName = "China",
          region = Some("22"),
          city = Some("Changchun"),
          latitude = 43.88f,
          longitude = 125.3228f,
          timezone = Some("Asia/Harbin"),
          postalCode = None,
          metroCode = None,
          regionName = Some("Jilin Sheng"),
          isInEuropeanUnion = false,
          continent = "Asia",
          accuracyRadius = 100
        ).asRight.some |
      "valid IP address with port" !! "175.16.199.0:8080" !
        IpLocation( // Taken from scala-maxmind-geoip. See that test suite for other valid IP addresses
          countryCode = "CN",
          countryName = "China",
          region = Some("22"),
          city = Some("Changchun"),
          latitude = 43.88f,
          longitude = 125.3228f,
          timezone = Some("Asia/Harbin"),
          postalCode = None,
          metroCode = None,
          regionName = Some("Jilin Sheng"),
          isInEuropeanUnion = false,
          continent = "Asia",
          accuracyRadius = 100
        ).asRight.some |> { (_, ipAddress, expected) =>
      (for {
        ipLookup <- config.enrichment[IO]
        result <- ipLookup.extractIpInformation(ipAddress)
        ipLocation = result.ipLocation.map(_.leftMap(_.getClass.getSimpleName))
      } yield ipLocation must beEqualTo(expected)).unsafeRunSync()
    }

  def e2 =
    for {
      ipLookup <- config.enrichment[IO]
      result <- ipLookup.extractIpInformation("70.46.123.145")
      isp = result.isp
    } yield isp must beEqualTo(Some(Right("FDN Communications")))

  def e3 =
    for {
      ipLookup <- config.enrichment[IO]
      oversized = "a".repeat(1000000)
      result <- ipLookup.extractIpInformation(oversized)
    } yield result.ipLocation must beLike {
      case Some(Left(_)) => ok
    }
}
