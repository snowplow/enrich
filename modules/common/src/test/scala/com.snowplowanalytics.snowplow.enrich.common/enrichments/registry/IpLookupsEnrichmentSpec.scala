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

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.maxmind.iplookups.model.IpLocation

class IpLookupsEnrichmentSpec extends Specification with DataTables with CatsEffect {

  def is = s2"""
  extractIpInformation should correctly extract location data from IP addresses where possible $e1
  extractIpInformation should correctly extract ISP data from IP addresses where possible      $e2
  extractIpInformation should skip the lookup when IP address is invalid oversized string    $e3
  getAsnContext should correctly extract ASN context from IpLookupResult                       $e4
  extractIpInformation should run for IPv4 and IPv6 and be skipped for a hostname or non-IP    $e5
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
          "database": "GeoIP2-City-Test.mmdb",
          "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
        },
        "isp": {
          "database": "GeoIP2-ISP-Test.mmdb",
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
      "blank IP address" !! "" ! None |
      "null IP address" !! null ! None |
      "invalid IP address #1" !! "localhost" ! None |
      "invalid IP address #2" !! "hello" ! None |
      "invalid IP address #3" !! ":::::::::::::::" ! None |
      "hostname" !! "example.com" ! None |
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
        ipLocation = result.flatMap(_.ipLocation).map(_.leftMap(_.getClass.getSimpleName))
      } yield ipLocation must beEqualTo(expected)).unsafeRunSync()
    }

  def e2 =
    for {
      ipLookup <- config.enrichment[IO]
      result <- ipLookup.extractIpInformation("70.46.123.145")
      isp = result.flatMap(_.isp)
    } yield isp must beEqualTo(Some(Right("FDN Communications")))

  def e3 =
    for {
      ipLookup <- config.enrichment[IO]
      oversized = "a".repeat(1000000)
      result <- ipLookup.extractIpInformation(oversized)
    } yield result must beNone

  def e4 =
    for {
      ipLookup <- config.enrichment[IO]
      result <- ipLookup.extractIpInformation("18.11.120.0")
      asnContext = result.flatMap(ipLookup.getAsnContext)
    } yield {
      val expected = SelfDescribingData(
        schema = IpLookupsEnrichment.asnSchema,
        data = json"""{
          "number": 3,
          "organization": "Massachusetts Institute of Technology"
        }"""
      ).some

      asnContext must beEqualTo(expected)
    }

  def e5 =
    "SPEC NAME" || "IP ADDRESS" | "LOOKUP RUNS" |
      "IPv4" !! "175.16.199.0" ! true |
      "IPv6" !! "2001:db8::1" ! true |
      "hostname" !! "example.com" ! false |
      "non-IP string" !! "not an ip" ! false |> { (_, ipAddress, expected) =>
      (for {
        ipLookup <- config.enrichment[IO]
        result <- ipLookup.extractIpInformation(ipAddress)
      } yield result.isDefined must beEqualTo(expected)).unsafeRunSync()
    }
}
