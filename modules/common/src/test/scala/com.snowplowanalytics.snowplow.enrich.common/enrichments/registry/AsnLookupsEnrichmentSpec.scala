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

import org.specs2.Specification

import cats.data.EitherT
import cats.syntax.either._

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect

import io.circe.literal.JsonStringContext

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

class AsnLookupsEnrichmentSpec extends Specification with CatsEffect {

  def is = s2"""
  AsnLookupsEnrichment should
    load ASNs from CSV file $e1
    work without botAsnsFile (file is optional) $e2
    merge botAsns from config with ASNs from file $e3
    return helpful error message when CSV contains an invalid ASN $e4
  AsnLookupsEnrichment.lookupAsn should
    return None when platform is in bypassPlatforms $e5
    return updated context with likelyBot=true when ASN in the bot list $e6
    return updated context with likelyBot=false when ASN is not in the bot list $e7
    include organization field when present $e8
    omit organization field when absent $e9
  """

  def e1 =
    (for {
      conf <- EitherT.fromEither[IO](
                AsnLookupsEnrichment
                  .parse(
                    json"""{
                      "name": "asn_lookups",
                      "vendor": "com.snowplowanalytics.snowplow.enrichments",
                      "enabled": true,
                      "parameters": {
                        "botAsnsFile": {
                          "uri": "http://example.com",
                          "database": "bad-asn-list.csv"
                        }
                      }
                    }""",
                    SchemaKey(
                      "com.snowplowanalytics.snowplow.enrichments",
                      "asn_lookups",
                      "jsonschema",
                      SchemaVer.Full(1, 0, 0)
                    ),
                    localMode = true
                  )
                  .toEither
                  .leftMap(_.head)
              )
      enrichment <- conf.enrichment[IO]
    } yield enrichment.botAsns).value.map { result =>
      result must beRight.like {
        case asns =>
          asns must beEqualTo(Set(174L, 612L, 1442L, 2611L, 3223L, 3396L, 3502L, 3561L, 3563L, 3700L))
      }
    }

  def e2 =
    (for {
      conf <- EitherT.fromEither[IO](
                AsnLookupsEnrichment
                  .parse(
                    json"""{
                      "name": "asn_lookups",
                      "vendor": "com.snowplowanalytics.snowplow.enrichments",
                      "enabled": true,
                      "parameters": {
                        "botAsns": [
                          {"asn": 1234, "name": "Test Entity"},
                          {"asn": 5678}
                        ]
                      }
                    }""",
                    SchemaKey(
                      "com.snowplowanalytics.snowplow.enrichments",
                      "asn_lookups",
                      "jsonschema",
                      SchemaVer.Full(1, 0, 0)
                    ),
                    localMode = false
                  )
                  .toEither
                  .leftMap(_.head)
              )
      enrichment <- conf.enrichment[IO]
    } yield enrichment.botAsns).value.map { result =>
      result must beRight.like {
        case asns =>
          asns must beEqualTo(Set(1234L, 5678L))
      }
    }

  def e3 =
    (for {
      conf <- EitherT.fromEither[IO](
                AsnLookupsEnrichment
                  .parse(
                    json"""{
                      "name": "asn_lookups",
                      "vendor": "com.snowplowanalytics.snowplow.enrichments",
                      "enabled": true,
                      "parameters": {
                        "botAsnsFile": {
                          "uri": "http://example.com",
                          "database": "bad-asn-list.csv"
                        },
                        "botAsns": [
                          {"asn": 9999, "name": "Test Entity"},
                          {"asn": 8888, "name": "Another Test"}
                        ]
                      }
                    }""",
                    SchemaKey(
                      "com.snowplowanalytics.snowplow.enrichments",
                      "asn_lookups",
                      "jsonschema",
                      SchemaVer.Full(1, 0, 0)
                    ),
                    localMode = true
                  )
                  .toEither
                  .leftMap(_.head)
              )
      enrichment <- conf.enrichment[IO]
    } yield enrichment.botAsns).value.map { result =>
      result must beRight.like {
        case asns =>
          asns must beEqualTo(Set(174L, 612L, 1442L, 2611L, 3223L, 3396L, 3502L, 3561L, 3563L, 3700L, 9999L, 8888L))
      }
    }

  def e4 =
    (for {
      conf <- EitherT.fromEither[IO](
                AsnLookupsEnrichment
                  .parse(
                    json"""{
                      "name": "asn_lookups",
                      "vendor": "com.snowplowanalytics.snowplow.enrichments",
                      "enabled": true,
                      "parameters": {
                        "botAsnsFile": {
                          "uri": "http://example.com",
                          "database": "bad-asn-list-invalid.csv"
                        }
                      }
                    }""",
                    SchemaKey(
                      "com.snowplowanalytics.snowplow.enrichments",
                      "asn_lookups",
                      "jsonschema",
                      SchemaVer.Full(1, 0, 0)
                    ),
                    localMode = true
                  )
                  .toEither
                  .leftMap(_.head)
              )
      enrichment <- conf.enrichment[IO]
    } yield enrichment).value.map { result =>
      result must beLeft.like {
        case error =>
          (error must contain("Failed to parse CSV file with ASNs")) and
            (error must contain("not-a-number"))
      }
    }

  def e5 = {
    val enrichment = AsnLookupsEnrichment(
      botAsns = Set(174L, 612L),
      bypassPlatforms = Set("srv", "cnsl")
    )

    val asnContext = SelfDescribingData(
      IpLookupsEnrichment.asnSchema,
      json"""{
        "number": 174,
        "organization": "Cogent Communications"
      }"""
    )

    (enrichment.lookupAsn(asnContext, Some("srv")) must beNone) and
      (enrichment.lookupAsn(asnContext, Some("cnsl")) must beNone) and
      (enrichment.lookupAsn(asnContext, Some("web")) must beSome)
  }

  def e6 = {
    val enrichment = AsnLookupsEnrichment(
      botAsns = Set(174L, 612L),
      bypassPlatforms = Set.empty
    )

    val asnContext = SelfDescribingData(
      IpLookupsEnrichment.asnSchema,
      json"""{
        "number": 174,
        "organization": "Cogent Communications"
      }"""
    )

    val result = enrichment.lookupAsn(asnContext, Some("web"))

    result must beSome.like {
      case sdj =>
        (sdj.schema must beEqualTo(IpLookupsEnrichment.asnSchema)) and
          (sdj.data.hcursor.get[Long]("number") must beRight(174L)) and
          (sdj.data.hcursor.get[Boolean]("likelyBot") must beRight(true)) and
          (sdj.data.hcursor.get[String]("organization") must beRight("Cogent Communications"))
    }
  }

  def e7 = {
    val enrichment = AsnLookupsEnrichment(
      botAsns = Set(174L, 612L),
      bypassPlatforms = Set.empty
    )

    val asnContext = SelfDescribingData(
      IpLookupsEnrichment.asnSchema,
      json"""{
        "number": 9999,
        "organization": "Clean AS"
      }"""
    )

    enrichment.lookupAsn(asnContext, Some("web")) must beSome.like {
      case sdj =>
        (sdj.schema must beEqualTo(IpLookupsEnrichment.asnSchema)) and
          (sdj.data.hcursor.get[Long]("number") must beRight(9999L)) and
          (sdj.data.hcursor.get[Boolean]("likelyBot") must beRight(false)) and
          (sdj.data.hcursor.get[String]("organization") must beRight("Clean AS"))
    }
  }

  def e8 = {
    val enrichment = AsnLookupsEnrichment(
      botAsns = Set(174L),
      bypassPlatforms = Set.empty
    )

    val asnContext = SelfDescribingData(
      IpLookupsEnrichment.asnSchema,
      json"""{
        "number": 174,
        "organization": "Cogent Communications"
      }"""
    )

    val result = enrichment.lookupAsn(asnContext, None)

    result must beSome.like {
      case sdj =>
        sdj.data.hcursor.keys.toList.flatten must contain(allOf("number", "organization", "likelyBot"))
    }
  }

  def e9 = {
    val enrichment = AsnLookupsEnrichment(
      botAsns = Set(174L),
      bypassPlatforms = Set.empty
    )

    val asnContext = SelfDescribingData(
      IpLookupsEnrichment.asnSchema,
      json"""{
        "number": 174
      }"""
    )

    val result = enrichment.lookupAsn(asnContext, None)

    result must beSome.like {
      case sdj =>
        (sdj.data.hcursor.keys.toList.flatten must contain(allOf("number", "likelyBot"))) and
          (sdj.data.hcursor.keys.toList.flatten must not(contain("organization")))
    }
  }
}
