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
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import java.net.URI

import org.specs2.Specification

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect

import io.circe.Json
import io.circe.literal._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.utils.ConversionUtils
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers

class EnrichmentRegistrySpec extends Specification with CatsEffect {
  import EnrichmentRegistrySpec._

  def is = s2"""
  EnrichmentRegistry should parse array of enrichments without any JS enrichment correctly $noJSEnrichment
  EnrichmentRegistry should parse array of enrichments with single JS enrichment correctly $singleJSEnrichment
  EnrichmentRegistry should parse array of enrichments with multiple JS enrichments correctly $multipleJSEnrichments
  EnrichmentRegistry should parse JS enrichment with config field correctly $jsEnrichmentWithConfig
  EnrichmentRegistry should parse IP lookup enrichment correctly $ipLookupEnrichment
  """

  def noJSEnrichment =
    EnrichmentRegistry
      .parse[IO](
        enrichmentConfig(),
        SpecHelpers.client,
        localMode = false,
        SpecHelpers.registryLookup
      )
      .map { res =>
        val jsConfs = res.getOrElse(List.empty).filter {
          case _: EnrichmentConf.JavascriptScriptConf => true
          case _ => false
        }
        jsConfs.size must beEqualTo(0)
      }

  def singleJSEnrichment = {
    val jsEnrichments = List(jsEnrichment())
    EnrichmentRegistry
      .parse[IO](
        enrichmentConfig(jsEnrichments),
        SpecHelpers.client,
        localMode = false,
        SpecHelpers.registryLookup
      )
      .map { res =>
        val jsConfs = res.getOrElse(List.empty).filter {
          case _: EnrichmentConf.JavascriptScriptConf => true
          case _ => false
        }
        jsConfs.size must beEqualTo(1)
      }
  }

  def multipleJSEnrichments = {
    val jsReturns = List("return1", "return2")
    val jsEnrichments = jsReturns.map(jsEnrichment(_))
    EnrichmentRegistry
      .parse[IO](
        enrichmentConfig(jsEnrichments),
        SpecHelpers.client,
        localMode = false,
        SpecHelpers.registryLookup
      )
      .map { res =>
        val jsConfs = res.getOrElse(List.empty).flatMap {
          case e: EnrichmentConf.JavascriptScriptConf => Some(e)
          case _ => None
        }
        jsReturns.zip(jsConfs).forall {
          case (jsReturn, jsConf) => jsConf.rawFunction should contain(jsReturn)
        }
      }
  }

  def jsEnrichmentWithConfig = {
    val jsEnrichments = List(jsEnrichment(addConfig = true))
    EnrichmentRegistry
      .parse[IO](
        enrichmentConfig(jsEnrichments),
        SpecHelpers.client,
        localMode = false,
        SpecHelpers.registryLookup
      )
      .map { res =>
        val jsConfs = res.getOrElse(List.empty).filter {
          case _: EnrichmentConf.JavascriptScriptConf => true
          case _ => false
        }
        jsConfs.size must beEqualTo(1)
      }
  }

  def ipLookupEnrichment =
    EnrichmentRegistry
      .parse[IO](
        enrichmentConfig(),
        SpecHelpers.client,
        localMode = false,
        SpecHelpers.registryLookup
      )
      .map { res =>
        val result = res.getOrElse(List.empty).filter {
          case _: EnrichmentConf.IpLookupsConf => true
          case _ => false
        }
        val schemaKey = SchemaKey(
          "com.snowplowanalytics.snowplow",
          "ip_lookups",
          "jsonschema",
          SchemaVer.Full(2, 0, 1)
        )
        val expected = EnrichmentConf.IpLookupsConf(
          schemaKey,
          Some(
            (
              new URI(
                "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind/GeoIP2-City.mmdb"
              ),
              "./ip_geo"
            )
          ),
          Some(
            (
              new URI(
                "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind/GeoIP2-ISP.mmdb"
              ),
              "./ip_isp"
            )
          ),
          Some(
            (
              new URI(
                "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind/GeoIP2-Domain.mmdb"
              ),
              "./ip_domain"
            )
          ),
          Some(
            (
              new URI(
                "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind/GeoIP2-Connection-Type.mmdb"
              ),
              "./ip_connectionType"
            )
          ),
          Some(
            (
              new URI(
                "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind/GeoLite2-ASN.mmdb"
              ),
              "./ip_asn"
            )
          )
        )
        result must beEqualTo(List(expected))
      }
}

object EnrichmentRegistrySpec {

  def jsEnrichment(jsReturn: String = "defaultReturn", addConfig: Boolean = false): Json = {
    val script = s"""
      function process(event) {
        return $jsReturn;
      }
    """

    val config = json"""{
      "schema": "iglu:com.snowplowanalytics.snowplow/javascript_script_config/jsonschema/1-0-1",
      "data": {
        "parameters": {
          "config": {
            "foo": 3,
            "nested": {
              "bar": 42
            }
          }
        }
      }
    }"""

    val jsEnrichment = json"""{
      "schema": "iglu:com.snowplowanalytics.snowplow/javascript_script_config/jsonschema/1-0-0",
      "data": {
        "vendor": "com.snowplowanalytics.snowplow",
        "name": "javascript_script_config",
        "enabled": true,
        "parameters": {
          "script": ${ConversionUtils.encodeBase64Url(script)}
        }
      }
    }"""
    if (addConfig) jsEnrichment.deepMerge(config) else jsEnrichment
  }

  // Vendor and name are intentionally tweaked in the first enrichment
  // to test that we are no longer validating them (users were confused about such validation)
  def enrichmentConfig(additionals: List[Json] = List.empty) = {
    val enrichmentArr = json"""[
      {
        "schema": "iglu:com.snowplowanalytics.snowplow/anon_ip/jsonschema/1-0-0",
        "data": {
          "vendor": "com.snowplowanalytics.snowplow_custom",
          "name": "anon_ip_custom",
          "enabled": true,
          "parameters": {
            "anonOctets": 1
          }
        }
      },
      {
        "schema": "iglu:com.snowplowanalytics.snowplow/campaign_attribution/jsonschema/1-0-0",
        "data": {
          "vendor": "com.snowplowanalytics.snowplow",
          "name": "campaign_attribution",
          "enabled": true,
          "parameters": {
            "mapping": "static",
            "fields": {
            "mktMedium": ["utm_medium", "medium"],
            "mktSource": ["utm_source", "source"],
            "mktTerm": ["utm_term", "legacy_term"],
            "mktContent": ["utm_content"],
            "mktCampaign": ["utm_campaign", "cid", "legacy_campaign"]
            }
          }
        }
      },
      {
        "schema": "iglu:com.snowplowanalytics.snowplow/user_agent_utils_config/jsonschema/1-0-0",
        "data": {
          "vendor": "com.snowplowanalytics.snowplow",
          "name": "user_agent_utils_config",
          "enabled": true,
            "parameters": {}
        }
      },
      {
        "schema": "iglu:com.snowplowanalytics.snowplow/referer_parser/jsonschema/2-0-0",
        "data": {
          "vendor": "com.snowplowanalytics.snowplow",
          "name": "referer_parser",
          "enabled": true,
          "parameters": {
            "internalDomains": ["www.subdomain1.snowplowanalytics.com"],
            "database": "referer-tests.json",
            "uri": "http://snowplow.com"
          }
        }
      },
      {
        "schema": "iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-1",
        "data": {
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
            },
            "domain": {
              "database": "GeoIP2-Domain.mmdb",
              "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
            },
            "connectionType": {
              "database": "GeoIP2-Connection-Type.mmdb",
              "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
            },
            "asn": {
              "database": "GeoLite2-ASN.mmdb",
              "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
            }
          }
        }
      }
    ]""".asArray.map(_.toList).getOrElse(List.empty) ::: additionals
    json"""{
      "schema": "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0",
      "data": $enrichmentArr
    }"""
  }
}
