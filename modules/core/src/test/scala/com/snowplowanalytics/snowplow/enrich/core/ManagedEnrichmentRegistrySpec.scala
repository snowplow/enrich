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
package com.snowplowanalytics.snowplow.enrich.core

import scala.concurrent.ExecutionContext
import scala.util.matching.Regex

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import org.specs2.mutable.Specification

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf._
import com.snowplowanalytics.snowplow.enrich.common.utils.{HttpClient => CommonHttpClient}

class ManagedEnrichmentRegistrySpec extends Specification with CatsEffect {

  "ManagedEnrichmentRegistry" should {

    "route each non-asset conf type to its own field in the snapshot" in {
      val confs = List(
        ManagedEnrichmentRegistrySpec.cookieExtractorConf,
        ManagedEnrichmentRegistrySpec.yauaaConf,
        ManagedEnrichmentRegistrySpec.botDetectionConf,
        ManagedEnrichmentRegistrySpec.httpHeaderExtractorConf
      )
      ManagedEnrichmentRegistry
        .build[IO](confs, Nil, CommonHttpClient.noop[IO], ExecutionContext.global, false, Set.empty)
        .use {
          case Left(err) =>
            IO.raiseError(new RuntimeException(s"Build failed unexpectedly: $err"))
          case Right(registry) =>
            registry.snapshot.use { snap =>
              IO {
                (snap.cookieExtractor must beSome) and
                  (snap.yauaa must beSome) and
                  (snap.botDetection must beSome) and
                  (snap.httpHeaderExtractor must beSome) and
                  (snap.anonIp must beNone) and // not in conf list
                  (snap.crossNavigation must beNone) and // not in conf list
                  (snap.ipLookups must beNone) // asset-backed, not in conf list
              }
            }
        }
    }

    "produce an all-None snapshot when the conf list is empty" in {
      ManagedEnrichmentRegistry
        .build[IO](Nil, Nil, CommonHttpClient.noop[IO], ExecutionContext.global, false, Set.empty)
        .use {
          case Left(err) =>
            IO.raiseError(new RuntimeException(s"Build failed unexpectedly: $err"))
          case Right(registry) =>
            registry.snapshot.use { snap =>
              IO {
                (snap.cookieExtractor must beNone) and
                  (snap.yauaa must beNone) and
                  (snap.anonIp must beNone) and
                  (snap.ipLookups must beNone) and
                  (snap.iab must beNone) and
                  (snap.asnLookups must beNone) and
                  (snap.refererParser must beNone) and
                  (snap.uaParser must beNone) and
                  (snap.eventSpec must beNone) and
                  (snap.javascriptScript must beEmpty)
              }
            }
        }
    }
  }
}

object ManagedEnrichmentRegistrySpec {

  private val dummySchemaKey: SchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow", "test", "jsonschema", SchemaVer.Full(1, 0, 0))

  val cookieExtractorConf: CookieExtractorConf =
    CookieExtractorConf(dummySchemaKey, cookieNames = List("sp"))

  val yauaaConf: YauaaConf =
    YauaaConf(dummySchemaKey, cacheSize = None)

  val botDetectionConf: BotDetectionConf =
    BotDetectionConf(dummySchemaKey, useYauaa = false, useIab = false, useAsnLookups = false, useClientSideDetection = false)

  val httpHeaderExtractorConf: HttpHeaderExtractorConf =
    HttpHeaderExtractorConf(dummySchemaKey, headersPattern = new Regex(".*"))
}
