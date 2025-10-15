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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.pii

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import cats.syntax.option._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SelfDescribingData}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import io.circe.Json
import org.apache.commons.codec.digest.DigestUtils
import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers

class AnonymousOnlySpec extends Specification with ValidatedMatchers with CatsEffect {
  import AnonymousOnlySpec._
  import PiiPseudonymizerEnrichmentSpec._

  def is = s2"""
  Scalar fields should get hashed when anonymousOnly is true and SP-Anonymous header is set         $e1
  Scalar fields should not get hashed when anonymousOnly is true and SP-Anonymous header is not set $e2
  Scalar fields should get hashed when anonymousOnly is false and SP-Anonymous header is set        $e3
  Scalar fields should get hashed when anonymousOnly is false and SP-Anonymous header is not set    $e4
  Json fields should get hashed when anonymousOnly is true and SP-Anonymous header is set           $e5
  Json fields should not get hashed when anonymousOnly is true and SP-Anonymous header is not set   $e6
  Json fields should get hashed when anonymousOnly is false and SP-Anonymous header is set          $e7
  Json fields should get hashed when anonymousOnly is false and SP-Anonymous header is not set      $e8
  """

  def e1 = {
    val actual = for {
      ipLookup <- ipEnrichment
      enrichmentReg = EnrichmentRegistry[IO](
                        ipLookups = Some(ipLookup),
                        campaignAttribution = campaignAttributionEnrichment.some,
                        piiPseudonymizer = piiEnrichmentConfig(isScalar = true, anonymousOnly = true).some
                      )
      output <- commonSetup(enrichmentReg, List("SP-Anonymous: true"))
    } yield output

    actual.map { output =>
      val size = output.size must_== 1
      val validOut = output.head must beRight.like {
        case enrichedEvent =>
          (enrichedEvent.app_id must_== Hashed.app_id) and
            (enrichedEvent.geo_city must_== Hashed.geo_city) and
            (enrichedEvent.etl_tstamp must_== Hashed.etl_tstamp) and
            (enrichedEvent.collector_tstamp must_== Hashed.collector_tstamp) and
            (enrichedEvent.user_id must_== Hashed.user_id) and
            (enrichedEvent.user_ipaddress must_== Hashed.user_ipaddress) and
            (enrichedEvent.user_fingerprint must_== Hashed.user_fingerprint) and
            (enrichedEvent.domain_userid must_== Hashed.domain_userid) and
            (enrichedEvent.network_userid must_== Hashed.network_userid) and
            (enrichedEvent.ip_organization must_== Hashed.ip_organization) and
            (enrichedEvent.ip_domain must_== Hashed.ip_domain) and
            (enrichedEvent.tr_orderid must_== Hashed.tr_orderid) and
            (enrichedEvent.ti_orderid must_== Hashed.ti_orderid) and
            (enrichedEvent.mkt_term must_== Hashed.mkt_term) and
            (enrichedEvent.mkt_clickid must_== Hashed.mkt_clickid) and
            (enrichedEvent.mkt_content must_== Hashed.mkt_content) and
            (enrichedEvent.se_category must_== Hashed.se_category) and
            (enrichedEvent.se_action must_== Hashed.se_action) and
            (enrichedEvent.se_label must_== Hashed.se_label) and
            (enrichedEvent.se_property must_== Hashed.se_property) and
            (enrichedEvent.refr_domain_userid must_== Hashed.refr_domain_userid) and
            (enrichedEvent.domain_sessionid must_== Hashed.domain_sessionid)
      }
      size and validOut
    }
  }

  def e2 = {
    val actual = for {
      ipLookup <- ipEnrichment
      enrichmentReg = EnrichmentRegistry[IO](
                        ipLookups = Some(ipLookup),
                        campaignAttribution = campaignAttributionEnrichment.some,
                        piiPseudonymizer = piiEnrichmentConfig(isScalar = true, anonymousOnly = true).some
                      )
      output <- commonSetup(enrichmentReg)
    } yield output

    actual.map { output =>
      val size = output.size must_== 1
      val validOut = output.head must beRight.like {
        case enrichedEvent =>
          (enrichedEvent.app_id must_== NotHashed.app_id) and
            (enrichedEvent.geo_city must_== NotHashed.geo_city) and
            (enrichedEvent.etl_tstamp must_== NotHashed.etl_tstamp) and
            (enrichedEvent.collector_tstamp must_== NotHashed.collector_tstamp) and
            (enrichedEvent.user_id must_== NotHashed.user_id) and
            (enrichedEvent.user_ipaddress must_== NotHashed.user_ipaddress) and
            (enrichedEvent.user_fingerprint must_== NotHashed.user_fingerprint) and
            (enrichedEvent.domain_userid must_== NotHashed.domain_userid) and
            (enrichedEvent.network_userid must_== NotHashed.network_userid) and
            (enrichedEvent.ip_organization must_== NotHashed.ip_organization) and
            (enrichedEvent.ip_domain must_== NotHashed.ip_domain) and
            (enrichedEvent.tr_orderid must_== NotHashed.tr_orderid) and
            (enrichedEvent.ti_orderid must_== NotHashed.ti_orderid) and
            (enrichedEvent.mkt_term must_== NotHashed.mkt_term) and
            (enrichedEvent.mkt_clickid must_== NotHashed.mkt_clickid) and
            (enrichedEvent.mkt_content must_== NotHashed.mkt_content) and
            (enrichedEvent.se_category must_== NotHashed.se_category) and
            (enrichedEvent.se_action must_== NotHashed.se_action) and
            (enrichedEvent.se_label must_== NotHashed.se_label) and
            (enrichedEvent.se_property must_== NotHashed.se_property) and
            (enrichedEvent.refr_domain_userid must_== NotHashed.refr_domain_userid) and
            (enrichedEvent.domain_sessionid must_== NotHashed.domain_sessionid)
      }
      size and validOut
    }
  }

  def e3 = {
    val actual = for {
      ipLookup <- ipEnrichment
      enrichmentReg = EnrichmentRegistry[IO](
                        ipLookups = Some(ipLookup),
                        campaignAttribution = campaignAttributionEnrichment.some,
                        piiPseudonymizer = piiEnrichmentConfig(isScalar = true, anonymousOnly = false).some
                      )
      output <- commonSetup(enrichmentReg, List("SP-Anonymous: true"))
    } yield output

    actual.map { output =>
      val size = output.size must_== 1
      val validOut = output.head must beRight.like {
        case enrichedEvent =>
          (enrichedEvent.app_id must_== Hashed.app_id) and
            (enrichedEvent.geo_city must_== Hashed.geo_city) and
            (enrichedEvent.etl_tstamp must_== Hashed.etl_tstamp) and
            (enrichedEvent.collector_tstamp must_== Hashed.collector_tstamp) and
            (enrichedEvent.user_id must_== Hashed.user_id) and
            (enrichedEvent.user_ipaddress must_== Hashed.user_ipaddress) and
            (enrichedEvent.user_fingerprint must_== Hashed.user_fingerprint) and
            (enrichedEvent.domain_userid must_== Hashed.domain_userid) and
            (enrichedEvent.network_userid must_== Hashed.network_userid) and
            (enrichedEvent.ip_organization must_== Hashed.ip_organization) and
            (enrichedEvent.ip_domain must_== Hashed.ip_domain) and
            (enrichedEvent.tr_orderid must_== Hashed.tr_orderid) and
            (enrichedEvent.ti_orderid must_== Hashed.ti_orderid) and
            (enrichedEvent.mkt_term must_== Hashed.mkt_term) and
            (enrichedEvent.mkt_clickid must_== Hashed.mkt_clickid) and
            (enrichedEvent.mkt_content must_== Hashed.mkt_content) and
            (enrichedEvent.se_category must_== Hashed.se_category) and
            (enrichedEvent.se_action must_== Hashed.se_action) and
            (enrichedEvent.se_label must_== Hashed.se_label) and
            (enrichedEvent.se_property must_== Hashed.se_property) and
            (enrichedEvent.refr_domain_userid must_== Hashed.refr_domain_userid) and
            (enrichedEvent.domain_sessionid must_== Hashed.domain_sessionid)
      }
      size and validOut
    }
  }

  def e4 = {
    val actual = for {
      ipLookup <- ipEnrichment
      enrichmentReg = EnrichmentRegistry[IO](
                        ipLookups = Some(ipLookup),
                        campaignAttribution = campaignAttributionEnrichment.some,
                        piiPseudonymizer = piiEnrichmentConfig(isScalar = true, anonymousOnly = false).some
                      )
      output <- commonSetup(enrichmentReg)
    } yield output

    actual.map { output =>
      val size = output.size must_== 1
      val validOut = output.head must beRight.like {
        case enrichedEvent =>
          (enrichedEvent.app_id must_== Hashed.app_id) and
            (enrichedEvent.geo_city must_== Hashed.geo_city) and
            (enrichedEvent.etl_tstamp must_== Hashed.etl_tstamp) and
            (enrichedEvent.collector_tstamp must_== Hashed.collector_tstamp) and
            (enrichedEvent.user_id must_== Hashed.user_id) and
            (enrichedEvent.user_ipaddress must_== Hashed.user_ipaddress) and
            (enrichedEvent.user_fingerprint must_== Hashed.user_fingerprint) and
            (enrichedEvent.domain_userid must_== Hashed.domain_userid) and
            (enrichedEvent.network_userid must_== Hashed.network_userid) and
            (enrichedEvent.ip_organization must_== Hashed.ip_organization) and
            (enrichedEvent.ip_domain must_== Hashed.ip_domain) and
            (enrichedEvent.tr_orderid must_== Hashed.tr_orderid) and
            (enrichedEvent.ti_orderid must_== Hashed.ti_orderid) and
            (enrichedEvent.mkt_term must_== Hashed.mkt_term) and
            (enrichedEvent.mkt_clickid must_== Hashed.mkt_clickid) and
            (enrichedEvent.mkt_content must_== Hashed.mkt_content) and
            (enrichedEvent.se_category must_== Hashed.se_category) and
            (enrichedEvent.se_action must_== Hashed.se_action) and
            (enrichedEvent.se_label must_== Hashed.se_label) and
            (enrichedEvent.se_property must_== Hashed.se_property) and
            (enrichedEvent.refr_domain_userid must_== Hashed.refr_domain_userid) and
            (enrichedEvent.domain_sessionid must_== Hashed.domain_sessionid)
      }
      size and validOut
    }
  }

  def e5 = {
    val actual = for {
      ipLookup <- ipEnrichment
      enrichmentReg = EnrichmentRegistry[IO](
                        ipLookups = Some(ipLookup),
                        javascriptScript = jsEnrichment,
                        piiPseudonymizer = piiEnrichmentConfig(isScalar = false, anonymousOnly = true).some
                      )
      output <- commonSetup(enrichmentReg, List("SP-Anonymous: true"))
    } yield output

    actual.map { output =>
      val size = output.size must_== 1
      val validOut = output.head must beRight.like {
        case enrichedEvent =>
          val testFirstContext = enrichedEvent.contexts.lift(0) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("emailAddress") must beRight(
                  "72f323d5359eabefc69836369e4cabc6257c43ab6419b05dfb2211d0e44284c6"
                )),
                (sdj.data.hcursor.get[String]("emailAddress2") must beRight("bob@acme.com"))
              ).reduce(_ and _)
          }

          val testSecondContext = enrichedEvent.contexts.lift(1) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("emailAddress") must beRight("tim@acme.com")),
                (sdj.data.hcursor.get[String]("emailAddress2") must beRight("tom@acme.com")),
                (sdj.data.hcursor.get[Json]("data") must beRight.like {
                  case json: Json =>
                    json.hcursor.get[String]("emailAddress2") must beRight(
                      "1c6660411341411d5431669699149283d10e070224be4339d52bbc4b007e78c5"
                    )
                }),
                (sdj.data.hcursor.get[String]("schema") must beRight("iglu:com.acme/email_sent/jsonschema/1-0-0"))
              ).reduce(_ and _)
          }

          val testUnstructEvent = enrichedEvent.unstruct_event must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("ip") must beRight("269c433d0cc00395e3bc5fe7f06c5ad822096a38bec2d8a005367b52c0dfb428")),
                (sdj.data.hcursor.get[String]("myVar2") must beRight("awesome"))
              ).reduce(_ and _)
          }

          val testThirdContext = enrichedEvent.contexts.lift(2) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[List[String]]("field") must beRight(
                  List[String]("b62f3a2475ac957009088f9b8ab77ceb7b4ed7c5a6fd920daa204a1953334acb",
                               "8ad32723b7435cbf535025e519cc94dbf1568e17ced2aeb4b9e7941f6346d7d0"
                  )
                )),
                (sdj.data.hcursor.downField("field2").focus must beSome.like { case json => json.isNull }),
                (sdj.data.hcursor.downField("field3").focus must beSome.like { case json => json.isNull }),
                (sdj.data.hcursor.get[String]("field4") must beRight("7a3477dad66e666bd203b834c54b6dfe8b546bdbc5283462ad14052abfb06600"))
              ).reduce(_ and _)
          }

          val testDerivedContext = enrichedEvent.derived_contexts.lift(0) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("emailAddress") must beRight(
                  "72f323d5359eabefc69836369e4cabc6257c43ab6419b05dfb2211d0e44284c6"
                )),
                (sdj.data.hcursor.get[String]("emailAddress2") must beRight("bob@acme.com"))
              ).reduce(_ and _)
          }

          testFirstContext and testSecondContext and testThirdContext and testUnstructEvent and testDerivedContext
      }
      size and validOut
    }
  }

  def e6 = {
    val actual = for {
      ipLookup <- ipEnrichment
      enrichmentReg = EnrichmentRegistry[IO](
                        ipLookups = Some(ipLookup),
                        javascriptScript = jsEnrichment,
                        piiPseudonymizer = piiEnrichmentConfig(isScalar = false, anonymousOnly = true).some
                      )
      output <- commonSetup(enrichmentReg)
    } yield output

    actual.map { output =>
      val size = output.size must_== 1
      val validOut = output.head must beRight.like {
        case enrichedEvent =>
          val testFirstContext = enrichedEvent.contexts.lift(0) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("emailAddress") must beRight(
                  "jim@acme.com"
                )),
                (sdj.data.hcursor.get[String]("emailAddress2") must beRight("bob@acme.com"))
              ).reduce(_ and _)
          }

          val testSecondContext = enrichedEvent.contexts.lift(1) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("emailAddress") must beRight("tim@acme.com")),
                (sdj.data.hcursor.get[String]("emailAddress2") must beRight("tom@acme.com")),
                (sdj.data.hcursor.get[Json]("data") must beRight.like {
                  case json: Json =>
                    json.hcursor.get[String]("emailAddress2") must beRight("bob@acme.com")
                }),
                (sdj.data.hcursor.get[String]("schema") must beRight("iglu:com.acme/email_sent/jsonschema/1-0-0"))
              ).reduce(_ and _)
          }

          val testUnstructEvent = enrichedEvent.unstruct_event must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("ip") must beRight("50.56.129.169")),
                (sdj.data.hcursor.get[String]("myVar2") must beRight("awesome"))
              ).reduce(_ and _)
          }

          val testThirdContext = enrichedEvent.contexts.lift(2) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[List[String]]("field") must beRight(List[String]("hello", "world"))),
                (sdj.data.hcursor.downField("field2").focus must beSome.like { case json => json.isNull }),
                (sdj.data.hcursor.downField("field3").focus must beSome.like { case json => json.isNull }),
                (sdj.data.hcursor.get[String]("field4") must beRight(""))
              ).reduce(_ and _)
          }

          val testDerivedContext = enrichedEvent.derived_contexts.lift(0) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("emailAddress") must beRight(
                  "jim@acme.com"
                )),
                (sdj.data.hcursor.get[String]("emailAddress2") must beRight("bob@acme.com"))
              ).reduce(_ and _)
          }

          testFirstContext and testSecondContext and testThirdContext and testUnstructEvent and testDerivedContext
      }
      size and validOut
    }
  }

  def e7 = {
    val actual = for {
      ipLookup <- ipEnrichment
      enrichmentReg = EnrichmentRegistry[IO](
                        ipLookups = Some(ipLookup),
                        javascriptScript = jsEnrichment,
                        piiPseudonymizer = piiEnrichmentConfig(isScalar = false, anonymousOnly = false).some
                      )
      output <- commonSetup(enrichmentReg, List("SP-Anonymous: true"))
    } yield output

    actual.map { output =>
      val size = output.size must_== 1
      val validOut = output.head must beRight.like {
        case enrichedEvent =>
          val testFirstContext = enrichedEvent.contexts.lift(0) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("emailAddress") must beRight(
                  "72f323d5359eabefc69836369e4cabc6257c43ab6419b05dfb2211d0e44284c6"
                )),
                (sdj.data.hcursor.get[String]("emailAddress2") must beRight("bob@acme.com"))
              ).reduce(_ and _)
          }

          val testSecondContext = enrichedEvent.contexts.lift(1) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("emailAddress") must beRight("tim@acme.com")),
                (sdj.data.hcursor.get[String]("emailAddress2") must beRight("tom@acme.com")),
                (sdj.data.hcursor.get[Json]("data") must beRight.like {
                  case json: Json =>
                    List(
                      (json.hcursor.get[String]("emailAddress") must beRight("jim@acme.com")),
                      (json.hcursor.get[String]("emailAddress2") must beRight(
                        "1c6660411341411d5431669699149283d10e070224be4339d52bbc4b007e78c5"
                      ))
                    ).reduce(_ and _)
                }),
                (sdj.data.hcursor.get[String]("schema") must beRight("iglu:com.acme/email_sent/jsonschema/1-0-0"))
              ).reduce(_ and _)
          }

          val testUnstructEvent = enrichedEvent.unstruct_event must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("ip") must beRight("269c433d0cc00395e3bc5fe7f06c5ad822096a38bec2d8a005367b52c0dfb428")),
                (sdj.data.hcursor.get[String]("myVar2") must beRight("awesome"))
              ).reduce(_ and _)
          }

          val testThirdContext = enrichedEvent.contexts.lift(2) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[List[String]]("field") must beRight(
                  List[String]("b62f3a2475ac957009088f9b8ab77ceb7b4ed7c5a6fd920daa204a1953334acb",
                               "8ad32723b7435cbf535025e519cc94dbf1568e17ced2aeb4b9e7941f6346d7d0"
                  )
                )),
                (sdj.data.hcursor.downField("field2").focus must beSome.like { case json => json.isNull }),
                (sdj.data.hcursor.downField("field3").focus must beSome.like { case json => json.isNull }),
                (sdj.data.hcursor.get[String]("field4") must beRight("7a3477dad66e666bd203b834c54b6dfe8b546bdbc5283462ad14052abfb06600"))
              ).reduce(_ and _)
          }

          val testDerivedContext = enrichedEvent.derived_contexts.lift(0) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("emailAddress") must beRight(
                  "72f323d5359eabefc69836369e4cabc6257c43ab6419b05dfb2211d0e44284c6"
                )),
                (sdj.data.hcursor.get[String]("emailAddress2") must beRight("bob@acme.com"))
              ).reduce(_ and _)
          }

          testFirstContext and testSecondContext and testThirdContext and testUnstructEvent and testDerivedContext
      }
      size and validOut
    }
  }

  def e8 = {
    val actual = for {
      ipLookup <- ipEnrichment
      enrichmentReg = EnrichmentRegistry[IO](
                        ipLookups = Some(ipLookup),
                        javascriptScript = jsEnrichment,
                        piiPseudonymizer = piiEnrichmentConfig(isScalar = false, anonymousOnly = false).some
                      )
      output <- commonSetup(enrichmentReg)
    } yield output

    actual.map { output =>
      val size = output.size must_== 1
      val validOut = output.head must beRight.like {
        case enrichedEvent =>
          val testFirstContext = enrichedEvent.contexts.lift(0) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("emailAddress") must beRight(
                  "72f323d5359eabefc69836369e4cabc6257c43ab6419b05dfb2211d0e44284c6"
                )),
                (sdj.data.hcursor.get[String]("emailAddress2") must beRight("bob@acme.com"))
              ).reduce(_ and _)
          }

          val testSecondContext = enrichedEvent.contexts.lift(1) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("emailAddress") must beRight("tim@acme.com")),
                (sdj.data.hcursor.get[String]("emailAddress2") must beRight("tom@acme.com")),
                (sdj.data.hcursor.get[Json]("data") must beRight.like {
                  case json: Json =>
                    List(
                      json.hcursor.get[String]("emailAddress") must beRight("jim@acme.com"),
                      json.hcursor.get[String]("emailAddress2") must beRight(
                        "1c6660411341411d5431669699149283d10e070224be4339d52bbc4b007e78c5"
                      )
                    ).reduce(_ and _)
                }),
                (sdj.data.hcursor.get[String]("schema") must beRight("iglu:com.acme/email_sent/jsonschema/1-0-0"))
              ).reduce(_ and _)
          }

          val testUnstructEvent = enrichedEvent.unstruct_event must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("ip") must beRight("269c433d0cc00395e3bc5fe7f06c5ad822096a38bec2d8a005367b52c0dfb428")),
                (sdj.data.hcursor.get[String]("myVar2") must beRight("awesome"))
              ).reduce(_ and _)
          }

          val testThirdContext = enrichedEvent.contexts.lift(2) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[List[String]]("field") must beRight(
                  List[String]("b62f3a2475ac957009088f9b8ab77ceb7b4ed7c5a6fd920daa204a1953334acb",
                               "8ad32723b7435cbf535025e519cc94dbf1568e17ced2aeb4b9e7941f6346d7d0"
                  )
                )),
                (sdj.data.hcursor.downField("field2").focus must beSome.like { case json => json.isNull }),
                (sdj.data.hcursor.downField("field3").focus must beSome.like { case json => json.isNull }),
                (sdj.data.hcursor.get[String]("field4") must beRight("7a3477dad66e666bd203b834c54b6dfe8b546bdbc5283462ad14052abfb06600"))
              ).reduce(_ and _)
          }

          val testDerivedContext = enrichedEvent.derived_contexts.lift(0) must beSome.like {
            case sdj: SelfDescribingData[Json] =>
              List(
                (sdj.data.hcursor.get[String]("emailAddress") must beRight(
                  "72f323d5359eabefc69836369e4cabc6257c43ab6419b05dfb2211d0e44284c6"
                )),
                (sdj.data.hcursor.get[String]("emailAddress2") must beRight("bob@acme.com"))
              ).reduce(_ and _)
          }

          testFirstContext and testSecondContext and testThirdContext and testUnstructEvent and testDerivedContext
      }
      size and validOut
    }
  }
}

object AnonymousOnlySpec {
  object Hashed {
    val app_id = "ads"
    val geo_city = null
    val etl_tstamp = "1970-01-18 08:40:00.000"
    val collector_tstamp = "2017-07-14 03:39:39.000"
    val user_id = "7d8a4beae5bc9d314600667d2f410918f9af265017a6ade99f60a9c8f3aac6e9"
    val user_ipaddress = "dd9720903c89ae891ed5c74bb7a9f2f90f6487927ac99afe73b096ad0287f3f5"
    val user_fingerprint = "27abac60dff12792c6088b8d00ce7f25c86b396b8c3740480cd18e21068ecff4"
    val domain_userid = "e97d86d49b16397e8fd654b32a0ed03cfe3a4d8d867d913620ce08e3ca855d6d"
    val network_userid = "47453d3c4428207d22005463bb3d945b137f9342d445b7114776e88311bbe648"
    val ip_organization = "4d5dd7eebeb9d47f9ebff5993502c0380a110c34711ef5062fdb84a563759f3b"
    val ip_domain = null
    val tr_orderid = "5139219b15f3d1ab0c5056296cf5246eeb0b934ee5d1c96cb2027e694005bbce"
    val ti_orderid = "326c0bfc5857f21695406ebd93068341c9f2d975cf00d117479e01e9012e196c"
    val mkt_term = "b62f3a2475ac957009088f9b8ab77ceb7b4ed7c5a6fd920daa204a1953334acb"
    val mkt_clickid = "fae3733fa03cdf57d82e89ac63026afd8782d07ba3c918acb415a4343457785f"
    val mkt_content = "8ad32723b7435cbf535025e519cc94dbf1568e17ced2aeb4b9e7941f6346d7d0"
    val se_category = "f33daec1ed4cb688f4f1762390735fd78f6a06083f855422a7303ed63707c962"
    val se_action = "53f3e1ca4a0dccce4a1b2900a6bcfd21b22a0f444253067e2fe022948a0b3be7"
    val se_label = "b243defc0d3b86333a104fb2b3a2f43371b8d73359c429b9177dfc5bb3840efd"
    val se_property = "eb19004c52cd4557aacfa0b30035160c417c3a6a5fad44b96f03c9e2bebaf0b3"
    val refr_domain_userid = "f3e68fd96eaef0cafc1257ec7132b4b3dbae20b1073155531f909999e5da9b2c"
    val domain_sessionid = "7378a72b0183f456df98453b2ff9ed5685206a67f312edb099dc74aed76e1b34"
  }

  object NotHashed {
    val app_id = "ads"
    val geo_city = null
    val etl_tstamp = "1970-01-18 08:40:00.000"
    val collector_tstamp = "2017-07-14 03:39:39.000"
    val user_id = "john@acme.com"
    val user_ipaddress = "70.46.123.145"
    val user_fingerprint = "its_you_again!"
    val domain_userid = "786d1b69-a603-4eb8-9178-fed2a195a1ed"
    val network_userid = "kuykyfkfykukfuy"
    val ip_organization = "DSLAM WAN Allocation"
    val ip_domain = null
    val tr_orderid = "t5465463"
    val ti_orderid = "6546b56356b354bbv"
    val mkt_term = "hello"
    val mkt_clickid = "500"
    val mkt_content = "world"
    val se_category = "super category"
    val se_action = "great action"
    val se_label = "awesome label"
    val se_property = "good property"
    val refr_domain_userid = "duid"
    val domain_sessionid = "87857856-a603-4eb8-9178-fed2a195a1ed"
  }

  val jsonFieldMutators: PiiMutators = {
    val contexts = List(
      PiiPseudonymizerEnrichment.JsonFieldLocator(
        schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0),
        jsonPath = "$.emailAddress"
      ),
      PiiPseudonymizerEnrichment.JsonFieldLocator(
        schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 1, 0),
        jsonPath = "$.data.emailAddress2"
      ),
      PiiPseudonymizerEnrichment.JsonFieldLocator(
        schemaCriterion = SchemaCriterion("com.test", "array", "jsonschema", 1, 0, 0),
        jsonPath = "$.field"
      ),
      PiiPseudonymizerEnrichment.JsonFieldLocator(
        schemaCriterion = SchemaCriterion("com.test", "array", "jsonschema", 1, 0, 0),
        jsonPath = "$.field2"
      ),
      PiiPseudonymizerEnrichment.JsonFieldLocator(
        schemaCriterion = SchemaCriterion("com.test", "array", "jsonschema", 1, 0, 0),
        jsonPath = "$.field3.a"
      ),
      PiiPseudonymizerEnrichment.JsonFieldLocator(
        schemaCriterion = SchemaCriterion("com.test", "array", "jsonschema", 1, 0, 0),
        jsonPath = "$.field4"
      )
    )

    val unstructs = List(
      PiiPseudonymizerEnrichment.JsonFieldLocator(
        schemaCriterion = SchemaCriterion("com.mailgun", "message_clicked", "jsonschema", 1, 0, 0),
        jsonPath = "$.ip"
      )
    )

    val derivedContexts = List(
      PiiPseudonymizerEnrichment.JsonFieldLocator(
        schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0),
        jsonPath = "$.emailAddress"
      )
    )

    PiiMutators(pojo = Nil, unstruct = unstructs, contexts = contexts, derivedContexts = derivedContexts)
  }

  val scalarFieldMutators: PiiMutators = {
    val mutators = List(
      ScalarMutators.byFieldName("user_id"),
      ScalarMutators.byFieldName("user_ipaddress"),
      ScalarMutators.byFieldName("user_fingerprint"),
      ScalarMutators.byFieldName("domain_userid"),
      ScalarMutators.byFieldName("network_userid"),
      ScalarMutators.byFieldName("ip_organization"),
      ScalarMutators.byFieldName("ip_domain"),
      ScalarMutators.byFieldName("tr_orderid"),
      ScalarMutators.byFieldName("ti_orderid"),
      ScalarMutators.byFieldName("mkt_term"),
      ScalarMutators.byFieldName("mkt_clickid"),
      ScalarMutators.byFieldName("mkt_content"),
      ScalarMutators.byFieldName("se_category"),
      ScalarMutators.byFieldName("se_action"),
      ScalarMutators.byFieldName("se_label"),
      ScalarMutators.byFieldName("se_property"),
      ScalarMutators.byFieldName("refr_domain_userid"),
      ScalarMutators.byFieldName("domain_sessionid")
    )
    PiiMutators(mutators, Nil, Nil, Nil)
  }

  def piiEnrichmentConfig(isScalar: Boolean, anonymousOnly: Boolean): PiiPseudonymizerEnrichment =
    PiiPseudonymizerEnrichment(
      if (isScalar) scalarFieldMutators else jsonFieldMutators,
      emitIdentificationEvent = false,
      PiiStrategyPseudonymize(
        "SHA-256",
        hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
        "pepper123"
      ),
      anonymousOnly = anonymousOnly
    )
}
