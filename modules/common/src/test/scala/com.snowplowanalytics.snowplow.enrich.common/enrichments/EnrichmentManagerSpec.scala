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

import org.apache.commons.codec.digest.DigestUtils

import org.specs2.mutable.Specification
import org.specs2.matcher.{EitherMatchers, MatchResult}

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect

import cats.implicits._
import cats.data.NonEmptyList

import io.circe.Json
import io.circe.literal._
import io.circe.parser.{parse => jparse}
import io.circe.syntax._

import org.joda.time.DateTime

import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.{Failure => BadRowFailure}

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.enrich.common.QueryStringParameters
import com.snowplowanalytics.snowplow.enrich.common.loaders._
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.pii.{
  PiiMutators,
  PiiPseudonymizerEnrichment,
  PiiStrategyPseudonymize
}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.{AtomicError, OptionIor}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.{
  CrossNavigationEnrichment,
  HttpHeaderExtractorEnrichment,
  IabEnrichment,
  JavascriptScriptEnrichment,
  YauaaEnrichment
}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.JavascriptScriptEnrichmentSpec.createJsEnrichment
import com.snowplowanalytics.snowplow.enrich.common.AcceptInvalid
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class EnrichmentManagerSpec extends Specification with EitherMatchers with CatsEffect {
  import EnrichmentManagerSpec._

  "enrichEvent" should {

    "set all atomic fields from tracker payload fields" >> {

      val tests: List[(String, String, EnrichedEvent => MatchResult[Any])] = List(
        ("e", "pp", _.event must beEqualTo("page_ping")),
        ("ip", "192.168.0.1", _.user_ipaddress must beEqualTo("192.168.0.1")),
        ("aid", "myapp", _.app_id must beEqualTo("myapp")),
        ("p", "web", _.platform must beEqualTo("web")),
        ("tid", "42", _.txn_id must beEqualTo(42)),
        ("uid", "myuser", _.user_id must beEqualTo("myuser")),
        ("duid", "myduid", _.domain_userid must beEqualTo("myduid")),
        ("nuid", "mynuid", _.network_userid must beEqualTo("mynuid")),
        ("ua", "myua", _.useragent must beEqualTo("myua")),
        ("fp", "myfp", _.user_fingerprint must beEqualTo("myfp")),
        ("vid", "42", _.domain_sessionidx must beEqualTo(42)),
        ("sid", "6e6e6d0f-f191-4249-8ac8-469db39332a3", _.domain_sessionid must beEqualTo("6e6e6d0f-f191-4249-8ac8-469db39332a3")),
        ("dtm", "1735693323000", _.dvce_created_tstamp must beEqualTo("2025-01-01 01:02:03.000")),
        ("ttm", "1735693323000", _.true_tstamp must beEqualTo("2025-01-01 01:02:03.000")),
        ("stm", "1735693323000", _.dvce_sent_tstamp must beEqualTo("2025-01-01 01:02:03.000")),
        ("tna", "mytna", _.name_tracker must beEqualTo("mytna")),
        ("tv", "mytv", _.v_tracker must beEqualTo("mytv")),
        ("cv", "mycv", _.v_collector must beEqualTo("mycv")),
        ("lang", "mylang", _.br_lang must beEqualTo("mylang")),
        ("f_pdf", "1", _.br_features_pdf must beEqualTo(1)),
        ("f_fla", "1", _.br_features_flash must beEqualTo(1)),
        ("f_java", "1", _.br_features_java must beEqualTo(1)),
        ("f_dir", "1", _.br_features_director must beEqualTo(1)),
        ("f_qt", "1", _.br_features_quicktime must beEqualTo(1)),
        ("f_realp", "1", _.br_features_realplayer must beEqualTo(1)),
        ("f_wma", "1", _.br_features_windowsmedia must beEqualTo(1)),
        ("f_gears", "1", _.br_features_gears must beEqualTo(1)),
        ("f_ag", "1", _.br_features_silverlight must beEqualTo(1)),
        ("cookie", "1", _.br_cookies must beEqualTo(1)),
        ("res", "42x420", e => (e.dvce_screenwidth must beEqualTo(42)) and (e.dvce_screenheight must beEqualTo(420))),
        ("cd", "mycd", _.br_colordepth must beEqualTo("mycd")),
        ("tz", "mytz", _.os_timezone must beEqualTo("mytz")),
        ("refr", "myrefr", _.page_referrer must beEqualTo("myrefr")),
        ("url", "myurl", _.page_url must beEqualTo("myurl")),
        ("page", "mypage", _.page_title must beEqualTo("mypage")),
        ("cs", "mycs", _.doc_charset must beEqualTo("mycs")),
        ("ds", "42x420", e => (e.doc_width must beEqualTo(42)) and (e.doc_height must beEqualTo(420))),
        ("vp", "42x420", e => (e.br_viewwidth must beEqualTo(42)) and (e.br_viewheight must beEqualTo(420))),
        ("eid", "a741bd4c-2365-4c35-92fa-00fe62d71b8d", _.event_id must beEqualTo("a741bd4c-2365-4c35-92fa-00fe62d71b8d")),
        ("ev_ca", "myval", _.se_category must beEqualTo("myval")),
        ("ev_ac", "myval", _.se_action must beEqualTo("myval")),
        ("ev_la", "myval", _.se_label must beEqualTo("myval")),
        ("ev_pr", "myval", _.se_property must beEqualTo("myval")),
        ("ev_va", "42", _.se_value.toString must beEqualTo("42")),
        ("se_ca", "myval", _.se_category must beEqualTo("myval")),
        ("se_ac", "myval", _.se_action must beEqualTo("myval")),
        ("se_la", "myval", _.se_label must beEqualTo("myval")),
        ("se_pr", "myval", _.se_property must beEqualTo("myval")),
        ("se_va", "42", _.se_value.toString must beEqualTo("42")),
        ("tr_id", "myval", _.tr_orderid must beEqualTo("myval")),
        ("tr_af", "myval", _.tr_affiliation must beEqualTo("myval")),
        ("tr_tt", "42", _.tr_total.toString must beEqualTo("42")),
        ("tr_tx", "42", _.tr_tax.toString must beEqualTo("42")),
        ("tr_sh", "42", _.tr_shipping.toString must beEqualTo("42")),
        ("tr_ci", "mycity", _.tr_city must beEqualTo("mycity")),
        ("tr_st", "mystate", _.tr_state must beEqualTo("mystate")),
        ("tr_co", "myhome", _.tr_country must beEqualTo("myhome")),
        ("ti_id", "myorder", _.ti_orderid must beEqualTo("myorder")),
        ("ti_sk", "mysku", _.ti_sku must beEqualTo("mysku")),
        ("ti_na", "myname", _.ti_name must beEqualTo("myname")),
        ("ti_nm", "myname2", _.ti_name must beEqualTo("myname2")),
        ("ti_ca", "mycat", _.ti_category must beEqualTo("mycat")),
        ("ti_pr", "42", _.ti_price.toString must beEqualTo("42")),
        ("ti_qu", "42", _.ti_quantity must beEqualTo(42)),
        ("pp_mix", "42", _.pp_xoffset_min must beEqualTo(42)),
        ("pp_max", "42", _.pp_xoffset_max must beEqualTo(42)),
        ("pp_miy", "42", _.pp_yoffset_min must beEqualTo(42)),
        ("pp_may", "42", _.pp_yoffset_max must beEqualTo(42)),
        ("tr_cu", "XYZ", _.tr_currency must beEqualTo("XYZ")),
        ("ti_cu", "XYZ", _.ti_currency must beEqualTo("XYZ")),
        ("tnuid", "mytnuid", _.network_userid must beEqualTo("mytnuid"))
      )

      tests.traverse {
        case (rawKey, rawValue, checkFn) =>
          val parameters = Map(
            "e" -> "pp",
            "tv" -> "js-0.13.1",
            "p" -> "web",
            rawKey -> rawValue
          ).toOpt
          val rawEvent = RawEvent(api, parameters, None, source, context)
          val enriched = EnrichmentManager.enrichEvent[IO](
            enrichmentReg,
            client,
            processor,
            timestamp,
            rawEvent,
            AcceptInvalid.featureFlags,
            IO.unit,
            SpecHelpers.registryLookup,
            atomicFieldLimits,
            emitFailed,
            SpecHelpers.DefaultMaxJsonDepth
          )
          enriched.value map { result =>
            result must beLike {
              case OptionIor.Right(e: EnrichedEvent) =>
                checkFn(e)
            }
          }
      }
    }

    "return a SchemaViolations bad row if the input event contains an invalid context" >> {
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "foo": "hello@world.com",
                  "emailAddress2": "foo@bar.org"
                }
              }
            ]
          }
        """
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value map {
        case OptionIor.Left(_: BadRow.SchemaViolations) => ok
        case other => ko(s"[$other] is not a SchemaViolations bad row")
      }
    }

    "return a SchemaViolations bad row if the input unstructured event is invalid" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "unallowedAdditionalField": "foo@bar.org"
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value map {
        case OptionIor.Left(_: BadRow.SchemaViolations) => ok
        case other => ko(s"[$other] is not a SchemaViolations bad row")
      }
    }

    "return a SchemaViolations bad row that contains 1 ValidationError for the atomic field and 1 ValidationError for the unstruct event" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "tr_tt" -> "not number",
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "unallowedAdditionalField": "foo@bar.org"
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      EnrichmentManager
        .enrichEvent[IO](
          enrichmentReg,
          client,
          processor,
          timestamp,
          rawEvent,
          AcceptInvalid.featureFlags,
          IO.unit,
          SpecHelpers.registryLookup,
          atomicFieldLimits,
          emitFailed,
          SpecHelpers.DefaultMaxJsonDepth
        )
        .value
        .map {
          case OptionIor.Left(
                BadRow.SchemaViolations(
                  _,
                  BadRowFailure.SchemaViolations(_,
                                                 NonEmptyList(FailureDetails.SchemaViolation.IgluError(schemaKey1, clientError1),
                                                              List(FailureDetails.SchemaViolation.IgluError(schemaKey2, clientError2))
                                                 )
                  ),
                  _
                )
              ) =>
            schemaKey1 must beEqualTo(AtomicFields.atomicSchema)
            clientError1.toString must contain("tr_tt")
            clientError1.toString must contain("Cannot be converted to java.math.BigDecimal")
            schemaKey2 must beEqualTo(emailSentSchema)
            clientError2.toString must contain(
              "unallowedAdditionalField: is not defined in the schema and the schema does not allow additional properties"
            )
          case other =>
            ko(s"[$other] is not a SchemaViolations bad row with 2 expected IgluError")
        }
    }

    "return an EnrichmentFailures bad row if one of the enrichment (JS enrichment here) fails" >> {
      val script =
        """
        function process(event) {
          throw "Javascript exception";
          return [ { a: "b" } ];
        }"""
      val jsEnrich = createJsEnrichment(script)
      val enrichmentReg = EnrichmentRegistry[IO](javascriptScript = List(jsEnrich))

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value map {
        case OptionIor.Left(
              BadRow.EnrichmentFailures(
                _,
                BadRowFailure.EnrichmentFailures(
                  _,
                  NonEmptyList(
                    FailureDetails.EnrichmentFailure(
                      _,
                      _: FailureDetails.EnrichmentFailureMessage.Simple
                    ),
                    Nil
                  )
                ),
                _
              )
            ) =>
          ok
        case other => ko(s"[$other] is not an EnrichmentFailures bad row with one EnrichmentFailureMessage.Simple")
      }
    }

    "return a SchemaViolations bad row containing one IgluError if one of the contexts added by the enrichments is invalid" >> {
      val script =
        """
        function process(event) {
          return [ { schema: "iglu:com.acme/email_sent/jsonschema/1-0-0",
                     data: {
                       emailAddress: "hello@world.com",
                       foo: "bar"
                     }
                   } ];
        }"""
      val jsEnrich = createJsEnrichment(script)
      val enrichmentReg = EnrichmentRegistry[IO](javascriptScript = List(jsEnrich))

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value map {
        case OptionIor.Left(
              BadRow.SchemaViolations(
                _,
                BadRowFailure.SchemaViolations(
                  _,
                  NonEmptyList(
                    _: FailureDetails.SchemaViolation.IgluError,
                    Nil
                  )
                ),
                _
              )
            ) =>
          ok
        case other => ko(s"[$other] is not a SchemaViolations bad row with one IgluError")
      }
    }

    // Also check that unstruct_event gets nullified
    "return a SchemaViolations bad row containing one IgluError if JS enrichment sets an invalid unstruct_event" >> {
      val script =
        """
        function process(event) {
          const ue = JSON.parse(event.getUnstruct_event())
          ue.data.data.foo = "bar"
          event.setUnstruct_event(JSON.stringify(ue))
          return []
        }"""
      val jsEnrich = createJsEnrichment(script)
      val enrichmentReg = EnrichmentRegistry[IO](javascriptScript = List(jsEnrich))

      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org"
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed = true,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value map {
        case OptionIor.Both(
              BadRow.SchemaViolations(
                _,
                BadRowFailure.SchemaViolations(
                  _,
                  NonEmptyList(
                    _: FailureDetails.SchemaViolation.IgluError,
                    Nil
                  )
                ),
                _
              ),
              enrichedEvent
            ) if enrichedEvent.unstruct_event.isEmpty =>
          ok
        case other =>
          ko(s"[$other] is not a SchemaViolations bad row with one IgluError and enriched event with unstruct_event set to None")
      }
    }

    // Also check that faulty context gets removed from the event
    "return a SchemaViolations bad row containing one IgluError if JS enrichment updates one of the contexts to something invalid" >> {
      val script =
        """
        function process(event) {
          const co = JSON.parse(event.getContexts())
          co.data[0].data.foo = "bar"
          event.setContexts(JSON.stringify(co))
          return []
        }"""
      val jsEnrich = createJsEnrichment(script)
      val enrichmentReg = EnrichmentRegistry[IO](javascriptScript = List(jsEnrich))

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "hello@world.com",
                  "emailAddress2": "foo@bar.org"
                }
              },
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "another@email.com",
                  "emailAddress2": "another2@email.com"
                }
              }
            ]
          }
        """
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed = true,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value map {
        case OptionIor.Both(
              BadRow.SchemaViolations(
                _,
                BadRowFailure.SchemaViolations(
                  _,
                  NonEmptyList(
                    _: FailureDetails.SchemaViolation.IgluError,
                    Nil
                  )
                ),
                _
              ),
              enrichedEvent
            ) if enrichedEvent.contexts.size == 1 =>
          ok
        case other =>
          ko(s"[$other] is not a SchemaViolations bad row with one IgluError and enriched event with only one context remaining")
      }
    }

    // Also check that unstruct_event gets nullified and that faulty context gets removed from the event
    "return a SchemaViolations bad row containing two IgluErrors if JS enrichment sets unstruct_event and one of the contexts to something invalid" >> {
      val script =
        """
        function process(event) {
          const ue = JSON.parse(event.getUnstruct_event())
          ue.data.data.foo = "bar"
          event.setUnstruct_event(JSON.stringify(ue))
          const co = JSON.parse(event.getContexts())
          co.data[0].data.foo = "bar"
          event.setContexts(JSON.stringify(co))
          return []
        }"""
      val jsEnrich = createJsEnrichment(script)
      val enrichmentReg = EnrichmentRegistry[IO](javascriptScript = List(jsEnrich))

      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "hello@world.com",
                  "emailAddress2": "foo@bar.org"
                }
              },
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "another@email.com",
                  "emailAddress2": "another2@email.com"
                }
              }
            ]
          }
        """,
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org"
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed = true,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value map {
        case OptionIor.Both(
              BadRow.SchemaViolations(
                _,
                BadRowFailure.SchemaViolations(
                  _,
                  NonEmptyList(
                    _: FailureDetails.SchemaViolation.IgluError,
                    List(_: FailureDetails.SchemaViolation.IgluError)
                  )
                ),
                _
              ),
              enrichedEvent
            ) if enrichedEvent.unstruct_event.isEmpty && enrichedEvent.contexts.size == 1 =>
          ok
        case other =>
          ko(
            s"[$other] is not a SchemaViolations bad row with two IgluErrors and enriched event with unstruct_event set to None and only one context remaining"
          )
      }
    }

    "emit an EnrichedEvent if everything goes well" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "hello@world.com",
                  "emailAddress2": "foo@bar.org"
                }
              }
            ]
          }
        """,
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org"
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Right(_) => ok
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "emit an EnrichedEvent if a PII value that needs to be hashed is an empty string" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "hello@world.com",
                  "emailAddress2": "foo@bar.org"
                }
              }
            ]
          }
        """,
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "emailAddress3": ""
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enrichmentReg = EnrichmentRegistry[IO](
        piiPseudonymizer = PiiPseudonymizerEnrichment(
          PiiMutators(
            pojo = Nil,
            unstruct = List(
              PiiPseudonymizerEnrichment.JsonFieldLocator(
                schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0, 0),
                jsonPath = "$.emailAddress3"
              )
            ),
            contexts = Nil,
            derivedContexts = Nil
          ),
          false,
          PiiStrategyPseudonymize(
            "MD5",
            hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
            "pepper123"
          ),
          anonymousOnly = false
        ).some
      )
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Right(_) => ok
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "emit an EnrichedEvent if a PII value that needs to be hashed is null" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "hello@world.com",
                  "emailAddress2": "foo@bar.org"
                }
              }
            ]
          }
        """,
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/2-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "emailAddress3": null
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enrichmentReg = EnrichmentRegistry[IO](
        piiPseudonymizer = PiiPseudonymizerEnrichment(
          PiiMutators(
            pojo = Nil,
            unstruct = List(
              PiiPseudonymizerEnrichment.JsonFieldLocator(
                schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0, 0),
                jsonPath = "$.emailAddress3"
              )
            ),
            contexts = Nil,
            derivedContexts = Nil
          ),
          false,
          PiiStrategyPseudonymize(
            "MD5",
            hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
            "pepper123"
          ),
          anonymousOnly = false
        ).some
      )
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Right(_) => ok
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "fail to emit an EnrichedEvent if a PII value that needs to be hashed is an empty object" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "hello@world.com",
                  "emailAddress2": "foo@bar.org"
                }
              }
            ]
          }
        """,
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "emailAddress3": {}
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enrichmentReg = EnrichmentRegistry[IO](
        piiPseudonymizer = PiiPseudonymizerEnrichment(
          PiiMutators(
            pojo = Nil,
            unstruct = List(
              PiiPseudonymizerEnrichment.JsonFieldLocator(
                schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0, 0),
                jsonPath = "$.emailAddress3"
              )
            ),
            contexts = Nil,
            derivedContexts = Nil
          ),
          false,
          PiiStrategyPseudonymize(
            "MD5",
            hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
            "pepper123"
          ),
          anonymousOnly = false
        ).some
      )
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Left(_) => ok
        case other => ko(s"[$other] is not a bad row")
      }
    }

    "fail to emit an EnrichedEvent if a context PII value that needs to be hashed is an empty object" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "hello@world.com",
                  "emailAddress2": "foo@bar.org",
                  "emailAddress3": {}
                }
              }
            ]
          }
        """,
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org"
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enrichmentReg = EnrichmentRegistry[IO](
        piiPseudonymizer = PiiPseudonymizerEnrichment(
          PiiMutators(
            pojo = Nil,
            unstruct = Nil,
            contexts = List(
              PiiPseudonymizerEnrichment.JsonFieldLocator(
                schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0, 0),
                jsonPath = "$.emailAddress3"
              )
            ),
            derivedContexts = Nil
          ),
          false,
          PiiStrategyPseudonymize(
            "MD5",
            hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
            "pepper123"
          ),
          anonymousOnly = false
        ).some
      )
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Left(_) => ok
        case other => ko(s"[$other] is not a bad row")
      }
    }

    "fail to emit an EnrichedEvent if a PII value needs to be hashed in both co and ue and is invalid in one of them" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "hello@world.com",
                  "emailAddress2": "foo@bar.org",
                  "emailAddress3": {}
                }
              }
            ]
          }
        """,
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "emailAddress3": ""
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enrichmentReg = EnrichmentRegistry[IO](
        piiPseudonymizer = PiiPseudonymizerEnrichment(
          PiiMutators(
            pojo = Nil,
            unstruct = List(
              PiiPseudonymizerEnrichment.JsonFieldLocator(
                schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0, 0),
                jsonPath = "$.emailAddress3"
              )
            ),
            contexts = List(
              PiiPseudonymizerEnrichment.JsonFieldLocator(
                schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0, 0),
                jsonPath = "$.emailAddress3"
              )
            ),
            derivedContexts = Nil
          ),
          false,
          PiiStrategyPseudonymize(
            "MD5",
            hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
            "pepper123"
          ),
          anonymousOnly = false
        ).some
      )
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Left(_) => ok
        case other => ko(s"[$other] is not a bad row")
      }
    }

    "emit an EnrichedEvent for valid integer fields" >> {
      val integers = List("42", "-42", "null")
      val fields = List("tid", "vid", "ti_qu", "pp_mix", "pp_max", "pp_miy", "pp_may")

      integers
        .flatTraverse { integer =>
          fields.traverse { field =>
            val parameters = Map(
              "e" -> "ue",
              "tv" -> "js-0.13.1",
              "p" -> "web",
              field -> integer
            ).toOpt
            val rawEvent = RawEvent(api, parameters, None, source, context)
            val enriched = EnrichmentManager.enrichEvent[IO](
              enrichmentReg,
              client,
              processor,
              timestamp,
              rawEvent,
              AcceptInvalid.featureFlags,
              IO.unit,
              SpecHelpers.registryLookup,
              atomicFieldLimits,
              emitFailed,
              SpecHelpers.DefaultMaxJsonDepth
            )
            enriched.value.map {
              case OptionIor.Right(_) => ok
              case other => ko(s"[$other] is not an enriched event")
            }
          }
        }
    }

    "emit an EnrichedEvent for valid decimal fields" >> {
      val decimals = List("42", "42.5", "null")
      val fields = List("ev_va", "se_va", "tr_tt", "tr_tx", "tr_sh", "ti_pr")

      decimals
        .flatTraverse { decimal =>
          fields.traverse { field =>
            val parameters = Map(
              "e" -> "ue",
              "tv" -> "js-0.13.1",
              "p" -> "web",
              field -> decimal
            ).toOpt
            val rawEvent = RawEvent(api, parameters, None, source, context)
            val enriched = EnrichmentManager.enrichEvent[IO](
              enrichmentReg,
              client,
              processor,
              timestamp,
              rawEvent,
              AcceptInvalid.featureFlags,
              IO.unit,
              SpecHelpers.registryLookup,
              atomicFieldLimits,
              emitFailed,
              SpecHelpers.DefaultMaxJsonDepth
            )
            enriched.value.map {
              case OptionIor.Right(_) => ok
              case other => ko(s"[$other] is not an enriched event")
            }
          }
        }
    }

    "create an EnrichedEvent with correct BigDecimal field values" >> {
      val decimals = List(
        // input, expected
        ("42", "42"),
        ("42.5", "42.5"),
        ("137777104559", "137777104559"),
        ("-137777104559", "-137777104559"),
        ("1E7", "10000000"),
        ("1.2E9", "1200000000"),
        ("0.000001", "0.000001"),
        ("0.0000001", "1E-7") // unavoidable consequence, due to BigDecimal internal representation
      )

      decimals
        .traverse {
          case (input, expected) =>
            val parameters = Map(
              "e" -> "ue",
              "tv" -> "js-0.13.1",
              "p" -> "web",
              "ev_va" -> input
            ).toOpt
            val rawEvent = RawEvent(api, parameters, None, source, context)
            val enriched = EnrichmentManager.enrichEvent[IO](
              enrichmentReg,
              client,
              processor,
              timestamp,
              rawEvent,
              AcceptInvalid.featureFlags,
              IO.unit,
              SpecHelpers.registryLookup,
              atomicFieldLimits,
              emitFailed,
              SpecHelpers.DefaultMaxJsonDepth
            )
            enriched.value.map {
              case OptionIor.Right(enriched) => enriched.se_value.toString must_== expected
              case other => ko(s"[$other] is not an enriched event")
            }
        }
    }

    "have a preference of 'ua' query string parameter over user agent of HTTP header" >> {
      val qs_ua = "Mozilla/5.0 (X11; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0"
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "ua" -> qs_ua,
        "p" -> "web"
      ).toOpt
      val contextWithUa = context.copy(useragent = Some("header-useragent"))
      val rawEvent = RawEvent(api, parameters, None, source, contextWithUa)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Right(enriched) =>
          enriched.useragent must_== qs_ua
          enriched.getDerived_contexts() must contain("\"agentName\":\"Firefox\"")
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "use user agent of HTTP header if 'ua' query string parameter is not set" >> {
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      ).toOpt
      val contextWithUa = context.copy(useragent = Some("header-useragent"))
      val rawEvent = RawEvent(api, parameters, None, source, contextWithUa)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Right(enriched) => enriched.useragent must_== "header-useragent"
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "accept user agent of HTTP header when it is not URL decodable" >> {
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      ).toOpt
      val ua = "Mozilla/5.0 (X11; Linux x86_64; rv:75.0) Gecko/20100101 %1$s/%2$s Firefox/75.0"
      val contextWithUa = context.copy(useragent = Some(ua))
      val rawEvent = RawEvent(api, parameters, None, source, contextWithUa)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Right(enriched) => enriched.useragent must_== ua
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "accept 'ua' in query string when it is not URL decodable" >> {
      val qs_ua = "Mozilla/5.0 (X11; Linux x86_64; rv:75.0) Gecko/20100101 %1$s/%2$s Firefox/75.0"
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "ua" -> qs_ua,
        "p" -> "web"
      ).toOpt
      val contextWithUa = context.copy(useragent = Some("header-useragent"))
      val rawEvent = RawEvent(api, parameters, None, source, contextWithUa)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Right(enriched) =>
          enriched.useragent must_== qs_ua
          enriched.getDerived_contexts() must contain("\"agentName\":\"%1$S\"")
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "pass derived contexts generated by previous enrichments to the JavaScript enrichment" >> {
      val script =
        """
        function process(event) {
          var derivedContexts = JSON.parse(event.getDerived_contexts());
          var firstHeaderValue = derivedContexts.data[0].data.value;
          event.setApp_id(firstHeaderValue);
          return [];
        }"""
      val enrichmentReg = EnrichmentRegistry[IO](
        javascriptScript = List(createJsEnrichment(script)),
        httpHeaderExtractor = Some(HttpHeaderExtractorEnrichment(".*".r))
      )

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      ).toOpt
      val headerContext = context.copy(headers = List("X-Tract-Me: moo"))
      val rawEvent = RawEvent(api, parameters, None, source, headerContext)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Right(enriched) => enriched.app_id must_== "moo"
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "run multiple JavaScript enrichments" >> {
      val script1 =
        """
        function process(event) {
          event.setApp_id("test_app_id");
          return [];
        }"""

      val script2 =
        """
        function process(event) {
          event.setPlatform("test_platform");
          return [];
        }"""
      val enrichmentReg = EnrichmentRegistry[IO](
        javascriptScript = List(
          createJsEnrichment(script1),
          createJsEnrichment(script2)
        )
      )

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Right(enriched) =>
          enriched.app_id must_== "test_app_id"
          enriched.platform must_== "test_platform"
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "emit an EnrichedEvent with superseded schemas" >> {
      val expectedContexts = jparse(
        """
        {
          "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
          "data": [
            {
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org"
              }
            },
            {
              "schema":"iglu:com.acme/superseding_example/jsonschema/1-0-1",
              "data": {
                "field_a": "value_a",
                "field_b": "value_b"
              }
            },
            {
              "schema":"iglu:com.acme/superseding_example/jsonschema/1-0-1",
              "data": {
                "field_a": "value_a",
                "field_b": "value_b",
                "field_d": "value_d"
              }
            },
            {
              "schema":"iglu:com.acme/superseding_example/jsonschema/1-0-1",
              "data": {
                "field_a": "value_a",
                "field_b": "value_b",
                "field_c": "value_c",
                "field_d": "value_d"
              }
            }
          ]
        }
        """
      ).toOption.get
      val expectedDerivedContexts = jparse(
        """
        {
          "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
          "data": [
            {
              "schema":"iglu:com.snowplowanalytics.iglu/validation_info/jsonschema/1-0-0",
              "data":{
                "originalSchema":"iglu:com.acme/superseding_example/jsonschema/2-0-0",
                "validatedWith":"2-0-1"
              }
            },
            {
              "schema":"iglu:com.snowplowanalytics.iglu/validation_info/jsonschema/1-0-0",
              "data":{
                "originalSchema":"iglu:com.acme/superseding_example/jsonschema/1-0-0",
                "validatedWith":"1-0-1"
              }
            }
          ]
        }
        """
      ).toOption.get
      val expectedUnstructEvent = jparse(
        """
        {
          "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
          "data":{
            "schema":"iglu:com.acme/superseding_example/jsonschema/2-0-1",
            "data": {
              "field_e": "value_e",
              "field_f": "value_f",
              "field_g": "value_g"
            }
          }
        }
        """
      ).toOption.get
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "hello@world.com",
                  "emailAddress2": "foo@bar.org"
                }
              },
              {
                "schema":"iglu:com.acme/superseding_example/jsonschema/1-0-0",
                "data": {
                  "field_a": "value_a",
                  "field_b": "value_b"
                }
              },
              {
                "schema":"iglu:com.acme/superseding_example/jsonschema/1-0-0",
                "data": {
                  "field_a": "value_a",
                  "field_b": "value_b",
                  "field_d": "value_d"
                }
              },
              {
                "schema":"iglu:com.acme/superseding_example/jsonschema/1-0-1",
                "data": {
                  "field_a": "value_a",
                  "field_b": "value_b",
                  "field_c": "value_c",
                  "field_d": "value_d"
                }
              }
            ]
          }
        """,
        "ue_pr" ->
          """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/superseding_example/jsonschema/2-0-0",
              "data": {
                "field_e": "value_e",
                "field_f": "value_f",
                "field_g": "value_g"
              }
            }
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg.copy(yauaa = None),
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed,
        SpecHelpers.DefaultMaxJsonDepth
      )

      enriched.value.map {
        case OptionIor.Right(enriched) =>
          val p = EnrichedEvent.toPartiallyEnrichedEvent(enriched)
          val contextsJson = jparse(p.contexts.get).toOption.get
          val derivedContextsJson = jparse(p.derived_contexts.get).toOption.get
          val ueJson = jparse(p.unstruct_event.get).toOption.get
          (contextsJson must beEqualTo(expectedContexts)) and
            (derivedContextsJson must beEqualTo(expectedDerivedContexts)) and
            (ueJson must beEqualTo(expectedUnstructEvent))
        case other => ko(s"[$other] is not an enriched event")
      }
    }

    "remove the invalid unstructured event and enrich the event if emitFailed is set to true" >> {
      val script =
        s"""
        function process(event) {
          return [ ${emailSent} ];
        }"""
      val enrichmentReg = EnrichmentRegistry[IO](
        javascriptScript = List(
          createJsEnrichment(script)
        )
      )
      val invalidUeData =
        """{
             "emailAddress": "hello@world.com",
             "emailAddress2": "foo@bar.org",
             "unallowedAdditionalField": "foo@bar.org"
           }"""
      val invalidUe =
        s"""{
             "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
             "data": $invalidUeData
           }"""

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          s"""
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              $clientSession
            ]
          }
        """,
        "ue_pr" ->
          s"""
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":$invalidUe
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      def expectedDerivedContexts(enriched: EnrichedEvent): Boolean = {
        val emailSentSDJ = SelfDescribingData.parse[Json](jparse(emailSent).toOption.get).toOption.get
        enriched.derived_contexts match {
          case List(SelfDescribingData(Failure.`failureSchemaKey`, feJson), `emailSentSDJ`)
              if feJson.field("failureType") === "ValidationError".asJson &&
                feJson.field("errors") === Json.arr(
                  Json.obj(
                    "message" := "$.unallowedAdditionalField: is not defined in the schema and the schema does not allow additional properties",
                    "source" := "unstruct",
                    "path" := "$",
                    "keyword" := "additionalProperties",
                    "targets" := List("unallowedAdditionalField")
                  )
                ) &&
                feJson.field("schema") === emailSentSchema.asJson &&
                feJson.field("data") === jparse(invalidUeData).toOption.get =>
            true
          case _ => false
        }
      }

      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed = true,
        SpecHelpers.DefaultMaxJsonDepth
      )

      enriched.value.map {
        case OptionIor.Both(_: BadRow.SchemaViolations, enriched)
            if enriched.unstruct_event.isEmpty &&
              enriched.contexts.map(_.schema) === List(clientSessionSchema) &&
              expectedDerivedContexts(enriched) =>
          ok
        case other => ko(s"[$other] is not a SchemaViolations bad row and an enriched event without the unstructured event")
      }
    }

    "remove the invalid context and enrich the event if emitFailed is set to true" >> {
      val script =
        s"""
        function process(event) {
          return [ ${emailSent} ];
        }"""
      val enrichmentReg = EnrichmentRegistry[IO](
        javascriptScript = List(
          createJsEnrichment(script)
        )
      )
      val invalidContextData =
        """{
            "foo": "hello@world.com",
            "emailAddress2": "foo@bar.org"
          }"""
      val invalidContext =
        s"""{
            "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
            "data": $invalidContextData
          }"""

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          s"""
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [$invalidContext]
          }
        """,
        "ue_pr" ->
          s"""
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data": $clientSession
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      def expectedDerivedContexts(enriched: EnrichedEvent): Boolean = {
        val emailSentSDJ = SelfDescribingData.parse[Json](jparse(emailSent).toOption.get).toOption.get
        enriched.derived_contexts match {
          case List(SelfDescribingData(Failure.`failureSchemaKey`, feJson), `emailSentSDJ`)
              if feJson.field("failureType") === "ValidationError".asJson &&
                feJson.field("errors") === Json.arr(
                  Json.obj(
                    "message" := "$.foo: is not defined in the schema and the schema does not allow additional properties",
                    "source" := "contexts",
                    "path" := "$",
                    "keyword" := "additionalProperties",
                    "targets" := List("foo")
                  ),
                  Json.obj(
                    "message" := "$.emailAddress: is missing but it is required",
                    "source" := "contexts",
                    "path" := "$",
                    "keyword" := "required",
                    "targets" := List("emailAddress")
                  )
                ) &&
                feJson.field("schema") === emailSentSchema.asJson &&
                feJson.field("data") === jparse(invalidContextData).toOption.get =>
            true
          case _ => false
        }
      }

      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed = true,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Both(_: BadRow.SchemaViolations, enriched)
            if enriched.contexts.isEmpty &&
              enriched.unstruct_event.map(_.schema) === Some(clientSessionSchema) &&
              expectedDerivedContexts(enriched) =>
          ok
        case other => ko(s"[$other] is not a SchemaViolations bad row and an enriched event with no input contexts")
      }
    }

    "remove one invalid context (out of 2) and enrich the event if emitFailed is set to true" >> {
      val script =
        s"""
        function process(event) {
          return [ ${emailSent} ];
        }"""
      val enrichmentReg = EnrichmentRegistry[IO](
        javascriptScript = List(
          createJsEnrichment(script)
        )
      )
      val invalidContextData =
        """
          {
            "foo": "hello@world.com",
            "emailAddress2": "foo@bar.org"
          }"""
      val invalidContext =
        s"""
          {
            "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
            "data": $invalidContextData
          }"""

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" ->
          s"""
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              $invalidContext,
              $clientSession
            ]
          }
        """,
        "ue_pr" ->
          s"""
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data": $clientSession
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      def expectedDerivedContexts(enriched: EnrichedEvent): Boolean = {
        val emailSentSDJ = SelfDescribingData.parse[Json](jparse(emailSent).toOption.get).toOption.get
        enriched.derived_contexts match {
          case List(SelfDescribingData(Failure.`failureSchemaKey`, feJson), `emailSentSDJ`)
              if feJson.field("failureType") === "ValidationError".asJson &&
                feJson.field("errors") === Json.arr(
                  Json.obj(
                    "message" := "$.foo: is not defined in the schema and the schema does not allow additional properties",
                    "source" := "contexts",
                    "path" := "$",
                    "keyword" := "additionalProperties",
                    "targets" := List("foo")
                  ),
                  Json.obj(
                    "message" := "$.emailAddress: is missing but it is required",
                    "source" := "contexts",
                    "path" := "$",
                    "keyword" := "required",
                    "targets" := List("emailAddress")
                  )
                ) &&
                feJson.field("schema") === emailSentSchema.asJson &&
                feJson.field("data") === jparse(invalidContextData).toOption.get =>
            true
          case _ => false
        }
      }

      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed = true,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Both(_: BadRow.SchemaViolations, enriched)
            if enriched.unstruct_event.map(_.schema) === Some(clientSessionSchema) &&
              enriched.contexts.map(_.schema) === List(clientSessionSchema) &&
              expectedDerivedContexts(enriched) =>
          ok
        case other => ko(s"[$other] is not a SchemaViolations bad row and an enriched event with 1 input context")
      }
    }

    "return the enriched event after an enrichment exception if emitFailed is set to true" >> {
      val script =
        s"""
        function process(event) {
          throw "Javascript exception";
          return [ $emailSent ];
        }"""
      val enrichmentReg = EnrichmentRegistry[IO](
        yauaa = Some(YauaaEnrichment(None)),
        javascriptScript = List(
          createJsEnrichment(script)
        )
      )

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "ue_pr" ->
          s"""
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data": $clientSession
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      def expectedDerivedContexts(enriched: EnrichedEvent): Boolean =
        enriched.derived_contexts match {
          case List(
                SelfDescribingData(Failure.`failureSchemaKey`, feJson),
                SelfDescribingData(SchemaKey("nl.basjes", "yauaa_context", "jsonschema", _), _)
              )
              if feJson.field("failureType") === "EnrichmentError: Javascript enrichment".asJson &&
                feJson.field("errors") === Json.arr(
                  Json.obj(
                    "message" := "Error during execution of JavaScript function: [Javascript exception in <eval> at line number 3 at column number 10]"
                  )
                ) &&
                feJson.field("schema") === JavascriptScriptEnrichment.supportedSchema.copy(addition = 0.some).asString.asJson &&
                feJson.field("data") === Json.Null =>
            true
          case _ => false
        }

      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed = true,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Both(_: BadRow.EnrichmentFailures, enriched)
            if enriched.unstruct_event.map(_.schema) === Some(clientSessionSchema) &&
              expectedDerivedContexts(enriched) =>
          ok
        case other => ko(s"[$other] is not an EnrichmentFailures bad row and an enriched event")
      }
    }

    "return a SchemaViolations bad row in the Left in case of both a schema violation and an enrichment failure if emitFailed is set to true" >> {
      val script =
        s"""
        function process(event) {
          throw "Javascript exception";
          return [ $emailSent ];
        }"""
      val enrichmentReg = EnrichmentRegistry[IO](
        javascriptScript = List(
          createJsEnrichment(script)
        )
      )

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "tr_tt" -> "foo",
        "ue_pr" ->
          s"""
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data": $clientSession
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      def expectedDerivedContexts(enriched: EnrichedEvent): Boolean =
        enriched.derived_contexts match {
          case List(
                SelfDescribingData(Failure.`failureSchemaKey`, validationError),
                SelfDescribingData(Failure.`failureSchemaKey`, enrichmentError)
              )
              if validationError.field("failureType") === "ValidationError".asJson &&
                validationError.field("errors") === Json.arr(
                  Json.obj(
                    "message" := "Cannot be converted to java.math.BigDecimal. Error : Character f is neither a decimal digit number, decimal point, nor \"e\" notation exponential mark.",
                    "source" := AtomicError.source,
                    "path" := "tr_tt",
                    "keyword" := AtomicError.keywordParse,
                    "targets" := Json.arr()
                  )
                ) &&
                validationError.field("schema") === AtomicFields.atomicSchema.asJson &&
                validationError.field("data") === Json.obj("tr_tt" := "foo") &&
                enrichmentError.field("failureType") === "EnrichmentError: Javascript enrichment".asJson &&
                enrichmentError.field("errors") === Json.arr(
                  Json.obj(
                    "message" := "Error during execution of JavaScript function: [Javascript exception in <eval> at line number 3 at column number 10]"
                  )
                ) &&
                enrichmentError.field("schema") === JavascriptScriptEnrichment.supportedSchema.copy(addition = 0.some).asString.asJson &&
                enrichmentError.field("data") === Json.Null =>
            true
          case _ => false
        }

      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed = true,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Both(_: BadRow.SchemaViolations, enriched) if expectedDerivedContexts(enriched) => ok
        case other => ko(s"[$other] doesn't have a SchemaViolations bad row in the Left")
      }
    }

    "remove an invalid enrichment context and return the enriched event if emitFailed is set to true" >> {
      val invalidContextData =
        """
          {
            "foo": "hello@world.com",
            "emailAddress2": "foo@bar.org"
          }"""
      val invalidContext =
        s"""
          {
            "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
            "data": $invalidContextData
          }"""
      val script =
        s"""
        function process(event) {
          return [
            $invalidContext
          ];
        }"""
      val enrichmentReg = EnrichmentRegistry[IO](
        yauaa = Some(YauaaEnrichment(None)),
        javascriptScript = List(
          createJsEnrichment(script)
        )
      )

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "ue_pr" ->
          s"""
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data": $clientSession
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      def expectedDerivedContexts(enriched: EnrichedEvent): Boolean =
        enriched.derived_contexts match {
          case List(
                SelfDescribingData(Failure.`failureSchemaKey`, feJson),
                SelfDescribingData(SchemaKey("nl.basjes", "yauaa_context", "jsonschema", _), _)
              )
              if feJson.field("failureType") === "ValidationError".asJson &&
                feJson.field("errors") === Json.arr(
                  Json.obj(
                    "message" := "$.foo: is not defined in the schema and the schema does not allow additional properties",
                    "source" := "derived_contexts",
                    "path" := "$",
                    "keyword" := "additionalProperties",
                    "targets" := List("foo")
                  ),
                  Json.obj(
                    "message" := "$.emailAddress: is missing but it is required",
                    "source" := "derived_contexts",
                    "path" := "$",
                    "keyword" := "required",
                    "targets" := List("emailAddress")
                  )
                ) &&
                feJson.field("schema") === emailSentSchema.asJson &&
                feJson.field("data") === jparse(invalidContextData).toOption.get =>
            true
          case _ => false
        }

      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed = true,
        SpecHelpers.DefaultMaxJsonDepth
      )
      enriched.value.map {
        case OptionIor.Both(_: BadRow.SchemaViolations, enriched)
            if enriched.unstruct_event.map(_.schema) === Some(clientSessionSchema) &&
              expectedDerivedContexts(enriched) =>
          ok
        case other => ko(s"[$other] is not a SchemaViolations bad row and an enriched event without the faulty enrichment context")
      }
    }

    "return a bad row that contains validation errors only from ue if there is validation error in both ue and derived contexts when emitFailed is set to true" >> {
      val invalidContext =
        """
          {
            "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
            "data": {
              "foo": "hello@world.com",
              "emailAddress2": "foo@bar.org"
            }
          }"""
      val invalidUe =
        """{
             "schema":"iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-1",
             "data": {
               "unallowedAdditionalField": "foo@bar.org"
             }
           }"""
      val script =
        s"""
        function process(event) {
          return [
            $invalidContext
          ];
        }"""
      val enrichmentReg = EnrichmentRegistry[IO](
        javascriptScript = List(
          createJsEnrichment(script)
        )
      )
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "ue_pr" ->
          s"""
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data": $invalidUe
          }"""
      ).toOpt
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent[IO](
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent,
        AcceptInvalid.featureFlags,
        IO.unit,
        SpecHelpers.registryLookup,
        atomicFieldLimits,
        emitFailed = true,
        SpecHelpers.DefaultMaxJsonDepth
      )
      def expectedDerivedContexts(enriched: EnrichedEvent): Boolean =
        enriched.derived_contexts.count(_.schema === Failure.failureSchemaKey) === 2

      def expectedBadRow(badRow: BadRow): Boolean =
        badRow match {
          case BadRow.SchemaViolations(
                _,
                BadRowFailure.SchemaViolations(
                  _,
                  NonEmptyList(FailureDetails.SchemaViolation.IgluError(`clientSessionSchema`, _), Nil)
                ),
                _
              ) =>
            true
          case _ => false
        }

      enriched.value.map {
        case OptionIor.Both(badRow, enriched)
            if enriched.unstruct_event.isEmpty &&
              expectedDerivedContexts(enriched) &&
              expectedBadRow(badRow) =>
          ok
        case other => ko(s"[$other] is not expected one")
      }
    }
  }

  "getCrossDomain" should {
    val schemaKey = SchemaKey(
      CrossNavigationEnrichment.supportedSchema.vendor,
      CrossNavigationEnrichment.supportedSchema.name,
      CrossNavigationEnrichment.supportedSchema.format,
      SchemaVer.Full(1, 0, 0)
    )

    "do nothing if none query string parameters - crossNavigation enabled" >> {
      val crossNavigationEnabled = Some(new CrossNavigationEnrichment(schemaKey))
      val qsMap: Option[QueryStringParameters] = None
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationEnabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation.Enriched =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beNone) and
                (p.refr_dvce_tstamp must beNone) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEmpty)
          }
        )
    }

    "do nothing if none query string parameters - crossNavigation disabled" >> {
      val crossNavigationDisabled = None
      val qsMap: Option[QueryStringParameters] = None
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationDisabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation.Enriched =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beNone) and
                (p.refr_dvce_tstamp must beNone) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEmpty)
          }
        )
    }

    "do nothing if _sp is empty - crossNavigation enabled" >> {
      val crossNavigationEnabled = Some(new CrossNavigationEnrichment(schemaKey))
      val qsMap: Option[QueryStringParameters] = Some(List(("_sp" -> Some(""))))
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationEnabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation.Enriched =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beNone) and
                (p.refr_dvce_tstamp must beNone) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEmpty)
          }
        )
    }

    "do nothing if _sp is empty - crossNavigation disabled" >> {
      val crossNavigationDisabled = None
      val qsMap: Option[QueryStringParameters] = Some(List(("_sp" -> Some(""))))
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationDisabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation.Enriched =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beNone) and
                (p.refr_dvce_tstamp must beNone) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEmpty)
          }
        )
    }

    "add atomic props and ctx with original _sp format and cross navigation enabled" >> {
      val crossNavigationEnabled = Some(new CrossNavigationEnrichment(schemaKey))
      val qsMap: Option[QueryStringParameters] = Some(
        List(
          ("_sp" -> Some("abc.1697175843762"))
        )
      )
      val expectedRefrDuid = Some("abc")
      val expectedRefrTstamp = Some("2023-10-13 05:44:03.762")
      val expectedCtx: List[SelfDescribingData[Json]] = List(
        SelfDescribingData(
          CrossNavigationEnrichment.outputSchema,
          Map(
            "domain_user_id" -> Some("abc"),
            "timestamp" -> Some("2023-10-13T05:44:03.762Z"),
            "session_id" -> None,
            "user_id" -> None,
            "source_id" -> None,
            "source_platform" -> None,
            "reason" -> None
          ).asJson
        )
      )
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationEnabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation.Enriched =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beEqualTo(expectedRefrDuid)) and
                (p.refr_dvce_tstamp must beEqualTo(expectedRefrTstamp)) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEqualTo(expectedCtx))
          }
        )
    }

    "add atomic props but no ctx with original _sp format and cross navigation disabled" >> {
      val crossNavigationDisabled = None
      val qsMap: Option[QueryStringParameters] = Some(
        List(
          ("_sp" -> Some("abc.1697175843762"))
        )
      )
      val expectedRefrDuid = Some("abc")
      val expectedRefrTstamp = Some("2023-10-13 05:44:03.762")
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationDisabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation.Enriched =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beEqualTo(expectedRefrDuid)) and
                (p.refr_dvce_tstamp must beEqualTo(expectedRefrTstamp)) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEmpty)
          }
        )
    }

    "add atomic props and ctx with extended _sp format and cross navigation enabled" >> {
      val crossNavigationEnabled = Some(new CrossNavigationEnrichment(schemaKey))
      val qsMap: Option[QueryStringParameters] = Some(
        List(
          ("_sp" -> Some("abc.1697175843762.176ff68a-4769-4566-ad0e-3792c1c8148f.dGVzdGVy.c29tZVNvdXJjZUlk.web.dGVzdGluZ19yZWFzb24"))
        )
      )
      val expectedRefrDuid = Some("abc")
      val expectedRefrTstamp = Some("2023-10-13 05:44:03.762")
      val expectedCtx: List[SelfDescribingData[Json]] = List(
        SelfDescribingData(
          CrossNavigationEnrichment.outputSchema,
          Map(
            "domain_user_id" -> Some("abc"),
            "timestamp" -> Some("2023-10-13T05:44:03.762Z"),
            "session_id" -> Some("176ff68a-4769-4566-ad0e-3792c1c8148f"),
            "user_id" -> Some("tester"),
            "source_id" -> Some("someSourceId"),
            "source_platform" -> Some("web"),
            "reason" -> Some("testing_reason")
          ).asJson
        )
      )
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationEnabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation.Enriched =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beEqualTo(expectedRefrDuid)) and
                (p.refr_dvce_tstamp must beEqualTo(expectedRefrTstamp)) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEqualTo(expectedCtx))
          }
        )
    }

    "add atomic props but no ctx with extended _sp format and cross navigation disabled" >> {
      val crossNavigationDisabled = None
      val qsMap: Option[QueryStringParameters] = Some(
        List(
          ("_sp" -> Some("abc.1697175843762.176ff68a-4769-4566-ad0e-3792c1c8148f.dGVzdGVy.c29tZVNvdXJjZUlk.web.dGVzdGluZ19yZWFzb24"))
        )
      )
      val expectedRefrDuid = Some("abc")
      val expectedRefrTstamp = Some("2023-10-13 05:44:03.762")
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationDisabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation.Enriched =>
              val p = EnrichedEvent.toPartiallyEnrichedEvent(acc.event)
              (p.refr_domain_userid must beEqualTo(expectedRefrDuid)) and
                (p.refr_dvce_tstamp must beEqualTo(expectedRefrTstamp)) and
                (acc.errors must beEmpty) and
                (acc.contexts must beEmpty)
          }
        )
    }

    "error with info if parsing failed and cross navigation is enabled" >> {
      val crossNavigationEnabled = Some(new CrossNavigationEnrichment(schemaKey))
      // causing a parsing failure by providing invalid tstamp
      val qsMap: Option[QueryStringParameters] = Some(
        List(
          ("_sp" -> Some("abc.some_invalid_timestamp_value"))
        )
      )
      val input = new EnrichedEvent()
      val expectedFail = FailureDetails.EnrichmentFailure(
        FailureDetails
          .EnrichmentInformation(
            schemaKey,
            "cross-navigation"
          )
          .some,
        FailureDetails.EnrichmentFailureMessage.InputData(
          "sp_dtm",
          "some_invalid_timestamp_value".some,
          "Not in the expected format: ms since epoch"
        )
      )
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationEnabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation.Enriched =>
              (acc.errors must not beEmpty) and
                (acc.errors must beEqualTo(List(expectedFail))) and
                (acc.contexts must beEmpty)
          }
        )
    }

    "error without info if parsing failed and cross navigation is disabled" >> {
      val crossNavigationDisabled = None
      // causing a parsing failure by providing invalid tstamp
      val qsMap: Option[QueryStringParameters] = Some(
        List(
          ("_sp" -> Some("abc.some_invalid_timestamp_value"))
        )
      )
      val input = new EnrichedEvent()
      val expectedFail = FailureDetails.EnrichmentFailure(
        None,
        FailureDetails.EnrichmentFailureMessage.InputData(
          "sp_dtm",
          "some_invalid_timestamp_value".some,
          "Not in the expected format: ms since epoch"
        )
      )
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      EnrichmentManager
        .getCrossDomain[IO](
          qsMap,
          crossNavigationDisabled
        )
        .runS(inputState)
        .map(
          _ must beLike {
            case acc: EnrichmentManager.Accumulation.Enriched =>
              (acc.errors must not beEmpty) and
                (acc.errors must beEqualTo(List(expectedFail))) and
                (acc.contexts must beEmpty)
          }
        )
    }
  }

  "getIabContext" should {
    "return no context if useragent is null" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("127.0.0.1")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      for {
        iab <- iabEnrichment
        result <- EnrichmentManager
                    .getIabContext[IO](Some(iab))
                    .runS(inputState)
      } yield result must beLike {
        case acc: EnrichmentManager.Accumulation.Enriched =>
          (acc.errors must beEmpty) and (acc.contexts must beEmpty)
      }
    }

    "return no context if user_ipaddress is null" >> {
      val input = new EnrichedEvent()
      input.setUseragent("Firefox")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      for {
        iab <- iabEnrichment
        result <- EnrichmentManager
                    .getIabContext[IO](Some(iab))
                    .runS(inputState)
      } yield result must beLike {
        case acc: EnrichmentManager.Accumulation.Enriched =>
          (acc.errors must beEmpty) and (acc.contexts must beEmpty)
      }
    }

    "return no context if derived_tstamp is null" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("127.0.0.1")
      input.setUseragent("Firefox")
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      for {
        iab <- iabEnrichment
        result <- EnrichmentManager
                    .getIabContext[IO](Some(iab))
                    .runS(inputState)
      } yield result must beLike {
        case acc: EnrichmentManager.Accumulation.Enriched =>
          (acc.errors must beEmpty) and (acc.contexts must beEmpty)
      }
    }

    "return no context if user_ipaddress is invalid" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("invalid")
      input.setUseragent("Firefox")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      for {
        iab <- iabEnrichment
        result <- EnrichmentManager
                    .getIabContext[IO](Some(iab))
                    .runS(inputState)
      } yield result must beLike {
        case acc: EnrichmentManager.Accumulation.Enriched =>
          (acc.errors must beEmpty) and (acc.contexts must beEmpty)
      }
    }

    "return no context if user_ipaddress is hostname (don't try to resovle it)" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("localhost")
      input.setUseragent("Firefox")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      for {
        iab <- iabEnrichment
        result <- EnrichmentManager
                    .getIabContext[IO](Some(iab))
                    .runS(inputState)
      } yield result must beLike {
        case acc: EnrichmentManager.Accumulation.Enriched =>
          (acc.errors must beEmpty) and (acc.contexts must beEmpty)
      }
    }

    "return Some if all arguments are valid" >> {
      val input = new EnrichedEvent()
      input.setUser_ipaddress("127.0.0.1")
      input.setUseragent("Firefox")
      input.setDerived_tstamp("2010-06-30 01:20:01.000")
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      for {
        iab <- iabEnrichment
        result <- EnrichmentManager
                    .getIabContext[IO](Some(iab))
                    .runS(inputState)
      } yield result must beLike {
        case acc: EnrichmentManager.Accumulation.Enriched =>
          (acc.errors must beEmpty) and (acc.contexts must not beEmpty)
      }
    }
  }

  "getCollectorVersionSet" should {
    "return an enrichment failure if v_collector is null" >> {
      val input = new EnrichedEvent()
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      EnrichmentManager
        .getCollectorVersionSet[IO]
        .runS(inputState)
        .map(_ must beLike {
          case acc: EnrichmentManager.Accumulation.Enriched =>
            acc.errors must not beEmpty
        })
    }

    "return an enrichment failure if v_collector is empty" >> {
      val input = new EnrichedEvent()
      input.v_collector = ""
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      EnrichmentManager
        .getCollectorVersionSet[IO]
        .runS(inputState)
        .map(_ must beLike {
          case acc: EnrichmentManager.Accumulation.Enriched =>
            acc.errors must not beEmpty
        })
    }

    "return Unit if v_collector is set" >> {
      val input = new EnrichedEvent()
      input.v_collector = "v42"
      val inputState = EnrichmentManager.Accumulation.Enriched(input, Nil, Nil)
      EnrichmentManager
        .getCollectorVersionSet[IO]
        .runS(inputState)
        .map(_ must beLike {
          case acc: EnrichmentManager.Accumulation.Enriched =>
            acc.errors must beEmpty
        })
    }
  }

  "validateEnriched" should {
    "create a SchemaViolations bad row if an atomic field is oversized" >> {
      EnrichmentManager
        .enrichEvent[IO](
          enrichmentReg,
          client,
          processor,
          timestamp,
          RawEvent(api, fatBody, None, source, context),
          featureFlags = AcceptInvalid.featureFlags.copy(acceptInvalid = false),
          IO.unit,
          SpecHelpers.registryLookup,
          atomicFieldLimits,
          emitFailed,
          SpecHelpers.DefaultMaxJsonDepth
        )
        .value
        .map {
          case OptionIor.Left(
                BadRow.SchemaViolations(
                  _,
                  BadRowFailure.SchemaViolations(_, NonEmptyList(FailureDetails.SchemaViolation.IgluError(schemaKey, clientError), Nil)),
                  _
                )
              ) =>
            schemaKey must beEqualTo(AtomicFields.atomicSchema)
            clientError.toString must contain("v_tracker")
            clientError.toString must contain("Field is longer than maximum allowed size")
          case other =>
            ko(s"[$other] is not a SchemaViolations bad row with one IgluError")
        }
    }

    "not create a bad row if an atomic field is oversized and acceptInvalid is set to true" >> {
      EnrichmentManager
        .enrichEvent[IO](
          enrichmentReg,
          client,
          processor,
          timestamp,
          RawEvent(api, fatBody, None, source, context),
          featureFlags = AcceptInvalid.featureFlags.copy(acceptInvalid = true),
          IO.unit,
          SpecHelpers.registryLookup,
          atomicFieldLimits,
          emitFailed,
          SpecHelpers.DefaultMaxJsonDepth
        )
        .value
        .map {
          case OptionIor.Right(_) => ok
          case other => ko(s"[$other] is not an enriched event")
        }
    }

    "return a SchemaViolations bad row containing both the atomic field length error and the invalid enrichment context error" >> {
      val script =
        """
        function process(event) {
          return [ { schema: "iglu:com.acme/email_sent/jsonschema/1-0-0",
                     data: {
                       emailAddress: "hello@world.com",
                       foo: "bar"
                     }
                   } ];
        }"""
      val jsEnrich = createJsEnrichment(script)
      val enrichmentReg = EnrichmentRegistry[IO](javascriptScript = List(jsEnrich))

      val rawEvent = RawEvent(api, fatBody, None, source, context)
      EnrichmentManager
        .enrichEvent[IO](
          enrichmentReg,
          client,
          processor,
          timestamp,
          rawEvent,
          AcceptInvalid.featureFlags,
          IO.unit,
          SpecHelpers.registryLookup,
          atomicFieldLimits,
          emitFailed,
          SpecHelpers.DefaultMaxJsonDepth
        )
        .value
        .map {
          case OptionIor.Left(
                BadRow.SchemaViolations(
                  _,
                  BadRowFailure.SchemaViolations(_,
                                                 NonEmptyList(FailureDetails.SchemaViolation.IgluError(schemaKey1, clientError1),
                                                              List(FailureDetails.SchemaViolation.IgluError(schemaKey2, clientError2))
                                                 )
                  ),
                  _
                )
              ) =>
            schemaKey1 must beEqualTo(emailSentSchema)
            clientError1.toString must contain("emailAddress2: is missing but it is required")
            schemaKey2 must beEqualTo(AtomicFields.atomicSchema)
            clientError2.toString must contain("v_tracker")
            clientError2.toString must contain("Field is longer than maximum allowed size")
          case other =>
            ko(s"[$other] is not a SchemaViolations bad row with 2 IgluError")
        }
    }

    "remove an oversized atomic field if emitFailed is set to true" >> {
      val enriched = EnrichmentManager
        .enrichEvent[IO](
          enrichmentReg,
          client,
          processor,
          timestamp,
          RawEvent(api, fatBody, None, source, context),
          featureFlags = AcceptInvalid.featureFlags.copy(acceptInvalid = false),
          IO.unit,
          SpecHelpers.registryLookup,
          atomicFieldLimits,
          emitFailed = true,
          SpecHelpers.DefaultMaxJsonDepth
        )

      enriched.value.map {
        case OptionIor.Both(_: BadRow.SchemaViolations, enriched) if Option(enriched.v_tracker).isEmpty => ok
        case other => ko(s"[$other] is not a SchemaViolations bad row and an enriched event without tracker version")
      }
    }
  }

  "setDerivedContexts" should {
    val sv = Failure.SchemaViolation(
      schemaViolation = FailureDetails.SchemaViolation.NotJson("testField", "testValue".some, "testError"),
      source = "testSource",
      data = Json.obj("testKey" := "testValue"),
      etlTstamp = etlTstamp
    )
    val ef = Failure.EnrichmentFailure(
      FailureDetails.EnrichmentFailure(
        None,
        FailureDetails.EnrichmentFailureMessage.Simple("testError")
      ),
      etlTstamp
    )
    val emailSentSDJ = SelfDescribingData.parse[Json](jparse(emailSent).toOption.get).toOption.get
    "set derived contexts correctly if enrichment result is OptionIor.Left" >> {
      val enriched = new EnrichedEvent()
      val enrichmentResult = OptionIor.Left(NonEmptyList.of(NonEmptyList.of(sv, ef), NonEmptyList.of(sv, ef)))
      EnrichmentManager.setDerivedContexts(enriched, enrichmentResult, processor)
      val schemas = enriched.derived_contexts.map(_.schema)
      schemas.size must beEqualTo(4)
      forall(schemas)(s => s must beEqualTo(Failure.failureSchemaKey))
    }
    "set derived contexts correctly if enrichment result is OptionIor.Right" >> {
      val enriched = new EnrichedEvent()
      val enrichmentResult = OptionIor.Right(List(emailSentSDJ, emailSentSDJ))
      EnrichmentManager.setDerivedContexts(enriched, enrichmentResult, processor)
      val schemas = enriched.derived_contexts.map(_.schema)
      schemas.size must beEqualTo(2)
      forall(schemas)(s => s must beEqualTo(emailSentSchema))
    }
    "set derived contexts correctly if enrichment result is OptionIor.Both" >> {
      val enriched = new EnrichedEvent()
      val enrichmentResult = OptionIor.Both(
        NonEmptyList.of(NonEmptyList.of(sv, ef), NonEmptyList.of(sv, ef)),
        List(emailSentSDJ, emailSentSDJ)
      )
      EnrichmentManager.setDerivedContexts(enriched, enrichmentResult, processor)
      val schemas = enriched.derived_contexts.map(_.schema)
      schemas.size must beEqualTo(6)
      schemas.count(_ === Failure.failureSchemaKey) must beEqualTo(4)
      schemas.count(_ === emailSentSchema) must beEqualTo(2)
    }
  }

  "drop" should {
    val defaultScript = """
        function process(event) {
          event.drop();
        }"""
    val defaultParameters = Map(
      "e" -> "pp",
      "tv" -> "js-0.13.1",
      "p" -> "web"
    ).toOpt
    val defaultMatcher: OptionIor[BadRow, EnrichedEvent] => MatchResult[Any] = {
      case OptionIor.None => ok
      case other => ko(s"[$other] is not a None")
    }

    def commonDropTest(
      matcher: OptionIor[BadRow, EnrichedEvent] => MatchResult[Any] = defaultMatcher,
      script: String = defaultScript,
      parameters: Map[String, Option[String]] = defaultParameters,
      emitFailed: Boolean = false,
      collectorName: String = source.name
    ) = {
      val jsEnrich = createJsEnrichment(script)
      val enrichmentReg = EnrichmentRegistry[IO](javascriptScript = List(jsEnrich))

      val rawEvent = RawEvent(api, parameters, None, source.copy(name = collectorName), context)
      EnrichmentManager
        .enrichEvent[IO](
          enrichmentReg,
          client,
          processor,
          timestamp,
          rawEvent,
          AcceptInvalid.featureFlags,
          IO.unit,
          SpecHelpers.registryLookup,
          atomicFieldLimits,
          emitFailed,
          SpecHelpers.DefaultMaxJsonDepth
        )
        .value
        .map(matcher)
    }

    "drop good event" >> {
      commonDropTest()
    }

    "drop event that should end up as SchemaViolations bad row normally" >> {
      commonDropTest(
        parameters = Map(
          "e" -> "ue",
          "tv" -> "js-0.13.1",
          "p" -> "web",
          "ue_pr" ->
            """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "unallowedAdditionalField": "foo@bar.org"
              }
            }
          }"""
        ).toOpt
      )
    }

    "drop event with oversized atomic field" >> {
      commonDropTest(parameters = fatBody)
    }

    "drop event that should end up as failed event normally when emitFailed is true" >> {
      commonDropTest(parameters = fatBody, emitFailed = true)
    }

    "drop event with multiple failures" >> {
      commonDropTest(
        parameters = Map(
          "e" -> "ue",
          "tv" -> "js-0.13.1",
          "p" -> "web",
          "tr_tt" -> "not number",
          "ue_pr" ->
            """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "unallowedAdditionalField": "foo@bar.org"
              }
            }
          }"""
        ).toOpt,
        collectorName = ""
      )
    }

    "drop event with multiple failures when emitFailed is true" >> {
      commonDropTest(
        parameters = Map(
          "e" -> "ue",
          "tv" -> "js-0.13.1",
          "p" -> "web",
          "tr_tt" -> "not number",
          "ue_pr" ->
            """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "unallowedAdditionalField": "foo@bar.org"
              }
            }
          }"""
        ).toOpt,
        collectorName = "",
        emitFailed = true
      )
    }
  }

  "eraseDerived_contexts" should {
    val defaultParameters = Map(
      "e" -> "pp",
      "tv" -> "js-0.13.1",
      "p" -> "web"
    ).toOpt

    def commonEraseDerivedContextsTest(
      matcher: OptionIor[BadRow, EnrichedEvent] => MatchResult[Any],
      scripts: List[String],
      parameters: Map[String, Option[String]] = defaultParameters,
      emitFailed: Boolean = false,
      collectorName: String = source.name
    ) = {
      val jsEnrichList = scripts.map(s => createJsEnrichment(s))
      val enrichmentReg = EnrichmentRegistry[IO](
        javascriptScript = jsEnrichList,
        httpHeaderExtractor = Some(HttpHeaderExtractorEnrichment(".*".r))
      )

      val headerContext = context.copy(headers = List("X-Tract-Me: moo"))
      val rawEvent = RawEvent(api, parameters, None, source.copy(name = collectorName), headerContext)
      EnrichmentManager
        .enrichEvent[IO](
          enrichmentReg,
          client,
          processor,
          timestamp,
          rawEvent,
          AcceptInvalid.featureFlags,
          IO.unit,
          SpecHelpers.registryLookup,
          atomicFieldLimits,
          emitFailed,
          SpecHelpers.DefaultMaxJsonDepth
        )
        .value
        .map(matcher)
    }

    "not have any effect if it isn't called in JS enrichment script" >> {
      commonEraseDerivedContextsTest(
        scripts = List("""
          function process(event) {
            return [{"schema":"iglu:com.snowplowanalytics.snowplow/consent_withdrawn/jsonschema/1-0-0","data":{"all": false}}]
          }"""),
        matcher = {
          case OptionIor.Right(enriched) =>
            enriched.derived_contexts must beEqualTo(
              List(
                SelfDescribingData(
                  SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/consent_withdrawn/jsonschema/1-0-0").toOption.get,
                  json"""{"all":false}"""
                ),
                SelfDescribingData(
                  SchemaKey.fromUri("iglu:org.ietf/http_header/jsonschema/1-0-0").toOption.get,
                  json"""{"name":"X-Tract-Me","value":"moo"}"""
                )
              )
            )
          case _ => ko
        }
      )
    }

    "erase existing derived contexts and only JS enrichment result should be returned as derived context" >> {
      commonEraseDerivedContextsTest(
        scripts = List("""
          function process(event) {
            event.eraseDerived_contexts();
            return [{"schema":"iglu:com.snowplowanalytics.snowplow/consent_withdrawn/jsonschema/1-0-0","data":{"all": false}}]
          }"""),
        matcher = {
          case OptionIor.Right(enriched) =>
            enriched.derived_contexts must beEqualTo(
              List(
                SelfDescribingData(
                  SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/consent_withdrawn/jsonschema/1-0-0").toOption.get,
                  json"""{"all":false}"""
                )
              )
            )
          case _ => ko
        }
      )
    }

    "erase existing derived contexts and derived contexts should be empty if JS enrichment script result is empty" >> {
      commonEraseDerivedContextsTest(
        scripts = List("""
          function process(event) {
            event.eraseDerived_contexts();
            return []
          }"""),
        matcher = {
          case OptionIor.Right(enriched) =>
            enriched.derived_contexts must beEmpty
          case _ => ko
        }
      )
    }

    "erase existing derived contexts with multiple JS enrichments" >> {
      commonEraseDerivedContextsTest(
        scripts = List(
          """
          function process(event) {
            return [{"schema":"iglu:com.snowplowanalytics.snowplow/application_background/jsonschema/1-0-0","data":{"backgroundIndex": 1}}]
          }""",
          """
          function process(event) {
            event.eraseDerived_contexts();
            return [{"schema":"iglu:com.snowplowanalytics.snowplow/application_background/jsonschema/1-0-0","data":{"backgroundIndex": 2}}]
          }""",
          """
          function process(event) {
            return [{"schema":"iglu:com.snowplowanalytics.snowplow/application_background/jsonschema/1-0-0","data":{"backgroundIndex": 3}}]
          }""",
          """
          function process(event) {
            event.eraseDerived_contexts();
            return [{"schema":"iglu:com.snowplowanalytics.snowplow/application_background/jsonschema/1-0-0","data":{"backgroundIndex": 4}}]
          }""",
          """
          function process(event) {
            return [{"schema":"iglu:com.snowplowanalytics.snowplow/application_background/jsonschema/1-0-0","data":{"backgroundIndex": 5}}]
          }""",
          """
          function process(event) {
            return [{"schema":"iglu:com.snowplowanalytics.snowplow/application_background/jsonschema/1-0-0","data":{"backgroundIndex": 6}}]
          }"""
        ),
        matcher = {
          case OptionIor.Right(enriched) =>
            enriched.derived_contexts must beEqualTo(
              List(
                SelfDescribingData(
                  SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/application_background/jsonschema/1-0-0").toOption.get,
                  json"""{"backgroundIndex":6}"""
                ),
                SelfDescribingData(
                  SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/application_background/jsonschema/1-0-0").toOption.get,
                  json"""{"backgroundIndex":5}"""
                ),
                SelfDescribingData(
                  SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/application_background/jsonschema/1-0-0").toOption.get,
                  json"""{"backgroundIndex":4}"""
                )
              )
            )
          case _ => ko
        }
      )
    }
  }
}

object EnrichmentManagerSpec {

  val enrichmentReg = EnrichmentRegistry[IO](yauaa = Some(YauaaEnrichment(None)))
  val client = SpecHelpers.client
  val processor = Processor("ssc-tests", "0.0.0")
  val timestamp = DateTime.now()

  val api = CollectorPayload.Api("com.snowplowanalytics.snowplow", "tp2")
  val source = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
  val context = CollectorPayload.Context(
    DateTime.parse("2013-08-29T00:18:48.000+00:00"),
    "37.157.33.123".some,
    None,
    None,
    Nil,
    None
  )

  val atomicFieldLimits = AtomicFields.from(Map("v_tracker" -> 100))

  val leanBody = Map(
    "e" -> "pp",
    "tv" -> "js-0.13.1",
    "p" -> "web"
  ).toOpt

  val fatBody = Map(
    "e" -> "pp",
    "tv" -> s"${"s" * 500}",
    "p" -> "web"
  ).toOpt

  val iabEnrichment = IabEnrichment
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
    .getOrElse(throw new RuntimeException("IAB enrichment couldn't be initialised")) // to make sure it's not none
    .enrichment[IO]

  val emailSentSchema =
    SchemaKey(
      "com.acme",
      "email_sent",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )

  val emailSent = s"""{
    "schema": "${emailSentSchema.toSchemaUri}",
    "data": {
      "emailAddress": "hello@world.com",
      "emailAddress2": "foo@bar.org"
    }
  }"""

  val clientSessionSchema =
    SchemaKey(
      "com.snowplowanalytics.snowplow",
      "client_session",
      "jsonschema",
      SchemaVer.Full(1, 0, 1)
    )

  val clientSession = s"""{
    "schema": "${clientSessionSchema.toSchemaUri}",
    "data": {
      "sessionIndex": 1,
      "storageMechanism": "LOCAL_STORAGE",
      "firstEventId": "5c33fccf-6be5-4ce6-afb1-e34026a3ca75",
      "sessionId": "21c2a0dd-892d-42d1-b156-3a9d4e147eef",
      "previousSessionId": null,
      "userId": "20d631b8-7837-49df-a73e-6da73154e6fd"
    }
  }"""

  implicit class JsonFieldGetter(json: Json) {
    def field(f: String): Json =
      json.hcursor.downField(f).as[Json].toOption.get
  }
}
