/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import cats.implicits._
import cats.data.ValidatedNel

import com.snowplowanalytics.snowplow.enrich.common.utils.AtomicError
import com.snowplowanalytics.snowplow.enrich.common.RawEventParameters

import com.snowplowanalytics.snowplow.enrich.common.enrichments.{EventEnrichments => EE}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{MiscEnrichments => ME}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{ClientEnrichments => CE}
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils => CU, JsonUtils => JU}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent

import java.lang.{Integer => JInteger}
import java.lang.{Byte => JByte}
import java.math.{BigDecimal => JBigDecimal}

object Transform {

  /**
   * Map the parameters of the input raw event to the fields of the enriched event,
   * with a possible transformation. For instance "ip" in the input would be mapped
   * to "user_ipaddress" in the enriched event
   * @param enriched /!\ MUTABLE enriched event, mutated IN-PLACE /!\
   */
  private[enrichments] def transform(raw: RawEvent, enriched: EnrichedEvent): ValidatedNel[AtomicError.ParseError, Unit] =
    allTransformers.traverse_(f => f(raw.parameters, enriched))

  private val allTransformers: List[(RawEventParameters, EnrichedEvent) => ValidatedNel[AtomicError.ParseError, Unit]] = List(
    simpleTransformer[String]("e", EE.extractEventType, _.event = _),
    simpleTransformer[String]("ip", ME.extractIp, _.user_ipaddress = _),
    simpleTransformer[String]("aid", ME.toTsvSafe, _.app_id = _),
    simpleTransformer[String]("p", ME.extractPlatform, _.platform = _),
    simpleTransformer[JInteger]("tid", CU.stringToJInteger2, _.txn_id = _),
    simpleTransformer[String]("uid", ME.toTsvSafe, _.user_id = _),
    simpleTransformer[String]("duid", ME.toTsvSafe, _.domain_userid = _),
    simpleTransformer[String]("nuid", ME.toTsvSafe, _.network_userid = _),
    simpleTransformer[String]("tnuid", ME.toTsvSafe, _.network_userid = _), // overrides nuid set on previous line
    simpleTransformer[String]("ua", ME.toTsvSafe, _.useragent = _),
    simpleTransformer[String]("fp", ME.toTsvSafe, _.user_fingerprint = _),
    simpleTransformer[JInteger]("vid", CU.stringToJInteger2, _.domain_sessionidx = _),
    simpleTransformer[String]("sid", CU.validateUuid, _.domain_sessionid = _),
    simpleTransformer[String]("dtm", EE.extractTimestamp, _.dvce_created_tstamp = _),
    simpleTransformer[String]("ttm", EE.extractTimestamp, _.true_tstamp = _),
    simpleTransformer[String]("stm", EE.extractTimestamp, _.dvce_sent_tstamp = _),
    simpleTransformer[String]("tna", ME.toTsvSafe, _.name_tracker = _),
    simpleTransformer[String]("tv", ME.toTsvSafe, _.v_tracker = _),
    simpleTransformer[String]("cv", ME.toTsvSafe, _.v_collector = _),
    simpleTransformer[String]("lang", ME.toTsvSafe, _.br_lang = _),
    simpleTransformer[JByte]("f_pdf", CU.stringToBooleanLikeJByte, _.br_features_pdf = _),
    simpleTransformer[JByte]("f_fla", CU.stringToBooleanLikeJByte, _.br_features_flash = _),
    simpleTransformer[JByte]("f_java", CU.stringToBooleanLikeJByte, _.br_features_java = _),
    simpleTransformer[JByte]("f_dir", CU.stringToBooleanLikeJByte, _.br_features_director = _),
    simpleTransformer[JByte]("f_qt", CU.stringToBooleanLikeJByte, _.br_features_quicktime = _),
    simpleTransformer[JByte]("f_realp", CU.stringToBooleanLikeJByte, _.br_features_realplayer = _),
    simpleTransformer[JByte]("f_wma", CU.stringToBooleanLikeJByte, _.br_features_windowsmedia = _),
    simpleTransformer[JByte]("f_gears", CU.stringToBooleanLikeJByte, _.br_features_gears = _),
    simpleTransformer[JByte]("f_ag", CU.stringToBooleanLikeJByte, _.br_features_silverlight = _),
    simpleTransformer[JByte]("cookie", CU.stringToBooleanLikeJByte, _.br_cookies = _),
    tuple2Transformer("res", CE.extractViewDimensions, _.dvce_screenwidth = _, _.dvce_screenheight = _),
    simpleTransformer[String]("cd", ME.toTsvSafe, _.br_colordepth = _),
    simpleTransformer[String]("tz", ME.toTsvSafe, _.os_timezone = _),
    simpleTransformer[String]("refr", ME.toTsvSafe, _.page_referrer = _),
    simpleTransformer[String]("url", ME.toTsvSafe, _.page_url = _),
    simpleTransformer[String]("page", ME.toTsvSafe, _.page_title = _),
    simpleTransformer[String]("cs", ME.toTsvSafe, _.doc_charset = _),
    tuple2Transformer("ds", CE.extractViewDimensions, _.doc_width = _, _.doc_height = _),
    tuple2Transformer("vp", CE.extractViewDimensions, _.br_viewwidth = _, _.br_viewheight = _),
    simpleTransformer[String]("eid", CU.validateUuid, _.event_id = _),
    // Custom contexts
    simpleTransformer[String]("co", JU.extractUnencJson, _.contexts = _),
    simpleTransformer[String]("cx", JU.extractBase64EncJson, _.contexts = _),
    // Custom structured events
    simpleTransformer[String]("ev_ca", ME.toTsvSafe, _.se_category = _), // LEGACY tracker var. Leave for backwards compat
    simpleTransformer[String]("ev_ac", ME.toTsvSafe, _.se_action = _), // LEGACY tracker var. Leave for backwards compat
    simpleTransformer[String]("ev_la", ME.toTsvSafe, _.se_label = _), // LEGACY tracker var. Leave for backwards compat
    simpleTransformer[String]("ev_pr", ME.toTsvSafe, _.se_property = _), // LEGACY tracker var. Leave for backwards compat
    simpleTransformer[JBigDecimal]("ev_va", CU.stringToJBigDecimal2, _.se_value = _), // LEGACY tracker var. Leave for backwards compat
    simpleTransformer[String]("se_ca", ME.toTsvSafe, _.se_category = _),
    simpleTransformer[String]("se_ac", ME.toTsvSafe, _.se_action = _),
    simpleTransformer[String]("se_la", ME.toTsvSafe, _.se_label = _),
    simpleTransformer[String]("se_pr", ME.toTsvSafe, _.se_property = _),
    simpleTransformer[JBigDecimal]("se_va", CU.stringToJBigDecimal2, _.se_value = _),
    // Custom unstructured events
    simpleTransformer[String]("ue_pr", JU.extractUnencJson, _.unstruct_event = _),
    simpleTransformer[String]("ue_px", JU.extractBase64EncJson, _.unstruct_event = _),
    // Ecommerce transactions
    simpleTransformer[String]("tr_id", ME.toTsvSafe, _.tr_orderid = _),
    simpleTransformer[String]("tr_af", ME.toTsvSafe, _.tr_affiliation = _),
    simpleTransformer[JBigDecimal]("tr_tt", CU.stringToJBigDecimal2, _.tr_total = _),
    simpleTransformer[JBigDecimal]("tr_tx", CU.stringToJBigDecimal2, _.tr_tax = _),
    simpleTransformer[JBigDecimal]("tr_sh", CU.stringToJBigDecimal2, _.tr_shipping = _),
    simpleTransformer[String]("tr_ci", ME.toTsvSafe, _.tr_city = _),
    simpleTransformer[String]("tr_st", ME.toTsvSafe, _.tr_state = _),
    simpleTransformer[String]("tr_co", ME.toTsvSafe, _.tr_country = _),
    // Ecommerce transaction items
    simpleTransformer[String]("ti_id", ME.toTsvSafe, _.ti_orderid = _),
    simpleTransformer[String]("ti_sk", ME.toTsvSafe, _.ti_sku = _),
    simpleTransformer[String]("ti_na", ME.toTsvSafe, _.ti_name = _), // ERROR in Tracker Protocol
    simpleTransformer[String]("ti_nm", ME.toTsvSafe, _.ti_name = _),
    simpleTransformer[String]("ti_ca", ME.toTsvSafe, _.ti_category = _),
    simpleTransformer[JBigDecimal]("ti_pr", CU.stringToJBigDecimal2, _.ti_price = _),
    simpleTransformer[JInteger]("ti_qu", CU.stringToJInteger2, _.ti_quantity = _),
    // Page pings
    simpleTransformer[JInteger]("pp_mix", CU.stringToJInteger2, _.pp_xoffset_min = _),
    simpleTransformer[JInteger]("pp_max", CU.stringToJInteger2, _.pp_xoffset_max = _),
    simpleTransformer[JInteger]("pp_miy", CU.stringToJInteger2, _.pp_yoffset_min = _),
    simpleTransformer[JInteger]("pp_may", CU.stringToJInteger2, _.pp_yoffset_max = _),
    // Currency
    simpleTransformer[String]("tr_cu", ME.toTsvSafe, _.tr_currency = _),
    simpleTransformer[String]("ti_cu", ME.toTsvSafe, _.ti_currency = _)
  )

  private def simpleTransformer[T](
    shortName: String,
    converter: (String, String) => Either[AtomicError.ParseError, T],
    setter: (EnrichedEvent, T) => Unit
  ): (RawEventParameters, EnrichedEvent) => ValidatedNel[AtomicError.ParseError, Unit] = {
    case (params, event) =>
      params.get(shortName) match {
        case Some(Some(v)) =>
          converter(shortName, v) match {
            case Right(converted) =>
              setter(event, converted).valid
            case Left(e) =>
              e.invalidNel
          }
        case Some(None) | None =>
          ().valid
      }
  }

  private def tuple2Transformer(
    shortName: String,
    converter: (String, String) => Either[AtomicError.ParseError, (JInteger, JInteger)],
    setter1: (EnrichedEvent, JInteger) => Unit,
    setter2: (EnrichedEvent, JInteger) => Unit
  ): (RawEventParameters, EnrichedEvent) => ValidatedNel[AtomicError.ParseError, Unit] = {
    case (params, event) =>
      params.get(shortName) match {
        case Some(Some(v)) =>
          converter(shortName, v) match {
            case Right((converted1, converted2)) =>
              setter1(event, converted1)
              setter2(event, converted2)
              ().valid
            case Left(e) =>
              e.invalidNel
          }
        case Some(None) | None =>
          ().valid
      }
  }
}
