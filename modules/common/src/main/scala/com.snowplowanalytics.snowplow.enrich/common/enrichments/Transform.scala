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

import com.snowplowanalytics.snowplow.enrich.common.enrichments.{EventEnrichments => EE}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{MiscEnrichments => ME}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{ClientEnrichments => CE}
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils => CU, JsonUtils => JU}
import com.snowplowanalytics.snowplow.enrich.common.utils.MapTransformer._
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent

object Transform {

  /**
   * Map the parameters of the input raw event to the fields of the enriched event,
   * with a possible transformation. For instance "ip" in the input would be mapped
   * to "user_ipaddress" in the enriched event
   * @param enriched /!\ MUTABLE enriched event, mutated IN-PLACE /!\
   */
  private[enrichments] def transform(raw: RawEvent, enriched: EnrichedEvent): ValidatedNel[AtomicError.ParseError, Unit] = {
    val sourceMap: SourceMap = raw.parameters.collect { case (k, Some(v)) => (k, v) }
    val firstPassTransform = enriched.transform(sourceMap, firstPassTransformMap)
    val secondPassTransform = enriched.transform(sourceMap, secondPassTransformMap)

    (firstPassTransform |+| secondPassTransform).void
  }

  // The TransformMap used to map/transform the fields takes the format:
  // "source key" -> (transformFunction, field(s) to set)
  // Caution: by definition, a TransformMap loses type safety. Always unit test!
  private val firstPassTransformMap: TransformMap =
    Map(
      ("e", (EE.extractEventType, "event")),
      ("ip", (ME.extractIp, "user_ipaddress")),
      ("aid", (ME.toTsvSafe, "app_id")),
      ("p", (ME.extractPlatform, "platform")),
      ("tid", (CU.stringToJInteger2, "txn_id")),
      ("uid", (ME.toTsvSafe, "user_id")),
      ("duid", (ME.toTsvSafe, "domain_userid")),
      ("nuid", (ME.toTsvSafe, "network_userid")),
      ("ua", (ME.toTsvSafe, "useragent")),
      ("fp", (ME.toTsvSafe, "user_fingerprint")),
      ("vid", (CU.stringToJInteger2, "domain_sessionidx")),
      ("sid", (CU.validateUuid, "domain_sessionid")),
      ("dtm", (EE.extractTimestamp, "dvce_created_tstamp")),
      ("ttm", (EE.extractTimestamp, "true_tstamp")),
      ("stm", (EE.extractTimestamp, "dvce_sent_tstamp")),
      ("tna", (ME.toTsvSafe, "name_tracker")),
      ("tv", (ME.toTsvSafe, "v_tracker")),
      ("cv", (ME.toTsvSafe, "v_collector")),
      ("lang", (ME.toTsvSafe, "br_lang")),
      ("f_pdf", (CU.stringToBooleanLikeJByte, "br_features_pdf")),
      ("f_fla", (CU.stringToBooleanLikeJByte, "br_features_flash")),
      ("f_java", (CU.stringToBooleanLikeJByte, "br_features_java")),
      ("f_dir", (CU.stringToBooleanLikeJByte, "br_features_director")),
      ("f_qt", (CU.stringToBooleanLikeJByte, "br_features_quicktime")),
      ("f_realp", (CU.stringToBooleanLikeJByte, "br_features_realplayer")),
      ("f_wma", (CU.stringToBooleanLikeJByte, "br_features_windowsmedia")),
      ("f_gears", (CU.stringToBooleanLikeJByte, "br_features_gears")),
      ("f_ag", (CU.stringToBooleanLikeJByte, "br_features_silverlight")),
      ("cookie", (CU.stringToBooleanLikeJByte, "br_cookies")),
      ("res", (CE.extractViewDimensions, ("dvce_screenwidth", "dvce_screenheight"))), // Note tuple target
      ("cd", (ME.toTsvSafe, "br_colordepth")),
      ("tz", (ME.toTsvSafe, "os_timezone")),
      ("refr", (ME.toTsvSafe, "page_referrer")),
      ("url", (ME.toTsvSafe, "page_url")), // Note we may override this below
      ("page", (ME.toTsvSafe, "page_title")),
      ("cs", (ME.toTsvSafe, "doc_charset")),
      ("ds", (CE.extractViewDimensions, ("doc_width", "doc_height"))),
      ("vp", (CE.extractViewDimensions, ("br_viewwidth", "br_viewheight"))),
      ("eid", (CU.validateUuid, "event_id")),
      // Custom contexts
      ("co", (JU.extractUnencJson, "contexts")),
      ("cx", (JU.extractBase64EncJson, "contexts")),
      // Custom structured events
      ("ev_ca", (ME.toTsvSafe, "se_category")), // LEGACY tracker var. Leave for backwards compat
      ("ev_ac", (ME.toTsvSafe, "se_action")), // LEGACY tracker var. Leave for backwards compat
      ("ev_la", (ME.toTsvSafe, "se_label")), // LEGACY tracker var. Leave for backwards compat
      ("ev_pr", (ME.toTsvSafe, "se_property")), // LEGACY tracker var. Leave for backwards compat
      ("ev_va", (CU.stringToJBigDecimal2, "se_value")), // LEGACY tracker var. Leave for backwards compat
      ("se_ca", (ME.toTsvSafe, "se_category")),
      ("se_ac", (ME.toTsvSafe, "se_action")),
      ("se_la", (ME.toTsvSafe, "se_label")),
      ("se_pr", (ME.toTsvSafe, "se_property")),
      ("se_va", (CU.stringToJBigDecimal2, "se_value")),
      // Custom unstructured events
      ("ue_pr", (JU.extractUnencJson, "unstruct_event")),
      ("ue_px", (JU.extractBase64EncJson, "unstruct_event")),
      // Ecommerce transactions
      ("tr_id", (ME.toTsvSafe, "tr_orderid")),
      ("tr_af", (ME.toTsvSafe, "tr_affiliation")),
      ("tr_tt", (CU.stringToJBigDecimal2, "tr_total")),
      ("tr_tx", (CU.stringToJBigDecimal2, "tr_tax")),
      ("tr_sh", (CU.stringToJBigDecimal2, "tr_shipping")),
      ("tr_ci", (ME.toTsvSafe, "tr_city")),
      ("tr_st", (ME.toTsvSafe, "tr_state")),
      ("tr_co", (ME.toTsvSafe, "tr_country")),
      // Ecommerce transaction items
      ("ti_id", (ME.toTsvSafe, "ti_orderid")),
      ("ti_sk", (ME.toTsvSafe, "ti_sku")),
      ("ti_na", (ME.toTsvSafe, "ti_name")), // ERROR in Tracker Protocol
      ("ti_nm", (ME.toTsvSafe, "ti_name")),
      ("ti_ca", (ME.toTsvSafe, "ti_category")),
      ("ti_pr", (CU.stringToJBigDecimal2, "ti_price")),
      ("ti_qu", (CU.stringToJInteger2, "ti_quantity")),
      // Page pings
      ("pp_mix", (CU.stringToJInteger2, "pp_xoffset_min")),
      ("pp_max", (CU.stringToJInteger2, "pp_xoffset_max")),
      ("pp_miy", (CU.stringToJInteger2, "pp_yoffset_min")),
      ("pp_may", (CU.stringToJInteger2, "pp_yoffset_max")),
      // Currency
      ("tr_cu", (ME.toTsvSafe, "tr_currency")),
      ("ti_cu", (ME.toTsvSafe, "ti_currency"))
    )

  // A second TransformMap which can overwrite values set by the first
  private val secondPassTransformMap: TransformMap =
    // Overwrite collector-set nuid with tracker-set tnuid
    Map(("tnuid", (ME.toTsvSafe, "network_userid")))
}
