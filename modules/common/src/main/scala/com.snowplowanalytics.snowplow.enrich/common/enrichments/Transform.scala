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
import cats.data.{NonEmptyList, ValidatedNel}

import com.snowplowanalytics.snowplow.enrich.common.utils.AtomicError

import com.snowplowanalytics.snowplow.enrich.common.enrichments.{EventEnrichments => EE, MiscEnrichments => ME, ClientEnrichments => CE}
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils => CU, IgluUtils, JsonUtils => JU}
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
  private[enrichments] def transform(
    raw: RawEvent,
    enriched: EnrichedEvent
  ): (ValidatedNel[AtomicError.ParseError, Unit], IgluUtils.EventExtractInput) = {
    val (errors, specials) = raw.parameters.foldLeft((List.empty[AtomicError.ParseError], SpecialFields.empty)) {
      case ((errors, specials), (key, Some(value))) =>
        transformMap.get(key) match {
          case Some(fn) =>
            val (maybeError, newSpecials) = fn(value, enriched, specials)
            (maybeError ++: errors, newSpecials)
          case None =>
            (errors, specials)
        }
      case ((errors, specials), (_, None)) =>
        (errors, specials)
    }

    // Set the network_userid.
    // This is done after the other transforms, because tnuid takes precedence over nuid
    specials.tnuid.foreach(enriched.network_userid = _)

    val validated = NonEmptyList.fromList(errors) match {
      case Some(nel) => nel.invalid
      case None => ().valid
    }

    val igluInputs = IgluUtils.EventExtractInput(unstructEvent = specials.unstruct, contexts = specials.contexts)
    (validated, igluInputs)
  }

  /**
   * Accumulateds special fields, which are not set directly on the EnrichedEvent
   *  @param unstruct The ue_pr or ue_px field. This must be JSON-decoded before setting it on the EnrichedEvent
   *  @param contexts The co or cx field. This must be JSON-decoded before setting it on the EnrichedEvent
   *  @param tnuid We delay setting this on the EnrichedEvent because it must take priority over `nuid` field
   */
  private case class SpecialFields(
    unstruct: Option[String],
    contexts: Option[String],
    tnuid: Option[String]
  )

  private object SpecialFields {
    def empty: SpecialFields = SpecialFields(None, None, None)
  }

  /**
   * Function for accumulating the results of transform on all fields
   *  - First input param is the String value sent by the tracker
   *  - Second input param is the EnrichedEvent in which we are accumulating results
   *  - Third input param is the SpecialFields in which we are accumulating special fields
   *  - First return param is optionally a converstion error
   *  - Second return param is the fixed SpecialFields after this accumulation
   */
  private type AccumulatorFn = (String, EnrichedEvent, SpecialFields) => (Option[AtomicError.ParseError], SpecialFields)

  /**
   * Maps from tracker protocol field name (e.g. `eid`) to a function for setting the field in an event
   */
  private final val transformMap: Map[String, AccumulatorFn] =
    Map(
      // Special fields that do not directly set a field in the EnrichedEvent
      "tnuid" -> {
        case (input, _, specials) =>
          ME.toTsvSafe("tnuid", input) match {
            case Right(converted) =>
              (None, specials.copy(tnuid = Some(converted)))
            case Left(error) =>
              (Some(error), specials)
          }
      },
      // co tracker field is accumulated in the SpecialFields
      "co" -> {
        case (input, _, specials) =>
          JU.extractUnencJson("co", input) match {
            case Right(converted) =>
              (None, specials.copy(contexts = Some(converted)))
            case Left(error) =>
              (Some(error), specials)
          }
      },
      // cx tracker field is accumulated in the SpecialFields
      "cx" -> {
        case (input, _, specials) =>
          JU.extractBase64EncJson("cx", input) match {
            case Right(converted) =>
              (None, specials.copy(contexts = Some(converted)))
            case Left(error) =>
              (Some(error), specials)
          }
      },
      // ue_pr tracker field is accumulated in the SpecialFields
      "ue_pr" -> {
        case (input, _, specials) =>
          JU.extractUnencJson("ue_pr", input) match {
            case Right(converted) =>
              (None, specials.copy(unstruct = Some(converted)))
            case Left(error) =>
              (Some(error), specials)
          }
      },
      // ue_px tracker field is accumulated in the SpecialFields
      "ue_px" -> {
        case (input, _, specials) =>
          JU.extractBase64EncJson("ue_px", input) match {
            case Right(converted) =>
              (None, specials.copy(unstruct = Some(converted)))
            case Left(error) =>
              (Some(error), specials)
          }
      },
      // Fields that correspond directly to the EnrichedEvent
      simpleTransform[String]("e", EE.extractEventType, _.event = _),
      simpleTransform[String]("ip", ME.extractIp, _.user_ipaddress = _),
      simpleTransform[String]("aid", ME.toTsvSafe, _.app_id = _),
      simpleTransform[String]("p", ME.extractPlatform, _.platform = _),
      simpleTransform[JInteger]("tid", CU.stringToJInteger2, _.txn_id = _),
      simpleTransform[String]("uid", ME.toTsvSafe, _.user_id = _),
      simpleTransform[String]("duid", ME.toTsvSafe, _.domain_userid = _),
      simpleTransform[String]("nuid", ME.toTsvSafe, _.network_userid = _),
      simpleTransform[String]("ua", ME.toTsvSafe, _.useragent = _),
      simpleTransform[String]("fp", ME.toTsvSafe, _.user_fingerprint = _),
      simpleTransform[JInteger]("vid", CU.stringToJInteger2, _.domain_sessionidx = _),
      simpleTransform[String]("sid", CU.validateUuid, _.domain_sessionid = _),
      simpleTransform[String]("dtm", EE.extractTimestamp, _.dvce_created_tstamp = _),
      simpleTransform[String]("ttm", EE.extractTimestamp, _.true_tstamp = _),
      simpleTransform[String]("stm", EE.extractTimestamp, _.dvce_sent_tstamp = _),
      simpleTransform[String]("tna", ME.toTsvSafe, _.name_tracker = _),
      simpleTransform[String]("tv", ME.toTsvSafe, _.v_tracker = _),
      simpleTransform[String]("cv", ME.toTsvSafe, _.v_collector = _),
      simpleTransform[String]("lang", ME.toTsvSafe, _.br_lang = _),
      simpleTransform[JByte]("f_pdf", CU.stringToBooleanLikeJByte, _.br_features_pdf = _),
      simpleTransform[JByte]("f_fla", CU.stringToBooleanLikeJByte, _.br_features_flash = _),
      simpleTransform[JByte]("f_java", CU.stringToBooleanLikeJByte, _.br_features_java = _),
      simpleTransform[JByte]("f_dir", CU.stringToBooleanLikeJByte, _.br_features_director = _),
      simpleTransform[JByte]("f_qt", CU.stringToBooleanLikeJByte, _.br_features_quicktime = _),
      simpleTransform[JByte]("f_realp", CU.stringToBooleanLikeJByte, _.br_features_realplayer = _),
      simpleTransform[JByte]("f_wma", CU.stringToBooleanLikeJByte, _.br_features_windowsmedia = _),
      simpleTransform[JByte]("f_gears", CU.stringToBooleanLikeJByte, _.br_features_gears = _),
      simpleTransform[JByte]("f_ag", CU.stringToBooleanLikeJByte, _.br_features_silverlight = _),
      simpleTransform[JByte]("cookie", CU.stringToBooleanLikeJByte, _.br_cookies = _),
      tupleTransform("res", CE.extractViewDimensions, _.dvce_screenwidth = _, _.dvce_screenheight = _),
      simpleTransform[String]("cd", ME.toTsvSafe, _.br_colordepth = _),
      simpleTransform[String]("tz", ME.toTsvSafe, _.os_timezone = _),
      simpleTransform[String]("refr", ME.toTsvSafe, _.page_referrer = _),
      simpleTransform[String]("url", ME.toTsvSafe, _.page_url = _),
      simpleTransform[String]("page", ME.toTsvSafe, _.page_title = _),
      simpleTransform[String]("cs", ME.toTsvSafe, _.doc_charset = _),
      tupleTransform("ds", CE.extractViewDimensions, _.doc_width = _, _.doc_height = _),
      tupleTransform("vp", CE.extractViewDimensions, _.br_viewwidth = _, _.br_viewheight = _),
      simpleTransform[String]("eid", CU.validateUuid, _.event_id = _),
      // Custom structured events
      simpleTransform[String]("ev_ca", ME.toTsvSafe, _.se_category = _), // LEGACY tracker var. Leave for backwards compat
      simpleTransform[String]("ev_ac", ME.toTsvSafe, _.se_action = _), // LEGACY tracker var. Leave for backwards compat
      simpleTransform[String]("ev_la", ME.toTsvSafe, _.se_label = _), // LEGACY tracker var. Leave for backwards compat
      simpleTransform[String]("ev_pr", ME.toTsvSafe, _.se_property = _), // LEGACY tracker var. Leave for backwards compat
      simpleTransform[JBigDecimal]("ev_va", CU.stringToJBigDecimal2, _.se_value = _),
      // LEGACY tracker var. Leave for backwards compat
      simpleTransform[String]("se_ca", ME.toTsvSafe, _.se_category = _),
      simpleTransform[String]("se_ac", ME.toTsvSafe, _.se_action = _),
      simpleTransform[String]("se_la", ME.toTsvSafe, _.se_label = _),
      simpleTransform[String]("se_pr", ME.toTsvSafe, _.se_property = _),
      simpleTransform[JBigDecimal]("se_va", CU.stringToJBigDecimal2, _.se_value = _),
      // Ecommerce transactions
      simpleTransform[String]("tr_id", ME.toTsvSafe, _.tr_orderid = _),
      simpleTransform[String]("tr_af", ME.toTsvSafe, _.tr_affiliation = _),
      simpleTransform[JBigDecimal]("tr_tt", CU.stringToJBigDecimal2, _.tr_total = _),
      simpleTransform[JBigDecimal]("tr_tx", CU.stringToJBigDecimal2, _.tr_tax = _),
      simpleTransform[JBigDecimal]("tr_sh", CU.stringToJBigDecimal2, _.tr_shipping = _),
      simpleTransform[String]("tr_ci", ME.toTsvSafe, _.tr_city = _),
      simpleTransform[String]("tr_st", ME.toTsvSafe, _.tr_state = _),
      simpleTransform[String]("tr_co", ME.toTsvSafe, _.tr_country = _),
      // Ecommerce transaction items
      simpleTransform[String]("ti_id", ME.toTsvSafe, _.ti_orderid = _),
      simpleTransform[String]("ti_sk", ME.toTsvSafe, _.ti_sku = _),
      simpleTransform[String]("ti_na", ME.toTsvSafe, _.ti_name = _), // ERROR in Tracker Protocol
      simpleTransform[String]("ti_nm", ME.toTsvSafe, _.ti_name = _),
      simpleTransform[String]("ti_ca", ME.toTsvSafe, _.ti_category = _),
      simpleTransform[JBigDecimal]("ti_pr", CU.stringToJBigDecimal2, _.ti_price = _),
      simpleTransform[JInteger]("ti_qu", CU.stringToJInteger2, _.ti_quantity = _),
      // Page pings
      simpleTransform[JInteger]("pp_mix", CU.stringToJInteger2, _.pp_xoffset_min = _),
      simpleTransform[JInteger]("pp_max", CU.stringToJInteger2, _.pp_xoffset_max = _),
      simpleTransform[JInteger]("pp_miy", CU.stringToJInteger2, _.pp_yoffset_min = _),
      simpleTransform[JInteger]("pp_may", CU.stringToJInteger2, _.pp_yoffset_max = _),
      // Currency
      simpleTransform[String]("tr_cu", ME.toTsvSafe, _.tr_currency = _),
      simpleTransform[String]("ti_cu", ME.toTsvSafe, _.ti_currency = _)
    )

  private def simpleTransform[T](
    key: String,
    converter: (String, String) => Either[AtomicError.ParseError, T],
    setter: (EnrichedEvent, T) => Unit
  ): (String, AccumulatorFn) = {
    val fn: AccumulatorFn = {
      case (input, event, specials) =>
        converter(key, input) match {
          case Right(converted) =>
            setter(event, converted)
            (None, specials)
          case Left(error) =>
            (Some(error), specials)
        }
    }

    key -> fn
  }

  private def tupleTransform(
    key: String,
    converter: (String, String) => Either[AtomicError.ParseError, (JInteger, JInteger)],
    setter1: (EnrichedEvent, JInteger) => Unit,
    setter2: (EnrichedEvent, JInteger) => Unit
  ): (String, AccumulatorFn) = {
    val fn: AccumulatorFn = {
      case (input, event, specials) =>
        converter(key, input) match {
          case Right((converted1, converted2)) =>
            setter1(event, converted1)
            setter2(event, converted2)
            (None, specials)
          case Left(error) =>
            (Some(error), specials)
        }
    }

    key -> fn
  }

}
