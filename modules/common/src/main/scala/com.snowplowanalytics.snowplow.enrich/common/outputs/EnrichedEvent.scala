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
package com.snowplowanalytics.snowplow.enrich.common.outputs

import java.lang.{Integer => JInteger}
import java.lang.{Float => JFloat}
import java.lang.{Byte => JByte}
import java.lang.{Boolean => JBoolean}
import java.math.{BigDecimal => JBigDecimal}
import java.lang.reflect.Field

import io.circe.{Decoder, Json}
import io.circe.parser._

import scala.beans.BeanProperty

import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.snowplow.badrows.Payload.PartiallyEnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.JavascriptScriptEnrichment.JavascriptRejectionException
import com.snowplowanalytics.snowplow.enrich.common.enrichments.MiscEnrichments

/**
 * The canonical output format for enriched events.
 *
 * For simplicity, we are using our Redshift format
 * as the canonical format, i.e. the below is
 * equivalent to the redshift-etl.q HiveQL script
 * used by the Hive ETL.
 *
 * When we move to Avro, we will
 * probably review some of these
 * types (e.g. move back to
 * Array for browser features, and
 * switch remaining Bytes to Booleans).
 */
// TODO: make the EnrichedEvent Avro-format, not Redshift-specific
class EnrichedEvent extends Serializable {
  import EnrichedEvent._

  // The application (site, game, app etc) enriched event belongs to, and the tracker platform
  @BeanProperty var app_id: String = _
  @BeanProperty var platform: String = _

  // Date/time
  @BeanProperty var etl_tstamp: String = _
  @BeanProperty var collector_tstamp: String = _
  @BeanProperty var dvce_created_tstamp: String = _

  // Transaction (i.e. enriched logging event)
  @BeanProperty var event: String = _
  @BeanProperty var event_id: String = _
  @BeanProperty var txn_id: JInteger = _

  // Versioning
  @BeanProperty var name_tracker: String = _
  @BeanProperty var v_tracker: String = _
  @BeanProperty var v_collector: String = _
  @BeanProperty var v_etl: String = _

  // User and visit
  @BeanProperty var user_id: String = _
  @BeanProperty var user_ipaddress: String = _
  @BeanProperty var user_fingerprint: String = _
  @BeanProperty var domain_userid: String = _
  @BeanProperty var domain_sessionidx: JInteger = _
  @BeanProperty var network_userid: String = _

  // Location
  @BeanProperty var geo_country: String = _
  @BeanProperty var geo_region: String = _
  @BeanProperty var geo_city: String = _
  @BeanProperty var geo_zipcode: String = _
  @BeanProperty var geo_latitude: JFloat = _
  @BeanProperty var geo_longitude: JFloat = _
  @BeanProperty var geo_region_name: String = _

  // Other IP lookups
  @BeanProperty var ip_isp: String = _
  @BeanProperty var ip_organization: String = _
  @BeanProperty var ip_domain: String = _
  @BeanProperty var ip_netspeed: String = _

  // Page
  @BeanProperty var page_url: String = _
  @BeanProperty var page_title: String = _
  @BeanProperty var page_referrer: String = _

  // Page URL components
  @BeanProperty var page_urlscheme: String = _
  @BeanProperty var page_urlhost: String = _
  @BeanProperty var page_urlport: JInteger = _
  @BeanProperty var page_urlpath: String = _
  @BeanProperty var page_urlquery: String = _
  @BeanProperty var page_urlfragment: String = _

  // Referrer URL components
  @BeanProperty var refr_urlscheme: String = _
  @BeanProperty var refr_urlhost: String = _
  @BeanProperty var refr_urlport: JInteger = _
  @BeanProperty var refr_urlpath: String = _
  @BeanProperty var refr_urlquery: String = _
  @BeanProperty var refr_urlfragment: String = _

  // Referrer details
  @BeanProperty var refr_medium: String = _
  @BeanProperty var refr_source: String = _
  @BeanProperty var refr_term: String = _

  // Marketing
  @BeanProperty var mkt_medium: String = _
  @BeanProperty var mkt_source: String = _
  @BeanProperty var mkt_term: String = _
  @BeanProperty var mkt_content: String = _
  @BeanProperty var mkt_campaign: String = _

  // Structured Event
  @BeanProperty var se_category: String = _
  @BeanProperty var se_action: String = _
  @BeanProperty var se_label: String = _
  @BeanProperty var se_property: String = _
  @BeanProperty var se_value: JBigDecimal = _

  // Ecommerce transaction (from querystring)
  @BeanProperty var tr_orderid: String = _
  @BeanProperty var tr_affiliation: String = _
  @BeanProperty var tr_total: JBigDecimal = _
  @BeanProperty var tr_tax: JBigDecimal = _
  @BeanProperty var tr_shipping: JBigDecimal = _
  @BeanProperty var tr_city: String = _
  @BeanProperty var tr_state: String = _
  @BeanProperty var tr_country: String = _

  // Ecommerce transaction item (from querystring)
  @BeanProperty var ti_orderid: String = _
  @BeanProperty var ti_sku: String = _
  @BeanProperty var ti_name: String = _
  @BeanProperty var ti_category: String = _
  @BeanProperty var ti_price: JBigDecimal = _
  @BeanProperty var ti_quantity: JInteger = _

  // Page Pings
  @BeanProperty var pp_xoffset_min: JInteger = _
  @BeanProperty var pp_xoffset_max: JInteger = _
  @BeanProperty var pp_yoffset_min: JInteger = _
  @BeanProperty var pp_yoffset_max: JInteger = _

  // User Agent
  @BeanProperty var useragent: String = _

  // Browser (from user-agent)
  @BeanProperty var br_name: String = _
  @BeanProperty var br_family: String = _
  @BeanProperty var br_version: String = _
  @BeanProperty var br_type: String = _
  @BeanProperty var br_renderengine: String = _

  // Browser (from querystring)
  @BeanProperty var br_lang: String = _
  // Individual feature fields for non-Hive targets (e.g. Infobright)
  @BeanProperty var br_features_pdf: JByte = _
  @BeanProperty var br_features_flash: JByte = _
  @BeanProperty var br_features_java: JByte = _
  @BeanProperty var br_features_director: JByte = _
  @BeanProperty var br_features_quicktime: JByte = _
  @BeanProperty var br_features_realplayer: JByte = _
  @BeanProperty var br_features_windowsmedia: JByte = _
  @BeanProperty var br_features_gears: JByte = _
  @BeanProperty var br_features_silverlight: JByte = _
  @BeanProperty var br_cookies: JByte = _
  @BeanProperty var br_colordepth: String = _
  @BeanProperty var br_viewwidth: JInteger = _
  @BeanProperty var br_viewheight: JInteger = _

  // OS (from user-agent)
  @BeanProperty var os_name: String = _
  @BeanProperty var os_family: String = _
  @BeanProperty var os_manufacturer: String = _
  @BeanProperty var os_timezone: String = _

  // Device/Hardware (from user-agent)
  @BeanProperty var dvce_type: String = _
  @BeanProperty var dvce_ismobile: JByte = _

  // Device (from querystring)
  @BeanProperty var dvce_screenwidth: JInteger = _
  @BeanProperty var dvce_screenheight: JInteger = _

  // Document
  @BeanProperty var doc_charset: String = _
  @BeanProperty var doc_width: JInteger = _
  @BeanProperty var doc_height: JInteger = _

  // Currency
  @BeanProperty var tr_currency: String = _
  @BeanProperty var tr_total_base: JBigDecimal = _
  @BeanProperty var tr_tax_base: JBigDecimal = _
  @BeanProperty var tr_shipping_base: JBigDecimal = _
  @BeanProperty var ti_currency: String = _
  @BeanProperty var ti_price_base: JBigDecimal = _
  @BeanProperty var base_currency: String = _

  // Geolocation
  @BeanProperty var geo_timezone: String = _

  // Click ID
  @BeanProperty var mkt_clickid: String = _
  @BeanProperty var mkt_network: String = _

  // ETL tags
  @BeanProperty var etl_tags: String = _

  // Time event was sent
  @BeanProperty var dvce_sent_tstamp: String = _

  // Referer
  @BeanProperty var refr_domain_userid: String = _
  @BeanProperty var refr_dvce_tstamp: String = _

  // Session ID
  @BeanProperty var domain_sessionid: String = _

  // Derived timestamp
  @BeanProperty var derived_tstamp: String = _

  // Derived event vendor/name/format/version
  @BeanProperty var event_vendor: String = _
  @BeanProperty var event_name: String = _
  @BeanProperty var event_format: String = _
  @BeanProperty var event_version: String = _

  // Event fingerprint
  @BeanProperty var event_fingerprint: String = _

  // True timestamp
  @BeanProperty var true_tstamp: String = _

  // The following fields are not @BeanProperty because we have customized getters/setters below
  private[enrich] var unstruct_event: Option[SelfDescribingData[Json]] = None
  private[enrich] var contexts: List[SelfDescribingData[Json]] = Nil
  private[enrich] var derived_contexts: List[SelfDescribingData[Json]] = Nil
  private[enrich] var pii: Option[SelfDescribingData[Json]] = None

  // Specifies whether derived contexts only from JS enrichment should be used
  // and all the other derived contexts should be ignored
  private[enrich] var use_derived_contexts_from_js_enrichment_only: JBoolean = _

  // The following Getters/Setters are for use in the Javascript enrichment only

  def getContexts(): String =
    MiscEnrichments.formatContexts(this.contexts).orNull

  def getUnstruct_event(): String =
    MiscEnrichments.formatUnstructEvent(this.unstruct_event).orNull

  def getDerived_contexts(): String =
    MiscEnrichments.formatContexts(this.derived_contexts).orNull

  def getPii(): String =
    MiscEnrichments.formatUnstructEvent(this.pii).orNull

  def setContexts(strOrNull: String): Unit =
    this.contexts = Option(strOrNull)
      .map { str =>
        decode[SelfDescribingData[List[SelfDescribingData[Json]]]](str) match {
          case Right(sdj) => sdj.data
          case Left(e) => throw new IllegalArgumentException("invalid contexts json", e)
        }
      }
      .getOrElse(Nil)

  def setUnstruct_event(strOrNull: String): Unit =
    this.unstruct_event = Option(strOrNull).map { str =>
      decode[SelfDescribingData[SelfDescribingData[Json]]](str) match {
        case Right(sdj) => sdj.data
        case Left(e) => throw new IllegalArgumentException("invalid unstruct_event json", e)
      }
    }

  def setPii(strOrNull: String): Unit =
    this.pii = Option(strOrNull).map { str =>
      decode[SelfDescribingData[SelfDescribingData[Json]]](str) match {
        case Right(sdj) => sdj.data
        case Left(e) => throw new IllegalArgumentException("invalid pii json", e)
      }
    }

  // Not supported in JS enrichment
  def setDerived_contexts(strOrNull: String): Unit = {
    val _ = strOrNull
  }

  // This method can be called from JS enrichment script to drop an event.
  // Raised exception will be caught by JS enrichment and event will be dropped.
  def drop(): Unit =
    throw new JavascriptRejectionException

  def eraseDerived_contexts(): Unit =
    use_derived_contexts_from_js_enrichment_only = true

}

object EnrichedEvent {

  private[enrich] val atomicFields: List[Field] =
    classOf[EnrichedEvent].getDeclaredFields
      .filterNot(f =>
        Set("pii", "contexts", "unstruct_event", "derived_contexts", "use_derived_contexts_from_js_enrichment_only").contains(f.getName)
      )
      .map { f =>
        f.setAccessible(true)
        f
      }
      .toList

  private implicit def unstructEventDecoder: Decoder[SelfDescribingData[SelfDescribingData[Json]]] =
    for {
      key <- Decoder[SchemaKey].at("schema")
      sdj <- Decoder[SelfDescribingData[Json]].at("data")
    } yield SelfDescribingData(key, sdj)

  private implicit def contextsDecoder: Decoder[SelfDescribingData[List[SelfDescribingData[Json]]]] =
    for {
      key <- Decoder[SchemaKey].at("schema")
      sdj <- Decoder[List[SelfDescribingData[Json]]].at("data")
    } yield SelfDescribingData(key, sdj)

  def toPartiallyEnrichedEvent(enrichedEvent: EnrichedEvent): PartiallyEnrichedEvent =
    PartiallyEnrichedEvent(
      app_id = Option(enrichedEvent.app_id),
      platform = Option(enrichedEvent.platform),
      etl_tstamp = Option(enrichedEvent.etl_tstamp),
      collector_tstamp = Option(enrichedEvent.collector_tstamp),
      dvce_created_tstamp = Option(enrichedEvent.dvce_created_tstamp),
      event = Option(enrichedEvent.event),
      event_id = Option(enrichedEvent.event_id),
      txn_id = Option(enrichedEvent.txn_id).map(_.toString),
      name_tracker = Option(enrichedEvent.name_tracker),
      v_tracker = Option(enrichedEvent.v_tracker),
      v_collector = Option(enrichedEvent.v_collector),
      v_etl = Option(enrichedEvent.v_etl),
      user_id = Option(enrichedEvent.user_id),
      user_ipaddress = Option(enrichedEvent.user_ipaddress),
      user_fingerprint = Option(enrichedEvent.user_fingerprint),
      domain_userid = Option(enrichedEvent.domain_userid),
      domain_sessionidx = Option(enrichedEvent.domain_sessionidx).map(Integer2int),
      network_userid = Option(enrichedEvent.network_userid),
      geo_country = Option(enrichedEvent.geo_country),
      geo_region = Option(enrichedEvent.geo_region),
      geo_city = Option(enrichedEvent.geo_city),
      geo_zipcode = Option(enrichedEvent.geo_zipcode),
      geo_latitude = Option(enrichedEvent.geo_latitude).map(Float2float),
      geo_longitude = Option(enrichedEvent.geo_longitude).map(Float2float),
      geo_region_name = Option(enrichedEvent.geo_region_name),
      ip_isp = Option(enrichedEvent.ip_isp),
      ip_organization = Option(enrichedEvent.ip_organization),
      ip_domain = Option(enrichedEvent.ip_domain),
      ip_netspeed = Option(enrichedEvent.ip_netspeed),
      page_url = Option(enrichedEvent.page_url),
      page_title = Option(enrichedEvent.page_title),
      page_referrer = Option(enrichedEvent.page_referrer),
      page_urlscheme = Option(enrichedEvent.page_urlscheme),
      page_urlhost = Option(enrichedEvent.page_urlhost),
      page_urlport = Option(enrichedEvent.page_urlport).map(Integer2int),
      page_urlpath = Option(enrichedEvent.page_urlpath),
      page_urlquery = Option(enrichedEvent.page_urlquery),
      page_urlfragment = Option(enrichedEvent.page_urlfragment),
      refr_urlscheme = Option(enrichedEvent.refr_urlscheme),
      refr_urlhost = Option(enrichedEvent.refr_urlhost),
      refr_urlport = Option(enrichedEvent.refr_urlport).map(Integer2int),
      refr_urlpath = Option(enrichedEvent.refr_urlpath),
      refr_urlquery = Option(enrichedEvent.refr_urlquery),
      refr_urlfragment = Option(enrichedEvent.refr_urlfragment),
      refr_medium = Option(enrichedEvent.refr_medium),
      refr_source = Option(enrichedEvent.refr_source),
      refr_term = Option(enrichedEvent.refr_term),
      mkt_medium = Option(enrichedEvent.mkt_medium),
      mkt_source = Option(enrichedEvent.mkt_source),
      mkt_term = Option(enrichedEvent.mkt_term),
      mkt_content = Option(enrichedEvent.mkt_content),
      mkt_campaign = Option(enrichedEvent.mkt_campaign),
      contexts = Option(enrichedEvent.getContexts()),
      se_category = Option(enrichedEvent.se_category),
      se_action = Option(enrichedEvent.se_action),
      se_label = Option(enrichedEvent.se_label),
      se_property = Option(enrichedEvent.se_property),
      se_value = Option(enrichedEvent.se_value).map(_.toString),
      unstruct_event = Option(enrichedEvent.getUnstruct_event()),
      tr_orderid = Option(enrichedEvent.tr_orderid),
      tr_affiliation = Option(enrichedEvent.tr_affiliation),
      tr_total = Option(enrichedEvent.tr_total).map(_.toString),
      tr_tax = Option(enrichedEvent.tr_tax).map(_.toString),
      tr_shipping = Option(enrichedEvent.tr_shipping).map(_.toString),
      tr_city = Option(enrichedEvent.tr_city),
      tr_state = Option(enrichedEvent.tr_state),
      tr_country = Option(enrichedEvent.tr_country),
      ti_orderid = Option(enrichedEvent.ti_orderid),
      ti_sku = Option(enrichedEvent.ti_sku),
      ti_name = Option(enrichedEvent.ti_name),
      ti_category = Option(enrichedEvent.ti_category),
      ti_price = Option(enrichedEvent.ti_price).map(_.toString),
      ti_quantity = Option(enrichedEvent.ti_quantity).map(Integer2int),
      pp_xoffset_min = Option(enrichedEvent.pp_xoffset_min).map(Integer2int),
      pp_xoffset_max = Option(enrichedEvent.pp_xoffset_max).map(Integer2int),
      pp_yoffset_min = Option(enrichedEvent.pp_yoffset_min).map(Integer2int),
      pp_yoffset_max = Option(enrichedEvent.pp_yoffset_max).map(Integer2int),
      useragent = Option(enrichedEvent.useragent),
      br_name = Option(enrichedEvent.br_name),
      br_family = Option(enrichedEvent.br_family),
      br_version = Option(enrichedEvent.br_version),
      br_type = Option(enrichedEvent.br_type),
      br_renderengine = Option(enrichedEvent.br_renderengine),
      br_lang = Option(enrichedEvent.br_lang),
      br_features_pdf = Option(enrichedEvent.br_features_pdf).map(Byte2byte),
      br_features_flash = Option(enrichedEvent.br_features_flash).map(Byte2byte),
      br_features_java = Option(enrichedEvent.br_features_java).map(Byte2byte),
      br_features_director = Option(enrichedEvent.br_features_director).map(Byte2byte),
      br_features_quicktime = Option(enrichedEvent.br_features_quicktime).map(Byte2byte),
      br_features_realplayer = Option(enrichedEvent.br_features_realplayer).map(Byte2byte),
      br_features_windowsmedia = Option(enrichedEvent.br_features_windowsmedia).map(Byte2byte),
      br_features_gears = Option(enrichedEvent.br_features_gears).map(Byte2byte),
      br_features_silverlight = Option(enrichedEvent.br_features_silverlight).map(Byte2byte),
      br_cookies = Option(enrichedEvent.br_cookies).map(Byte2byte),
      br_colordepth = Option(enrichedEvent.br_colordepth),
      br_viewwidth = Option(enrichedEvent.br_viewwidth).map(Integer2int),
      br_viewheight = Option(enrichedEvent.br_viewheight).map(Integer2int),
      os_name = Option(enrichedEvent.os_name),
      os_family = Option(enrichedEvent.os_family),
      os_manufacturer = Option(enrichedEvent.os_manufacturer),
      os_timezone = Option(enrichedEvent.os_timezone),
      dvce_type = Option(enrichedEvent.dvce_type),
      dvce_ismobile = Option(enrichedEvent.dvce_ismobile).map(Byte2byte),
      dvce_screenwidth = Option(enrichedEvent.dvce_screenwidth).map(Integer2int),
      dvce_screenheight = Option(enrichedEvent.dvce_screenheight).map(Integer2int),
      doc_charset = Option(enrichedEvent.doc_charset),
      doc_width = Option(enrichedEvent.doc_width).map(Integer2int),
      doc_height = Option(enrichedEvent.doc_height).map(Integer2int),
      tr_currency = Option(enrichedEvent.tr_currency),
      tr_total_base = Option(enrichedEvent.tr_total_base).map(_.toString),
      tr_tax_base = Option(enrichedEvent.tr_tax_base).map(_.toString),
      tr_shipping_base = Option(enrichedEvent.tr_shipping_base).map(_.toString),
      ti_currency = Option(enrichedEvent.ti_currency),
      ti_price_base = Option(enrichedEvent.ti_price_base).map(_.toString),
      base_currency = Option(enrichedEvent.base_currency),
      geo_timezone = Option(enrichedEvent.geo_timezone),
      mkt_clickid = Option(enrichedEvent.mkt_clickid),
      mkt_network = Option(enrichedEvent.mkt_network),
      etl_tags = Option(enrichedEvent.etl_tags),
      dvce_sent_tstamp = Option(enrichedEvent.dvce_sent_tstamp),
      refr_domain_userid = Option(enrichedEvent.refr_domain_userid),
      refr_dvce_tstamp = Option(enrichedEvent.refr_dvce_tstamp),
      derived_contexts = Option(enrichedEvent.getDerived_contexts()),
      domain_sessionid = Option(enrichedEvent.domain_sessionid),
      derived_tstamp = Option(enrichedEvent.derived_tstamp),
      event_vendor = Option(enrichedEvent.event_vendor),
      event_name = Option(enrichedEvent.event_name),
      event_format = Option(enrichedEvent.event_format),
      event_version = Option(enrichedEvent.event_version),
      event_fingerprint = Option(enrichedEvent.event_fingerprint),
      true_tstamp = Option(enrichedEvent.true_tstamp)
    )
}
