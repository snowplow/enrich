/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.common.outputs

import java.lang.{Integer => JInteger}
import java.lang.{Float => JFloat}
import java.lang.{Byte => JByte}
import java.time.format.DateTimeFormatter

import scala.beans.BeanProperty

import cats.implicits._

import io.circe.Json

import com.snowplowanalytics.snowplow.badrows.Payload.PartiallyEnrichedEvent

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

  // The application (site, game, app etc) this event belongs to, and the tracker platform
  @BeanProperty var app_id: String = _
  @BeanProperty var platform: String = _

  // Date/time
  @BeanProperty var etl_tstamp: String = _
  @BeanProperty var collector_tstamp: String = _
  @BeanProperty var dvce_created_tstamp: String = _

  // Transaction (i.e. this logging event)
  @BeanProperty var event: String = _
  @BeanProperty var event_id: String = _
  @BeanProperty var txn_id: String = _

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

  // Custom Contexts
  @BeanProperty var contexts: String = _

  // Structured Event
  @BeanProperty var se_category: String = _
  @BeanProperty var se_action: String = _
  @BeanProperty var se_label: String = _
  @BeanProperty var se_property: String = _
  @BeanProperty var se_value: String =
    _ // Technically should be a Double but may be rendered incorrectly by Cascading with scientific notification (which Redshift can't process)

  // Unstructured Event
  @BeanProperty var unstruct_event: String = _

  // Ecommerce transaction (from querystring)
  @BeanProperty var tr_orderid: String = _
  @BeanProperty var tr_affiliation: String = _
  @BeanProperty var tr_total: String = _
  @BeanProperty var tr_tax: String = _
  @BeanProperty var tr_shipping: String = _
  @BeanProperty var tr_city: String = _
  @BeanProperty var tr_state: String = _
  @BeanProperty var tr_country: String = _

  // Ecommerce transaction item (from querystring)
  @BeanProperty var ti_orderid: String = _
  @BeanProperty var ti_sku: String = _
  @BeanProperty var ti_name: String = _
  @BeanProperty var ti_category: String = _
  @BeanProperty var ti_price: String = _
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
  @BeanProperty var tr_total_base: String = _
  @BeanProperty var tr_tax_base: String = _
  @BeanProperty var tr_shipping_base: String = _
  @BeanProperty var ti_currency: String = _
  @BeanProperty var ti_price_base: String = _
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

  // Derived contexts
  @BeanProperty var derived_contexts: String = _

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

  // Fields modified in PII enrichemnt (JSON String)
  @BeanProperty var pii: String = _

  private val JsonSchemaDateTimeFormat =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  private def toKv[T](
    k: String,
    v: T,
    f: T => Json
  ): Option[(String, Json)] =
    Option(v).map(value => (k, f(value)))

  private def toKv(k: String, s: String): Option[(String, Json)] = toKv(k, s, Json.fromString)
  private def toKv(k: String, i: JInteger): Option[(String, Json)] = toKv(k, i, (jInt: JInteger) => Json.fromInt(jInt))
  private def toKv(k: String, f: JFloat): Option[(String, Json)] = toKv(k, f, (jFloat: JFloat) => Json.fromFloatOrNull(jFloat))
  private def toKv(k: String, b: JByte): Option[(String, Json)] = toKv(k, b, (jByte: JByte) => Json.fromBoolean(jByte != 0))
  private def toDateKv(k: String, s: String): Option[(String, Json)] =
    toKv(
      k,
      s,
      (s: String) => Json.fromString(DateTimeFormatter.ISO_DATE_TIME.format(JsonSchemaDateTimeFormat.parse(s)))
    )

  def toAtomicEvent: Either[Throwable, Json] =
    Either.catchNonFatal(
      Json.fromFields(
        toKv("app_id", this.app_id) ++
          toKv("platform", this.platform) ++
          toDateKv("etl_tstamp", this.etl_tstamp) ++
          toDateKv("collector_tstamp", this.collector_tstamp) ++
          toDateKv("dvce_created_tstamp", this.dvce_created_tstamp) ++
          toKv("event", this.event) ++
          toKv("event_id", this.event_id) ++
          toKv("txn_id", this.txn_id) ++
          toKv("name_tracker", this.name_tracker) ++
          toKv("v_tracker", this.v_tracker) ++
          toKv("v_collector", this.v_collector) ++
          toKv("v_etl", this.v_etl) ++
          toKv("user_id", this.user_id) ++
          toKv("user_ipaddress", this.user_ipaddress) ++
          toKv("user_fingerprint", this.user_fingerprint) ++
          toKv("domain_userid", this.domain_userid) ++
          toKv("domain_sessionidx", this.domain_sessionidx) ++
          toKv("network_userid", this.network_userid) ++
          toKv("geo_country", this.geo_country) ++
          toKv("geo_region", this.geo_region) ++
          toKv("geo_city", this.geo_city) ++
          toKv("geo_zipcode", this.geo_zipcode) ++
          toKv("geo_latitude", this.geo_latitude) ++
          toKv("geo_longitude", this.geo_longitude) ++
          toKv("geo_region_name", this.geo_region_name) ++
          toKv("ip_isp", this.ip_isp) ++
          toKv("ip_organization", this.ip_organization) ++
          toKv("ip_domain", this.ip_domain) ++
          toKv("ip_netspeed", this.ip_netspeed) ++
          toKv("page_url", this.page_url) ++
          toKv("page_title", this.page_title) ++
          toKv("page_referrer", this.page_referrer) ++
          toKv("page_urlscheme", this.page_urlscheme) ++
          toKv("page_urlhost", this.page_urlhost) ++
          toKv("page_urlport", this.page_urlport) ++
          toKv("page_urlpath", this.page_urlpath) ++
          toKv("page_urlquery", this.page_urlquery) ++
          toKv("page_urlfragment", this.page_urlfragment) ++
          toKv("refr_urlscheme", this.refr_urlscheme) ++
          toKv("refr_urlhost", this.refr_urlhost) ++
          toKv("refr_urlport", this.refr_urlport) ++
          toKv("refr_urlpath", this.refr_urlpath) ++
          toKv("refr_urlquery", this.refr_urlquery) ++
          toKv("refr_urlfragment", this.refr_urlfragment) ++
          toKv("refr_medium", this.refr_medium) ++
          toKv("refr_source", this.refr_source) ++
          toKv("refr_term", this.refr_term) ++
          toKv("mkt_medium", this.mkt_medium) ++
          toKv("mkt_source", this.mkt_source) ++
          toKv("mkt_term", this.mkt_term) ++
          toKv("mkt_content", this.mkt_content) ++
          toKv("mkt_campaign", this.mkt_campaign) ++
          toKv("se_category", this.se_category) ++
          toKv("se_action", this.se_action) ++
          toKv("se_label", this.se_label) ++
          toKv("se_property", this.se_property) ++
          toKv("se_value", this.se_value) ++
          toKv("tr_orderid", this.tr_orderid) ++
          toKv("tr_affiliation", this.tr_affiliation) ++
          toKv("tr_total", this.tr_total) ++
          toKv("tr_tax", this.tr_tax) ++
          toKv("tr_shipping", this.tr_shipping) ++
          toKv("tr_city", this.tr_city) ++
          toKv("tr_state", this.tr_state) ++
          toKv("tr_country", this.tr_country) ++
          toKv("ti_orderid", this.ti_orderid) ++
          toKv("ti_sku", this.ti_sku) ++
          toKv("ti_name", this.ti_name) ++
          toKv("ti_category", this.ti_category) ++
          toKv("ti_price", this.ti_price) ++
          toKv("ti_quantity", this.ti_quantity) ++
          toKv("pp_xoffset_min", this.pp_xoffset_min) ++
          toKv("pp_xoffset_max", this.pp_xoffset_max) ++
          toKv("pp_yoffset_min", this.pp_yoffset_min) ++
          toKv("pp_yoffset_max", this.pp_yoffset_max) ++
          toKv("useragent", this.useragent) ++
          toKv("br_name", this.br_name) ++
          toKv("br_family", this.br_family) ++
          toKv("br_version", this.br_version) ++
          toKv("br_type", this.br_type) ++
          toKv("br_renderengine", this.br_renderengine) ++
          toKv("br_lang", this.br_lang) ++
          toKv("br_features_pdf", this.br_features_pdf) ++
          toKv("br_features_flash", this.br_features_flash) ++
          toKv("br_features_java", this.br_features_java) ++
          toKv("br_features_director", this.br_features_director) ++
          toKv("br_features_quicktime", this.br_features_quicktime) ++
          toKv("br_features_realplayer", this.br_features_realplayer) ++
          toKv("br_features_windowsmedia", this.br_features_windowsmedia) ++
          toKv("br_features_gears", this.br_features_gears) ++
          toKv("br_features_silverlight", this.br_features_silverlight) ++
          toKv("br_cookies", this.br_cookies) ++
          toKv("br_colordepth", this.br_colordepth) ++
          toKv("br_viewwidth", this.br_viewwidth) ++
          toKv("br_viewheight", this.br_viewheight) ++
          toKv("os_name", this.os_name) ++
          toKv("os_family", this.os_family) ++
          toKv("os_manufacturer", this.os_manufacturer) ++
          toKv("os_timezone", this.os_timezone) ++
          toKv("dvce_type", this.dvce_type) ++
          toKv("dvce_ismobile", this.dvce_ismobile) ++
          toKv("dvce_screenwidth", this.dvce_screenwidth) ++
          toKv("dvce_screenheight", this.dvce_screenheight) ++
          toKv("doc_charset", this.doc_charset) ++
          toKv("doc_width", this.doc_width) ++
          toKv("doc_height", this.doc_height) ++
          toKv("tr_currency", this.tr_currency) ++
          toKv("tr_total_base", this.tr_total_base) ++
          toKv("tr_tax_base", this.tr_tax_base) ++
          toKv("tr_shipping_base", this.tr_shipping_base) ++
          toKv("ti_currency", this.ti_currency) ++
          toKv("ti_price_base", this.ti_price_base) ++
          toKv("base_currency", this.base_currency) ++
          toKv("geo_timezone", this.geo_timezone) ++
          toKv("mkt_clickid", this.mkt_clickid) ++
          toKv("mkt_network", this.mkt_network) ++
          toKv("etl_tags", this.etl_tags) ++
          toDateKv("dvce_sent_tstamp", this.dvce_sent_tstamp) ++
          toKv("refr_domain_userid", this.refr_domain_userid) ++
          toDateKv("refr_dvce_tstamp", this.refr_dvce_tstamp) ++
          toKv("domain_sessionid", this.domain_sessionid) ++
          toDateKv("derived_tstamp", this.derived_tstamp) ++
          toKv("event_vendor", this.event_vendor) ++
          toKv("event_name", this.event_name) ++
          toKv("event_format", this.event_format) ++
          toKv("event_version", this.event_version) ++
          toKv("event_fingerprint", this.event_fingerprint) ++
          toDateKv("true_tstamp", this.true_tstamp)
      )
    )

}

object EnrichedEvent {

  def toPartiallyEnrichedEvent(enrichedEvent: EnrichedEvent): PartiallyEnrichedEvent =
    PartiallyEnrichedEvent(
      app_id = Option(enrichedEvent.app_id),
      platform = Option(enrichedEvent.platform),
      etl_tstamp = Option(enrichedEvent.etl_tstamp),
      collector_tstamp = Option(enrichedEvent.collector_tstamp),
      dvce_created_tstamp = Option(enrichedEvent.dvce_created_tstamp),
      event = Option(enrichedEvent.event),
      event_id = Option(enrichedEvent.event_id),
      txn_id = Option(enrichedEvent.txn_id),
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
      contexts = Option(enrichedEvent.contexts),
      se_category = Option(enrichedEvent.se_category),
      se_action = Option(enrichedEvent.se_action),
      se_label = Option(enrichedEvent.se_label),
      se_property = Option(enrichedEvent.se_property),
      se_value = Option(enrichedEvent.se_value),
      unstruct_event = Option(enrichedEvent.unstruct_event),
      tr_orderid = Option(enrichedEvent.tr_orderid),
      tr_affiliation = Option(enrichedEvent.tr_affiliation),
      tr_total = Option(enrichedEvent.tr_total),
      tr_tax = Option(enrichedEvent.tr_tax),
      tr_shipping = Option(enrichedEvent.tr_shipping),
      tr_city = Option(enrichedEvent.tr_city),
      tr_state = Option(enrichedEvent.tr_state),
      tr_country = Option(enrichedEvent.tr_country),
      ti_orderid = Option(enrichedEvent.ti_orderid),
      ti_sku = Option(enrichedEvent.ti_sku),
      ti_name = Option(enrichedEvent.ti_name),
      ti_category = Option(enrichedEvent.ti_category),
      ti_price = Option(enrichedEvent.ti_price),
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
      tr_total_base = Option(enrichedEvent.tr_total_base),
      tr_tax_base = Option(enrichedEvent.tr_tax_base),
      tr_shipping_base = Option(enrichedEvent.tr_shipping_base),
      ti_currency = Option(enrichedEvent.ti_currency),
      ti_price_base = Option(enrichedEvent.ti_price_base),
      base_currency = Option(enrichedEvent.base_currency),
      geo_timezone = Option(enrichedEvent.geo_timezone),
      mkt_clickid = Option(enrichedEvent.mkt_clickid),
      mkt_network = Option(enrichedEvent.mkt_network),
      etl_tags = Option(enrichedEvent.etl_tags),
      dvce_sent_tstamp = Option(enrichedEvent.dvce_sent_tstamp),
      refr_domain_userid = Option(enrichedEvent.refr_domain_userid),
      refr_dvce_tstamp = Option(enrichedEvent.refr_dvce_tstamp),
      derived_contexts = Option(enrichedEvent.derived_contexts),
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
