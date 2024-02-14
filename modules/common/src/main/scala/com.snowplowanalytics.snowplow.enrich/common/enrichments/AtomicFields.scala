/*
 * Copyright (c) 2024-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments

import cats.data.NonEmptyList

import io.circe.syntax._

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.iglu.client.ClientError.ValidationError
import com.snowplowanalytics.iglu.client.validator.ValidatorError

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.AtomicFields.LimitedAtomicField
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.AtomicError

final case class AtomicFields(value: List[LimitedAtomicField])

object AtomicFields {

  val atomicSchema = SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1, 0, 0))

  final case class AtomicField(
    name: String,
    enrichedValueExtractor: EnrichedEvent => String,
    nullify: EnrichedEvent => Unit
  )
  final case class LimitedAtomicField(value: AtomicField, limit: Int)

  // format: off
  val supportedFields: List[AtomicField] =
    List(
      AtomicField(name = "app_id",             _.app_id            , _.app_id = null            ),
      AtomicField(name = "platform",           _.platform          , _.platform = null          ),
      AtomicField(name = "event",              _.event             , _.event = null             ),
      AtomicField(name = "event_id",           _.event_id          , _ => ()                    ), // required in loading
      AtomicField(name = "name_tracker",       _.name_tracker      , _.name_tracker = null      ),
      AtomicField(name = "v_tracker",          _.v_tracker         , _.v_tracker = null         ),
      AtomicField(name = "v_collector",        _.v_collector       , _ => ()                    ), // required in loading
      AtomicField(name = "v_etl",              _.v_etl             , _ => ()                    ), // required in loading
      AtomicField(name = "user_id",            _.user_id           , _.user_id = null           ),
      AtomicField(name = "user_ipaddress",     _.user_ipaddress    , _.user_ipaddress = null    ),
      AtomicField(name = "user_fingerprint",   _.user_fingerprint  , _.user_fingerprint = null  ),
      AtomicField(name = "domain_userid",      _.domain_userid     , _.domain_userid = null     ),
      AtomicField(name = "network_userid",     _.network_userid    , _.network_userid = null    ),
      AtomicField(name = "geo_country",        _.geo_country       , _.geo_country = null       ),
      AtomicField(name = "geo_region",         _.geo_region        , _.geo_region = null        ),
      AtomicField(name = "geo_city",           _.geo_city          , _.geo_city = null          ),
      AtomicField(name = "geo_zipcode",        _.geo_zipcode       , _.geo_zipcode = null       ),
      AtomicField(name = "geo_region_name",    _.geo_region_name   , _.geo_region_name = null   ),
      AtomicField(name = "ip_isp",             _.ip_isp            , _.ip_isp = null            ),
      AtomicField(name = "ip_organization",    _.ip_organization   , _.ip_organization = null   ),
      AtomicField(name = "ip_domain",          _.ip_domain         , _.ip_domain = null         ),
      AtomicField(name = "ip_netspeed",        _.ip_netspeed       , _.ip_netspeed = null       ),
      AtomicField(name = "page_url",           _.page_url          , _.page_url = null          ),
      AtomicField(name = "page_title",         _.page_title        , _.page_title = null        ),
      AtomicField(name = "page_referrer",      _.page_referrer     , _.page_referrer = null     ),
      AtomicField(name = "page_urlscheme",     _.page_urlscheme    , _.page_urlscheme = null    ),
      AtomicField(name = "page_urlhost",       _.page_urlhost      , _.page_urlhost = null      ),
      AtomicField(name = "page_urlpath",       _.page_urlpath      , _.page_urlpath = null      ),
      AtomicField(name = "page_urlquery",      _.page_urlquery     , _.page_urlquery = null     ),
      AtomicField(name = "page_urlfragment",   _.page_urlfragment  , _.page_urlfragment = null  ),
      AtomicField(name = "refr_urlscheme",     _.refr_urlscheme    , _.refr_urlscheme = null    ),
      AtomicField(name = "refr_urlhost",       _.refr_urlhost      , _.refr_urlhost = null      ),
      AtomicField(name = "refr_urlpath",       _.refr_urlpath      , _.refr_urlpath = null      ),
      AtomicField(name = "refr_urlquery",      _.refr_urlquery     , _.refr_urlquery = null     ),
      AtomicField(name = "refr_urlfragment",   _.refr_urlfragment  , _.refr_urlfragment = null  ),
      AtomicField(name = "refr_medium",        _.refr_medium       , _.refr_medium = null       ),
      AtomicField(name = "refr_source",        _.refr_source       , _.refr_source = null       ),
      AtomicField(name = "refr_term",          _.refr_term         , _.refr_term = null         ),
      AtomicField(name = "mkt_medium",         _.mkt_medium        , _.mkt_medium = null        ),
      AtomicField(name = "mkt_source",         _.mkt_source        , _.mkt_source = null        ),
      AtomicField(name = "mkt_term",           _.mkt_term          , _.mkt_term = null          ),
      AtomicField(name = "mkt_content",        _.mkt_content       , _.mkt_content = null       ),
      AtomicField(name = "mkt_campaign",       _.mkt_campaign      , _.mkt_campaign = null      ),
      AtomicField(name = "se_category",        _.se_category       , _.se_category = null       ),
      AtomicField(name = "se_action",          _.se_action         , _.se_action = null         ),
      AtomicField(name = "se_label",           _.se_label          , _.se_label = null          ),
      AtomicField(name = "se_property",        _.se_property       , _.se_property = null       ),
      AtomicField(name = "tr_orderid",         _.tr_orderid        , _.tr_orderid = null        ),
      AtomicField(name = "tr_affiliation",     _.tr_affiliation    , _.tr_affiliation = null    ),
      AtomicField(name = "tr_city",            _.tr_city           , _.tr_city = null           ),
      AtomicField(name = "tr_state",           _.tr_state          , _.tr_state = null          ),
      AtomicField(name = "tr_country",         _.tr_country        , _.tr_country = null        ),
      AtomicField(name = "ti_orderid",         _.ti_orderid        , _.ti_orderid = null        ),
      AtomicField(name = "ti_sku",             _.ti_sku            , _.ti_sku = null            ),
      AtomicField(name = "ti_name",            _.ti_name           , _.ti_name = null           ),
      AtomicField(name = "ti_category",        _.ti_category       , _.ti_category = null       ),
      AtomicField(name = "useragent",          _.useragent         , _.useragent = null         ),
      AtomicField(name = "br_name",            _.br_name           , _.br_name = null           ),
      AtomicField(name = "br_family",          _.br_family         , _.br_family = null         ),
      AtomicField(name = "br_version",         _.br_version        , _.br_version = null        ),
      AtomicField(name = "br_type",            _.br_type           , _.br_type = null           ),
      AtomicField(name = "br_renderengine",    _.br_renderengine   , _.br_renderengine = null   ),
      AtomicField(name = "br_lang",            _.br_lang           , _.br_lang = null           ),
      AtomicField(name = "br_colordepth",      _.br_colordepth     , _.br_colordepth = null     ),
      AtomicField(name = "os_name",            _.os_name           , _.os_name = null           ),
      AtomicField(name = "os_family",          _.os_family         , _.os_family = null         ),
      AtomicField(name = "os_manufacturer",    _.os_manufacturer   , _.os_manufacturer = null   ),
      AtomicField(name = "os_timezone",        _.os_timezone       , _.os_timezone = null       ),
      AtomicField(name = "dvce_type",          _.dvce_type         , _.dvce_type = null         ),
      AtomicField(name = "doc_charset",        _.doc_charset       , _.doc_charset = null       ),
      AtomicField(name = "tr_currency",        _.tr_currency       , _.tr_currency = null       ),
      AtomicField(name = "ti_currency",        _.ti_currency       , _.ti_currency = null       ),
      AtomicField(name = "base_currency",      _.base_currency     , _.base_currency = null     ),
      AtomicField(name = "geo_timezone",       _.geo_timezone      , _.geo_timezone = null      ),
      AtomicField(name = "mkt_clickid",        _.mkt_clickid       , _.mkt_clickid = null       ),
      AtomicField(name = "mkt_network",        _.mkt_network       , _.mkt_network = null       ),
      AtomicField(name = "etl_tags",           _.etl_tags          , _.etl_tags = null          ),
      AtomicField(name = "refr_domain_userid", _.refr_domain_userid, _.refr_domain_userid = null),
      AtomicField(name = "domain_sessionid",   _.domain_sessionid  , _.domain_sessionid = null  ),
      AtomicField(name = "event_vendor",       _.event_vendor      , _.event_vendor = null      ),
      AtomicField(name = "event_name",         _.event_name        , _.event_name = null        ),
      AtomicField(name = "event_format",       _.event_format      , _.event_format = null      ),
      AtomicField(name = "event_version",      _.event_version     , _.event_version = null     ),
      AtomicField(name = "event_fingerprint",  _.event_fingerprint , _.event_fingerprint = null )
    )
  // format: on

  def from(valueLimits: Map[String, Int]): AtomicFields = {
    val withLimits = supportedFields.map { field =>
      val limit = valueLimits.getOrElse(field.name, Int.MaxValue)
      LimitedAtomicField(field, limit)
    }

    AtomicFields(withLimits)
  }

  def errorsToSchemaViolation(errors: NonEmptyList[AtomicError]): Failure.SchemaViolation = {
    val clientError = ValidationError(ValidatorError.InvalidData(errors.map(_.toValidatorReport)), None)

    val failureData = errors.toList.map(e => e.field := e.value).toMap.asJson

    Failure.SchemaViolation(
      schemaViolation = FailureDetails.SchemaViolation.IgluError(
        AtomicFields.atomicSchema,
        clientError
      ),
      // Source atomic field and actual value of the field should be already on the ValidatorReport list
      source = AtomicError.source,
      data = failureData
    )
  }
}
