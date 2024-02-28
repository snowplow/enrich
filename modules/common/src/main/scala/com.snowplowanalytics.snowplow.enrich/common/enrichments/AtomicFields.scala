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

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.iglu.client.ClientError.ValidationError
import com.snowplowanalytics.iglu.client.validator.{ValidatorError, ValidatorReport}

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.AtomicFields.LimitedAtomicField
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

final case class AtomicFields(value: List[LimitedAtomicField])

object AtomicFields {

  val atomicSchema = SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1, 0, 0))

  final case class AtomicField(
    name: String,
    enrichedValueExtractor: EnrichedEvent => String
  )
  final case class LimitedAtomicField(value: AtomicField, limit: Int)

  // format: off
  val supportedFields: List[AtomicField] =
    List(
      AtomicField(name = "app_id",             _.app_id             ),
      AtomicField(name = "platform",           _.platform           ),
      AtomicField(name = "event",              _.event              ),
      AtomicField(name = "event_id",           _.event              ),
      AtomicField(name = "name_tracker",       _.name_tracker       ),
      AtomicField(name = "v_tracker",          _.v_tracker          ),
      AtomicField(name = "v_collector",        _.v_collector        ),
      AtomicField(name = "v_etl",              _.v_etl              ),
      AtomicField(name = "user_id",            _.user_id            ),
      AtomicField(name = "user_ipaddress",     _.user_ipaddress     ),
      AtomicField(name = "user_fingerprint",   _.user_fingerprint   ),
      AtomicField(name = "domain_userid",      _.domain_userid      ),
      AtomicField(name = "network_userid",     _.network_userid     ),
      AtomicField(name = "geo_country",        _.geo_country        ),
      AtomicField(name = "geo_region",         _.geo_region         ),
      AtomicField(name = "geo_city",           _.geo_city           ),
      AtomicField(name = "geo_zipcode",        _.geo_zipcode        ),
      AtomicField(name = "geo_region_name",    _.geo_region_name    ),
      AtomicField(name = "ip_isp",             _.ip_isp             ),
      AtomicField(name = "ip_organization",    _.ip_organization    ),
      AtomicField(name = "ip_domain",          _.ip_domain          ),
      AtomicField(name = "ip_netspeed",        _.ip_netspeed        ),
      AtomicField(name = "page_url",           _.page_url           ),
      AtomicField(name = "page_title",         _.page_title         ),
      AtomicField(name = "page_referrer",      _.page_referrer      ),
      AtomicField(name = "page_urlscheme",     _.page_urlscheme     ),
      AtomicField(name = "page_urlhost",       _.page_urlhost       ),
      AtomicField(name = "page_urlpath",       _.page_urlpath       ),
      AtomicField(name = "page_urlquery",      _.page_urlquery      ),
      AtomicField(name = "page_urlfragment",   _.page_urlfragment   ),
      AtomicField(name = "refr_urlscheme",     _.refr_urlscheme     ),
      AtomicField(name = "refr_urlhost",       _.refr_urlhost       ),
      AtomicField(name = "refr_urlpath",       _.refr_urlpath       ),
      AtomicField(name = "refr_urlquery",      _.refr_urlquery      ),
      AtomicField(name = "refr_urlfragment",   _.refr_urlfragment   ),
      AtomicField(name = "refr_medium",        _.refr_medium        ),
      AtomicField(name = "refr_source",        _.refr_source        ),
      AtomicField(name = "refr_term",          _.refr_term          ),
      AtomicField(name = "mkt_medium",         _.mkt_medium         ),
      AtomicField(name = "mkt_source",         _.mkt_source         ),
      AtomicField(name = "mkt_term",           _.mkt_term           ),
      AtomicField(name = "mkt_content",        _.mkt_content        ),
      AtomicField(name = "mkt_campaign",       _.mkt_campaign       ),
      AtomicField(name = "se_category",        _.se_category        ),
      AtomicField(name = "se_action",          _.se_action          ),
      AtomicField(name = "se_label",           _.se_label           ),
      AtomicField(name = "se_property",        _.se_property        ),
      AtomicField(name = "tr_orderid",         _.tr_orderid         ),
      AtomicField(name = "tr_affiliation",     _.tr_affiliation     ),
      AtomicField(name = "tr_city",            _.tr_city            ),
      AtomicField(name = "tr_state",           _.tr_state           ),
      AtomicField(name = "tr_country",         _.tr_country         ),
      AtomicField(name = "ti_orderid",         _.ti_orderid         ),
      AtomicField(name = "ti_sku",             _.ti_sku             ),
      AtomicField(name = "ti_name",            _.ti_name            ),
      AtomicField(name = "ti_category",        _.ti_category        ),
      AtomicField(name = "useragent",          _.useragent          ),
      AtomicField(name = "br_name",            _.br_name            ),
      AtomicField(name = "br_family",          _.br_family          ),
      AtomicField(name = "br_version",         _.br_version         ),
      AtomicField(name = "br_type",            _.br_type            ),
      AtomicField(name = "br_renderengine",    _.br_renderengine    ),
      AtomicField(name = "br_lang",            _.br_lang            ),
      AtomicField(name = "br_colordepth",      _.br_colordepth      ),
      AtomicField(name = "os_name",            _.os_name            ),
      AtomicField(name = "os_family",          _.os_family          ),
      AtomicField(name = "os_manufacturer",    _.os_manufacturer    ),
      AtomicField(name = "os_timezone",        _.os_timezone        ),
      AtomicField(name = "dvce_type",          _.dvce_type          ),
      AtomicField(name = "doc_charset",        _.doc_charset        ),
      AtomicField(name = "tr_currency",        _.tr_currency        ),
      AtomicField(name = "ti_currency",        _.ti_currency        ),
      AtomicField(name = "base_currency",      _.base_currency      ),
      AtomicField(name = "geo_timezone",       _.geo_timezone       ),
      AtomicField(name = "mkt_clickid",        _.mkt_clickid        ),
      AtomicField(name = "mkt_network",        _.mkt_network        ),
      AtomicField(name = "etl_tags",           _.etl_tags           ),
      AtomicField(name = "refr_domain_userid", _.refr_domain_userid ),
      AtomicField(name = "domain_sessionid",   _.domain_sessionid   ),
      AtomicField(name = "event_vendor",       _.event_vendor       ),
      AtomicField(name = "event_name",         _.event_name         ),
      AtomicField(name = "event_format",       _.event_format       ),
      AtomicField(name = "event_version",      _.event_version      ),
      AtomicField(name = "event_fingerprint",  _.event_fingerprint  ),
    )
  // format: on

  def from(valueLimits: Map[String, Int]): AtomicFields = {
    val withLimits = supportedFields.map { field =>
      val limit = valueLimits.getOrElse(field.name, Int.MaxValue)
      LimitedAtomicField(field, limit)
    }

    AtomicFields(withLimits)
  }

  def atomicErrorsToSchemaViolation(errors: NonEmptyList[AtomicError]): FailureDetails.SchemaViolation = {
    val messages = errors.map { error =>
      ValidatorReport(error.message, Some(error.field), Nil, error.value)
    }
    val validatorError = ValidatorError.InvalidData(messages)
    val clientError = ValidationError(validatorError, None)

    FailureDetails.SchemaViolation.IgluError(
      AtomicFields.atomicSchema,
      clientError
    )
  }
}

case class AtomicError(
  field: String,
  value: Option[String],
  message: String
)
