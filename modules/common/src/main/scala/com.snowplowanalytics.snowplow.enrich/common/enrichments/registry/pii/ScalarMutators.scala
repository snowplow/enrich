/*
 * Copyright (c) 2017-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.pii

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

object ScalarMutators {

  private def applyScalarStrategy(
    fieldName: String,
    fieldValue: String,
    strategy: PiiStrategy
  ): (String, ModifiedFields) =
    if (fieldValue != null) {
      val modifiedValue = strategy.scramble(fieldValue)
      (modifiedValue, List(ScalarModifiedField(fieldName, fieldValue, modifiedValue)))
    } else (null, List())

  /**
   * Maps from a configuration field name to an EnrichedEvent mutator.
   */
  val byFieldName: Map[String, MutatorFn] = Map(
    "user_id" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("user_id", event.user_id, strategy)
      event.user_id = newValue
      modifiedFields
    },
    "user_ipaddress" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("user_ipaddress", event.user_ipaddress, strategy)
      event.user_ipaddress = newValue
      modifiedFields
    },
    "user_fingerprint" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("user_fingerprint", event.user_fingerprint, strategy)
      event.user_fingerprint = newValue
      modifiedFields
    },
    "domain_userid" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("domain_userid", event.domain_userid, strategy)
      event.domain_userid = newValue
      modifiedFields
    },
    "network_userid" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("network_userid", event.network_userid, strategy)
      event.network_userid = newValue
      modifiedFields
    },
    "ip_organization" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("ip_organization", event.ip_organization, strategy)
      event.ip_organization = newValue
      modifiedFields
    },
    "ip_domain" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("ip_domain", event.ip_domain, strategy)
      event.ip_domain = newValue
      modifiedFields
    },
    "tr_orderid" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("tr_orderid", event.tr_orderid, strategy)
      event.tr_orderid = newValue
      modifiedFields
    },
    "ti_orderid" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("ti_orderid", event.ti_orderid, strategy)
      event.ti_orderid = newValue
      modifiedFields
    },
    "mkt_term" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("mkt_term", event.mkt_term, strategy)
      event.mkt_term = newValue
      modifiedFields
    },
    "mkt_content" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("mkt_content", event.mkt_content, strategy)
      event.mkt_content = newValue
      modifiedFields
    },
    "se_category" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("se_category", event.se_category, strategy)
      event.se_category = newValue
      modifiedFields
    },
    "se_action" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("se_action", event.se_action, strategy)
      event.se_action = newValue
      modifiedFields
    },
    "se_label" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("se_label", event.se_label, strategy)
      event.se_label = newValue
      modifiedFields
    },
    "se_property" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("se_property", event.se_property, strategy)
      event.se_property = newValue
      modifiedFields
    },
    "mkt_clickid" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("mkt_clickid", event.mkt_clickid, strategy)
      event.mkt_clickid = newValue
      modifiedFields
    },
    "refr_domain_userid" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("refr_domain_userid", event.refr_domain_userid, strategy)
      event.refr_domain_userid = newValue
      modifiedFields
    },
    "domain_sessionid" -> { (event: EnrichedEvent, strategy: PiiStrategy) =>
      val (newValue, modifiedFields) = applyScalarStrategy("domain_sessionid", event.domain_sessionid, strategy)
      event.domain_sessionid = newValue
      modifiedFields
    }
  )
}
