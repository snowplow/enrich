/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common

/* For transition, until EnrichmentManager.validateEnriched never accepts invalid any more
 * See https://github.com/snowplow/enrich/issues/517#issuecomment-1033910690
 */
object AcceptInvalid {
  val featureFlags = EtlPipeline.FeatureFlags(acceptInvalid = false, legacyEnrichmentOrder = false)
}
