/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import scala.collection.immutable.IntMap

import com.snowplowanalytics.iglu.core.SelfDescribingData

import com.snowplowanalytics.lrumap.{CreateLruMap, LruMap}

import io.circe.Json

package object sqlquery {
  type SqlCacheInit[F[_]] =
    CreateLruMap[F, IntMap[Input.ExtractedValue], CachingEvaluator.CachedItem[List[SelfDescribingData[Json]]]]

  type SqlCache[F[_]] =
    LruMap[F, IntMap[Input.ExtractedValue], CachingEvaluator.CachedItem[List[SelfDescribingData[Json]]]]

  type SqlQueryEvaluator[F[_]] = CachingEvaluator[F, IntMap[Input.ExtractedValue], List[SelfDescribingData[Json]]]
}
