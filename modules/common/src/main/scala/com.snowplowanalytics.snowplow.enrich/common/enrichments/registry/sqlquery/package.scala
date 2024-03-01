/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
