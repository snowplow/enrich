/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich

import common.enrichments.registry.Enrichment

/** Scala package object to hold types, helper methods etc. */
package object common {

  /** Type alias for HTTP headers */
  type HttpHeaders = List[String]

  /** Type alias for a map whose keys are enrichment names and whose values are enrichments */
  type EnrichmentMap = Map[String, Enrichment]

  /** Parameters inside of a raw event */
  type RawEventParameters = Map[String, Option[String]]

  /** Parameters extracted from query string */
  type QueryStringParameters = List[(String, Option[String])]

  /**
   * Type alias for either Throwable or successful value
   * It has Monad instance unlike Validation
   */
  type EitherThrowable[+A] = Either[Throwable, A]
}
