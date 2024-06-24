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
