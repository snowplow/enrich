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
package com.snowplowanalytics.snowplow.enrich.common

/**
 * Base type for errors that must crash enrich rather than turn the event into a bad row.
 *
 * Any other exception raised while enriching an event or a collector payload is caught, and that
 * event or payload becomes a `generic_error` bad row. Extend this type for errors where crashing is deliberate, e.g. an
 * infrastructure outage that would otherwise turn every good event into a bad row.
 */
abstract class FatalEnrichError(message: String) extends RuntimeException(message)
