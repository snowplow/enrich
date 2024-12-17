/*
 * Copyright (c) 2019-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.pubsub

import com.google.api.gax.rpc.FixedHeaderProvider

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.GcpUserAgent

object Utils {

  def createPubsubUserAgentHeader(gcpUserAgent: GcpUserAgent): FixedHeaderProvider =
    FixedHeaderProvider.create("user-agent", s"${gcpUserAgent.productName}/enrich (GPN:Snowplow;)")

}
