/*
 * Copyright (c) 2019-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.pubsub

import java.util.regex.Pattern
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.GcpUserAgent
import org.specs2.mutable.Specification

class GcpUserAgentSpec extends Specification {

  "createUserAgent" should {
    "create user agent string correctly" in {
      val gcpUserAgent = GcpUserAgent(productName = "Snowplow OSS")
      val resultUserAgent = Utils.createPubsubUserAgentHeader(gcpUserAgent).getHeaders.get("user-agent")
      val expectedUserAgent = s"Snowplow OSS/enrich (GPN:Snowplow;)"

      val userAgentRegex = Pattern.compile(
        """(?iU)(?:[^\(\)\/]+\/[^\/]+\s+)*(?:[^\s][^\(\)\/]+\/[^\/]+\s?\([^\(\)]*)gpn:(.*)[;\)]"""
      )
      val matcher = userAgentRegex.matcher(resultUserAgent)
      val matched = if (matcher.find()) Some(matcher.group(1)) else None
      val expectedMatched = "Snowplow;"

      resultUserAgent must beEqualTo(expectedUserAgent)
      matched must beSome(expectedMatched)
    }
  }

}
