/*
 * Copyright (c) 2019-2023 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
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
