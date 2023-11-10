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

import com.google.api.gax.rpc.FixedHeaderProvider

import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.GcpUserAgent

object Utils {

  def createPubsubUserAgentHeader(gcpUserAgent: GcpUserAgent): FixedHeaderProvider =
    FixedHeaderProvider.create("user-agent", s"${gcpUserAgent.productName}/enrich (GPN:Snowplow;)")

}
