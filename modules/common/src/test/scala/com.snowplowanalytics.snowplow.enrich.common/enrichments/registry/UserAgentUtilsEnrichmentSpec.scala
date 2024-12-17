/**
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import org.specs2.matcher.DataTables
import org.specs2.mutable.Specification

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

class UserAgentUtilsEnrichmentSpec extends Specification with DataTables {
  val schemaKey = SchemaKey("vendor", "name", "format", SchemaVer.Full(1, 0, 0))

  "useragent parser" should {
    "parse useragent" in {
      "SPEC NAME" || "Input UserAgent" | "Browser name" | "Browser family" | "Browser version" | "Browser type" | "Browser rendering enging" | "OS fields" | "Device type" | "Device is mobile" |>
        "Safari spec" !! "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36" ! "Chrome 33" ! "Chrome" ! Some(
          "33.0.1750.152"
        ) ! "Browser" ! "WEBKIT" ! (("Mac OS X", "Mac OS X", "Apple Inc.")) ! "Computer" ! false |
        "IE spec" !! "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0" ! "Internet Explorer 11" ! "Internet Explorer" ! Some(
          "11.0"
        ) ! "Browser" ! "TRIDENT" ! (("Windows 7", "Windows", "Microsoft Corporation")) ! "Computer" ! false | {

        (
          _,
          input,
          browserName,
          browserFamily,
          browserVersion,
          browserType,
          browserRenderEngine,
          osFields,
          deviceType,
          deviceIsMobile
        ) =>
          val expected = ClientAttributes(
            browserName,
            browserFamily,
            browserVersion,
            browserType,
            browserRenderEngine,
            osFields._1,
            osFields._2,
            osFields._3,
            deviceType,
            deviceIsMobile
          )
          UserAgentUtilsEnrichment(schemaKey).extractClientAttributes(input) must beRight(
            expected
          )
      }
    }
  }
}
