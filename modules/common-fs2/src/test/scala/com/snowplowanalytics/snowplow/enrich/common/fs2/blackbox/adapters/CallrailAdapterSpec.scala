/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.adapters

import io.circe.literal._

import org.specs2.mutable.Specification

import cats.effect.testing.specs2.CatsIO

import cats.syntax.option._

import com.snowplowanalytics.snowplow.enrich.common.fs2.blackbox.BlackBoxTesting

class CallrailAdapterSpec extends Specification with CatsIO {
  "enrichWith" should {
    "enrich with CallRailAdapter" in {
      val input = BlackBoxTesting.buildCollectorPayload(
        path = "/com.callrail/v1",
        querystring =
          "aid=bnb&answered=true&callercity=BAKERSFIELD&callercountry=US&callername=SKYPE+CALLER&callernum=%2B166&callerstate=CA&callerzip=93307&callsource=keyword&datetime=2014-10-09+16%3A23%3A45&destinationnum=20&duration=247&first_call=true&id=30&ip=86.178.183.7&landingpage=http%3A%2F%2Flndpage.com%2F&recording=http%3A%2F%2Fapp.callrail.com%2Fcalls%2F30%2Frecording%2F9f&referrer=direct&referrermedium=Direct&trackingnum=%2B12015911668".some
      )
      val expected = Map(
        "v_tracker" -> "com.callrail-v1",
        "event_vendor" -> "com.callrail",
        "event_name" -> "call_complete",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-2",
        "event" -> "unstruct",
        "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.callrail/call_complete/jsonschema/1-0-2","data":{"duration":247,"ip":"86.178.183.7","destinationnum":"20","datetime":"2014-10-09T16:23:45.000Z","landingpage":"http://lndpage.com/","callerzip":"93307","callername":"SKYPE CALLER","id":"30","callernum":"+166","trackingnum":"+12015911668","referrermedium":"Direct","referrer":"direct","callerstate":"CA","recording":"http://app.callrail.com/calls/30/recording/9f","first_call":true,"callercountry":"US","callercity":"BAKERSFIELD","answered":true,"callsource":"keyword"}}}""".noSpaces
      )
      BlackBoxTesting.runTest(input, expected)
    }
  }
}
