/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.beam
package adapters

import java.nio.file.Paths

import cats.syntax.option._
import com.spotify.scio.pubsub.PubsubIO
import com.spotify.scio.testing._
import io.circe.literal._
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import scala.jdk.CollectionConverters._

object GoogleAnalyticsAdapterSpec {
  val body = "t=pageview&dh=host&dp=path"
  val raw = Seq(
    new PubsubMessage(SpecHelpers.buildCollectorPayload(
                        path = "/com.google.analytics/v1",
                        body = body.some
                      ),
                      Map.empty[String, String].asJava
    )
  )
  val expected = Map(
    "v_tracker" -> "com.google.analytics.measurement-protocol-v1",
    "event_vendor" -> "com.google.analytics.measurement-protocol",
    "event_name" -> "page_view",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "unstruct",
    "unstruct_event" -> json"""{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0","data":{"documentHostName":"host","documentPath":"path"}}}""".noSpaces
  )
}

class GoogleAnalyticsAdapterSpec extends PipelineSpec {
  import GoogleAnalyticsAdapterSpec._
  "GoogleAnalyticsAdapter" should "enrich using the google analytics adapter" in {
    JobTest[Enrich.type]
      .args(
        "--job-name=j",
        "--raw=in",
        "--enriched=out",
        "--bad=bad",
        "--resolver=" + Paths.get(getClass.getResource("/iglu_resolver.json").toURI())
      )
      .input(PubsubIO.pubsub[PubsubMessage]("in"), raw)
      .distCache(DistCacheIO(""), List.empty[Either[String, String]])
      .output(PubsubIO.string("bad")) { b =>
        b should beEmpty; ()
      }
      .output(PubsubIO.string("out")) { o =>
        o should satisfySingleValue { c: String =>
          SpecHelpers.compareEnrichedEvent(expected, c)
        }; ()
      }
      .run()
  }
}
