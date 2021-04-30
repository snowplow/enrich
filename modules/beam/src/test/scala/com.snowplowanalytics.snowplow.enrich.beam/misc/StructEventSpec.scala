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
package misc

import java.nio.file.Paths

import cats.syntax.option._
import com.spotify.scio.pubsub.PubsubIO
import com.spotify.scio.testing._
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import scala.jdk.CollectionConverters._

object StructEventSpec {
  val querystring =
    "e=se&se_ca=ecomm&se_ac=add-to-basket&se_la=%CE%A7%CE%B1%CF%81%CE%B9%CF%84%CE%AF%CE%BD%CE%B7&se_pr=1&se_va=35708.23"
  val raw = Seq(
    new PubsubMessage(SpecHelpers.buildCollectorPayload(
                        path = "/ice.png",
                        querystring = querystring.some
                      ),
                      Map.empty[String, String].asJava
    )
  )
  val expected = Map(
    "event_vendor" -> "com.google.analytics",
    "event_name" -> "event",
    "event_format" -> "jsonschema",
    "event_version" -> "1-0-0",
    "event" -> "struct",
    "se_action" -> "add-to-basket",
    "se_category" -> "ecomm",
    "se_label" -> "Χαριτίνη",
    "se_property" -> "1",
    "se_value" -> "35708.23"
  )
}

class StructEventSpec extends PipelineSpec {
  import StructEventSpec._
  "Enrich" should "enrich and produce a struct event" in {
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
