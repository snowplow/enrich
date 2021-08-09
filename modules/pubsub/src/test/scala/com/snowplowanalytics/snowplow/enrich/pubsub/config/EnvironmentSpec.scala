/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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
package config

import cats.effect.testing.specs2.CatsIO
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.pubsub.Environment
import org.specs2.mutable.Specification

class EnvironmentSpec extends Specification with CatsIO {

  "outputAttributes" should {
    "fetch attribute values shorter than 1024 characters" in {
      val output = io.Output.PubSub("projects/test-project/topics/good-topic", Some(Set("app_id", "platform")), None, None, None, None)
      val ee = new EnrichedEvent()
      ee.app_id = "test_app"
      ee.platform = "a".repeat(1025)

      val result = Environment.outputAttributes(output)(ee)

      result must haveSize(1)
      result must haveKey("app_id")
      result must haveValue("test_app")
    }
  }
}
