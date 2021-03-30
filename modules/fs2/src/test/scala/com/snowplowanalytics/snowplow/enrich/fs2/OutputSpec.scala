/*
 * Copyright (c) 2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.fs2

import java.time.Instant

import cats.data.NonEmptyList
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

class OutputSpec extends Specification with ScalaCheck {
  "Output" should {
    "serialize a bad event to the bad output" in {
      implicit val cpGen = PayloadGen.getPageViewArbitrary
      prop { (collectorPayload: CollectorPayload) =>
        val failure = Failure.AdapterFailures(Instant.now,
                                              "vendor",
                                              "1-0-0",
                                              NonEmptyList.one(FailureDetails.AdapterFailure.NotJson("field", None, "error"))
        )
        val badRow = BadRow.AdapterFailures(Enrich.processor, failure, collectorPayload.toBadRowPayload)

        Output.serialize(Output.Bad(badRow)) must beLike {
          case Output.Bad(bytes) => bytes must not be empty
        }
      }
    }

    "serialize a good event to the good output" in {
      val ee = new EnrichedEvent()

      Output.serialize(Output.Good(ee)) must beLike {
        case Output.Good(result) =>
          result must not be empty
      }

    }

    "serialize a pii event to the pii output" in {
      val ee = new EnrichedEvent()

      Output.serialize(Output.Pii(ee)) must beLike {
        case Output.Pii(result) =>
          result must not be empty
      }

    }


    "serialize an over-sized good event to the bad output" in {
      val ee = new EnrichedEvent()
      ee.app_id = "x" * 10000000

      Output.serialize(Output.Good(ee)) must beLike {
        case Output.Bad(bytes) =>
          bytes must not be empty
          bytes must have size(be_<=(6900000))
      }

    }

    "serialize an over-sized pii event to the bad output" in {
      val ee = new EnrichedEvent()
      ee.app_id = "x" * 10000000

      Output.serialize(Output.Pii(ee)) must beLike {
        case Output.Bad(bytes) =>
          bytes must not be empty
          bytes must have size(be_<=(6900000))
      }

    }
  }
}
