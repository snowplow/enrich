/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
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

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck.Prop.forAll

import org.joda.time.DateTime
import org.apache.http.NameValuePair

import _root_.io.circe.{Encoder, Json} 
import _root_.io.circe.syntax._
import _root_.io.circe.generic.semiauto._

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.fs2.io.Source

class SourceSpec extends Specification with ScalaCheck {
  implicit val nvpEncoder: Encoder[NameValuePair] = new Encoder[NameValuePair] {
    final def apply(nvp: NameValuePair): Json = Json.obj(
      (nvp.getName, Json.fromString(nvp.getValue))
    )
  }
  implicit val dateTimeEncoder: Encoder[DateTime] =
    Encoder[String].contramap(_.toString)
  implicit val apiEncoder: Encoder[CollectorPayload.Api] = deriveEncoder[CollectorPayload.Api]
  implicit val sourceEncoder: Encoder[CollectorPayload.Source] = deriveEncoder[CollectorPayload.Source]
  implicit val contextEncoder: Encoder[CollectorPayload.Context] = deriveEncoder[CollectorPayload.Context]
  implicit val collectorPayloadEncoder: Encoder[CollectorPayload] =
    deriveEncoder[CollectorPayload]

  "parseAsCollectorPayload" should {
    "parse a JSON string into a CollectorPayload" in {
      forAll(PayloadGen.getPageView) { cp =>
        Source.decodeCollectorPayload(cp.asJson.noSpaces) mustEqual cp
      }
    }
  }
}