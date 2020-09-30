/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common

import cats.Id
import cats.implicits._

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.lrumap.CreateLruMap._

import io.circe.Json
import io.circe.literal._

import org.apache.http.NameValuePair
import org.apache.http.message.BasicNameValuePair

import com.snowplowanalytics.snowplow.enrich.common.utils.JsonUtils

object SpecHelpers {

  // Standard Iglu configuration
  private val igluConfig = json"""{
    "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
    "data": {
      "cacheSize": 500,
      "repositories": [
        {
          "name": "Iglu Central",
          "priority": 0,
          "vendorPrefixes": [ "com.snowplowanalytics" ],
          "connection": {
            "http": {
              "uri": "http://iglucentral.com"
            }
          }
        },
        {
          "name": "Embedded src/test/resources",
          "priority": 100,
          "vendorPrefixes": [ "com.snowplowanalytics" ],
          "connection": {
            "embedded": {
              "path": "/iglu-schemas"
            }
          }
        }
      ]
    }
  }"""

  /** Builds an Iglu client from the above Iglu configuration. */
  val client: Client[Id, Json] = Client
    .parseDefault[Id](igluConfig)
    .value
    .getOrElse(throw new RuntimeException("invalid resolver configuration"))

  private type NvPair = (String, String)

  /**
   * Converts an NvPair into a
   * BasicNameValuePair
   *
   * @param pair The Tuple2[String, String] name-value
   * pair to convert
   * @return the basic name value pair
   */
  private def toNvPair(pair: NvPair): BasicNameValuePair =
    new BasicNameValuePair(pair._1, pair._2)

  /** Converts the supplied NvPairs into a NameValueNel */
  def toNameValuePairs(pairs: NvPair*): List[NameValuePair] =
    List(pairs.map(toNvPair): _*)

  /**
   * Builds a self-describing JSON by
   * wrapping the supplied JSON with
   * schema and data properties
   *
   * @param json The JSON to use as the request body
   * @param schema The name of the schema to insert
   * @return a self-describing JSON
   */
  def toSelfDescJson(json: String, schema: String): String =
    s"""{"schema":"iglu:com.snowplowanalytics.snowplow/${schema}/jsonschema/1-0-0","data":${json}}"""

  /** Parse a string containing a SDJ as [[SelfDescribingData]] */
  def jsonStringToSDJ(rawJson: String): Either[String, SelfDescribingData[Json]] =
    JsonUtils
      .extractJson(rawJson)
      .leftMap(err => s"Can't parse [$rawJson] as Json, error: [$err]")
      .flatMap(SelfDescribingData.parse[Json])
      .leftMap(err => s"Can't parse Json [$rawJson] as as SelfDescribingData, error: [$err]")

  implicit class MapOps[A, B](underlying: Map[A, B]) {
    def toOpt: Map[A, Option[B]] = underlying.map { case (a, b) => (a, Option(b)) }
  }
}
