/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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

import scala.util.control.NonFatal
import scala.concurrent.ExecutionContext

import cats.implicits._
import cats.effect.{Blocker, IO}

import scalaj.http._

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.lrumap.CreateLruMap._

import io.circe.Json
import io.circe.literal._

import org.apache.http.NameValuePair
import org.apache.http.message.BasicNameValuePair

import com.snowplowanalytics.snowplow.enrich.common.utils.{HttpClient, JsonUtils}

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
  val client: IgluCirceClient[IO] = IgluCirceClient
    .parseDefault[IO](igluConfig)
    .value
    .unsafeRunSync()
    .getOrElse(throw new RuntimeException("invalid resolver configuration"))

  val blockingEC = Blocker.liftExecutionContext(ExecutionContext.global)

//  implicit def httpClient(implicit C: ContextShift[IO]): Http4sClient[IO] = JavaNetClientBuilder[IO](blockingEC).create

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

  implicit val ioHttpClient: HttpClient[IO] =
    new HttpClient[IO] {
      override def getResponse(
        uri: String,
        authUser: Option[String],
        authPassword: Option[String],
        body: Option[String],
        method: String,
        connectionTimeout: Option[Long],
        readTimeout: Option[Long]
      ): IO[Either[Throwable, String]] =
        getBody(
          buildRequest(
            uri,
            authUser,
            authPassword,
            body,
            method,
            connectionTimeout,
            readTimeout
          )
        )
    }

  // The defaults are from scalaj library
  val DEFAULT_CONNECTION_TIMEOUT_MS = 1000
  val DEFAULT_READ_TIMEOUT_MS = 5000

  /**
   * Blocking method to get body of HTTP response
   *
   * @param request assembled request object
   * @return validated body of HTTP request
   */
  private def getBody(request: HttpRequest): IO[Either[Throwable, String]] =
    IO.delay(request.asString)
      .map { res =>
        if (res.isSuccess) res.body.asRight
        else new Exception(s"Request failed with status ${res.code} and body ${res.body}").asLeft
      }
      .recover {
        case NonFatal(e) => new Exception(e).asLeft
      }

  /**
   * Build HTTP request object
   *
   * @param uri               full URI to request
   * @param authUser          optional username for basic auth
   * @param authPassword      optional password for basic auth
   * @param body              optional request body
   * @param method            HTTP method
   * @param connectionTimeout connection timeout, if not set default is 1000ms
   * @param readTimeout       read timeout, if not set default is 5000ms
   * @return HTTP request
   */
  def buildRequest(
    uri: String,
    authUser: Option[String],
    authPassword: Option[String],
    body: Option[String],
    method: String = "GET",
    connectionTimeout: Option[Long],
    readTimeout: Option[Long]
  ): HttpRequest = {
    val req: HttpRequest = Http(uri).method(method).maybeTimeout(connectionTimeout, readTimeout)
    req.maybeAuth(authUser, authPassword).maybePostData(body)
  }

  implicit class RichHttpRequest(request: HttpRequest) {

    def maybeAuth(user: Option[String], password: Option[String]): HttpRequest =
      if (user.isDefined || password.isDefined)
        request.auth(user.getOrElse(""), password.getOrElse(""))
      else request

    def maybeTimeout(connectionTimeout: Option[Long], readTimeout: Option[Long]): HttpRequest =
      (connectionTimeout, readTimeout) match {
        case (Some(ct), Some(rt)) => request.timeout(ct.toInt, rt.toInt)
        case (Some(ct), None) => request.timeout(ct.toInt, DEFAULT_READ_TIMEOUT_MS)
        case (None, Some(rt)) => request.timeout(DEFAULT_CONNECTION_TIMEOUT_MS, rt.toInt)
        case _ => request.timeout(DEFAULT_CONNECTION_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS)
      }

    def maybePostData(body: Option[String]): HttpRequest =
      body
        .map(data => request.postData(data).header("content-type", "application/json").header("accept", "*/*"))
        .getOrElse(request)
  }
}
