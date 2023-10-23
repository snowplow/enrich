/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry

import java.time.Instant
import cats.implicits._
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.effect.testing.specs2.CatsIO
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.{Payload => BadrowPayload}

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class CloudfrontAccessLogAdapterSpec extends Specification with DataTables with ValidatedMatchers with CatsIO {
  val processor = Processor("CloudfrontAccessLogAdapterSpec", "v1")

  def is = s2"""
  toRawEvents should return a NEL containing one RawEvent if the line contains 12 fields   $e1
  toRawEvents should return a NEL containing one RawEvent if the line contains 15 fields   $e2
  toRawEvents should return a NEL containing one RawEvent if the line contains 18 fields   $e3
  toRawEvents should return a NEL containing one RawEvent if the line contains 19 fields   $e4
  toRawEvents should return a NEL containing one RawEvent if the line contains 23 fields   $e5
  toRawEvents should return a NEL containing one RawEvent if the line contains 24 fields   $e6
  toRawEvents should return a NEL containing one RawEvent if the line contains 26 fields   $e7
  toRawEvents should return a Validation Failure if the line is the wrong length           $e8
  toRawEvents should return a Validation Failure if the line contains an unparseable field $e9
  """

  val loader = CloudfrontAccessLogAdapterSpec.TsvLoader("com.amazon.aws.cloudfront/wd_access_log")

  val doubleEncodedUa =
    "Mozilla/5.0%2520(Macintosh;%2520Intel%2520Mac%2520OS%2520X%252010_9_2)%2520AppleWebKit/537.36%2520(KHTML,%2520like%2520Gecko)%2520Chrome/34.0.1847.131%2520Safari/537.36"
  val singleEncodedUa =
    "Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_9_2)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/34.0.1847.131%20Safari/537.36"
  val unEncodedUa =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36"

  val doubleEncodedQs = "a=b%2520c"
  val singleEncodedQs = "a=b%20c"

  val url = "http://snowplowanalytics.com/analytics/index.html"
  val adapterWithDefaultSchemas = CloudfrontAccessLogAdapter(schemas = cloudfrontAccessLogSchemas)

  object Shared {
    val api = CollectorPayload.Api("com.amazon.aws.cloudfront", "wd_access_log")
    val source = CollectorPayload.Source("tsv", "UTF-8", None)
    val context =
      CollectorPayload.Context(
        DateTime.parse("2013-10-07T23:35:30.000Z").some,
        "255.255.255.255".some,
        singleEncodedUa.some,
        None,
        Nil,
        None
      )
  }

  object Expected {
    val staticNoPlatform = Map(
      "tv" -> "com.amazon.aws.cloudfront/wd_access_log",
      "e" -> "ue",
      "url" -> url
    ).toOpt
    val static = staticNoPlatform ++ Map(
      "p" -> "srv"
    ).toOpt
  }

  def e1 = {
    val input =
      s"2013-10-07\t23:35:30\tc\t100\t255.255.255.255\tf\tg\th\ti\t$url\t$doubleEncodedUa\t$doubleEncodedQs"

    val expectedJson =
      s"""|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-0",
              |"data":{
                |"dateTime":"2013-10-07T23:35:30Z",
                |"xEdgeLocation":"c",
                |"scBytes":100,
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"$url",
                |"csUserAgent":"$unEncodedUa",
                |"csUriQuery":"$singleEncodedQs"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    loader
      .toCollectorPayload(input, processor)
      .traverse(_.traverse(adapterWithDefaultSchemas.toRawEvents(_, SpecHelpers.client)))
      .map(
        _ must beValid(
          Some(
            Validated.Valid(
              NonEmptyList.one(
                RawEvent(
                  Shared.api,
                  Expected.static ++ Map("ue_pr" -> expectedJson).toOpt,
                  None,
                  Shared.source,
                  Shared.context
                )
              )
            )
          )
        )
      )
  }

  def e2 = {
    val input =
      s"2013-10-07\t23:35:30\tc\t100\t255.255.255.255\tf\tg\th\ti\t$url\t$doubleEncodedUa\t$doubleEncodedQs\tm\tn\to"

    val expectedJson =
      s"""|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-1",
              |"data":{
                |"dateTime":"2013-10-07T23:35:30Z",
                |"xEdgeLocation":"c",
                |"scBytes":100,
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"$url",
                |"csUserAgent":"$unEncodedUa",
                |"csUriQuery":"$singleEncodedQs",
                |"csCookie":"m",
                |"xEdgeResultType":"n",
                |"xEdgeRequestId":"o"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    loader
      .toCollectorPayload(input, processor)
      .traverse(_.traverse(adapterWithDefaultSchemas.toRawEvents(_, SpecHelpers.client)))
      .map(
        _ must beValid(
          Some(
            Validated.Valid(
              NonEmptyList.one(
                RawEvent(
                  Shared.api,
                  Expected.static ++ Map("ue_pr" -> expectedJson).toOpt,
                  None,
                  Shared.source,
                  Shared.context
                )
              )
            )
          )
        )
      )
  }

  def e3 = {
    val input =
      s"2013-10-07\t23:35:30\tc\t100\t255.255.255.255\tf\tg\th\ti\t$url\t$doubleEncodedUa\t$doubleEncodedQs\tm\tn\to\tp\tq\t90"

    val expectedJson =
      s"""|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-2",
              |"data":{
                |"dateTime":"2013-10-07T23:35:30Z",
                |"xEdgeLocation":"c",
                |"scBytes":100,
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"$url",
                |"csUserAgent":"$unEncodedUa",
                |"csUriQuery":"$singleEncodedQs",
                |"csCookie":"m",
                |"xEdgeResultType":"n",
                |"xEdgeRequestId":"o",
                |"xHostHeader":"p",
                |"csProtocol":"q",
                |"csBytes":90
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    loader
      .toCollectorPayload(input, processor)
      .traverse(_.traverse(adapterWithDefaultSchemas.toRawEvents(_, SpecHelpers.client)))
      .map(
        _ must beValid(
          Some(
            Validated.Valid(
              NonEmptyList.one(
                RawEvent(
                  Shared.api,
                  Expected.static ++ Map("ue_pr" -> expectedJson).toOpt,
                  None,
                  Shared.source,
                  Shared.context
                )
              )
            )
          )
        )
      )
  }

  def e4 = {
    val input =
      s"2013-10-07\t23:35:30\tc\t100\t255.255.255.255\tf\tg\th\ti\t$url\t$doubleEncodedUa\t$doubleEncodedQs\tm\tn\to\tp\tq\t90\t0.001"

    val expectedJson =
      s"""|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-3",
              |"data":{
                |"dateTime":"2013-10-07T23:35:30Z",
                |"xEdgeLocation":"c",
                |"scBytes":100,
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"$url",
                |"csUserAgent":"$unEncodedUa",
                |"csUriQuery":"$singleEncodedQs",
                |"csCookie":"m",
                |"xEdgeResultType":"n",
                |"xEdgeRequestId":"o",
                |"xHostHeader":"p",
                |"csProtocol":"q",
                |"csBytes":90,
                |"timeTaken":0.001
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    loader
      .toCollectorPayload(input, processor)
      .traverse(_.traverse(adapterWithDefaultSchemas.toRawEvents(_, SpecHelpers.client)))
      .map(
        _ must beValid(
          Some(
            Validated.Valid(
              NonEmptyList.one(
                RawEvent(
                  Shared.api,
                  Expected.static ++ Map("ue_pr" -> expectedJson).toOpt,
                  None,
                  Shared.source,
                  Shared.context
                )
              )
            )
          )
        )
      )
  }

  def e5 = {
    val input =
      s"2013-10-07\t23:35:30\tc\t100\t255.255.255.255\tf\tg\th\ti\t$url\t$doubleEncodedUa\t$doubleEncodedQs\tm\tn\to\tp\tq\t90\t0.001\tr\ts\tt\tu"

    val expectedJson =
      s"""|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-4",
              |"data":{
                |"dateTime":"2013-10-07T23:35:30Z",
                |"xEdgeLocation":"c",
                |"scBytes":100,
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"$url",
                |"csUserAgent":"$unEncodedUa",
                |"csUriQuery":"$singleEncodedQs",
                |"csCookie":"m",
                |"xEdgeResultType":"n",
                |"xEdgeRequestId":"o",
                |"xHostHeader":"p",
                |"csProtocol":"q",
                |"csBytes":90,
                |"timeTaken":0.001,
                |"xForwardedFor":"r",
                |"sslProtocol":"s",
                |"sslCipher":"t",
                |"xEdgeResponseResultType":"u"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    loader
      .toCollectorPayload(input, processor)
      .traverse(_.traverse(adapterWithDefaultSchemas.toRawEvents(_, SpecHelpers.client)))
      .map(
        _ must beValid(
          Some(
            Validated.Valid(
              NonEmptyList.one(
                RawEvent(
                  Shared.api,
                  Expected.static ++ Map("ue_pr" -> expectedJson).toOpt,
                  None,
                  Shared.source,
                  Shared.context
                )
              )
            )
          )
        )
      )
  }

  def e6 = {
    val input =
      s"2013-10-07\t23:35:30\tc\t100\t255.255.255.255\tf\tg\th\ti\t$url\t$doubleEncodedUa\t$doubleEncodedQs\tm\tn\to\tp\tq\t90\t0.001\tr\ts\tt\tu\tHTTP/2.0"

    val expectedJson =
      s"""|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-5",
              |"data":{
                |"dateTime":"2013-10-07T23:35:30Z",
                |"xEdgeLocation":"c",
                |"scBytes":100,
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"$url",
                |"csUserAgent":"$unEncodedUa",
                |"csUriQuery":"$singleEncodedQs",
                |"csCookie":"m",
                |"xEdgeResultType":"n",
                |"xEdgeRequestId":"o",
                |"xHostHeader":"p",
                |"csProtocol":"q",
                |"csBytes":90,
                |"timeTaken":0.001,
                |"xForwardedFor":"r",
                |"sslProtocol":"s",
                |"sslCipher":"t",
                |"xEdgeResponseResultType":"u",
                |"csProtocolVersion":"HTTP/2.0"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    loader
      .toCollectorPayload(input, processor)
      .traverse(_.traverse(adapterWithDefaultSchemas.toRawEvents(_, SpecHelpers.client)))
      .map(
        _ must beValid(
          Some(
            Validated.Valid(
              NonEmptyList.one(
                RawEvent(
                  Shared.api,
                  Expected.static ++ Map("ue_pr" -> expectedJson).toOpt,
                  None,
                  Shared.source,
                  Shared.context
                )
              )
            )
          )
        )
      )
  }

  def e7 = {
    val input =
      s"2013-10-07\t23:35:30\tc\t100\t255.255.255.255\tf\tg\th\ti\t$url\t$doubleEncodedUa\t$doubleEncodedQs\tm\tn\to\tp\tq\t90\t0.001\tr\ts\tt\tu\tHTTP/2.0\tProcessed\t12"

    val expectedJson =
      s"""|{
            |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            |"data":{
              |"schema":"iglu:com.amazon.aws.cloudfront/wd_access_log/jsonschema/1-0-6",
              |"data":{
                |"dateTime":"2013-10-07T23:35:30Z",
                |"xEdgeLocation":"c",
                |"scBytes":100,
                |"cIp":"255.255.255.255",
                |"csMethod":"f",
                |"csHost":"g",
                |"csUriStem":"h",
                |"scStatus":"i",
                |"csReferer":"$url",
                |"csUserAgent":"$unEncodedUa",
                |"csUriQuery":"$singleEncodedQs",
                |"csCookie":"m",
                |"xEdgeResultType":"n",
                |"xEdgeRequestId":"o",
                |"xHostHeader":"p",
                |"csProtocol":"q",
                |"csBytes":90,
                |"timeTaken":0.001,
                |"xForwardedFor":"r",
                |"sslProtocol":"s",
                |"sslCipher":"t",
                |"xEdgeResponseResultType":"u",
                |"csProtocolVersion":"HTTP/2.0",
                |"fleStatus":"Processed",
                |"fleEncryptedFields":"12"
              |}
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    loader
      .toCollectorPayload(input, processor)
      .traverse(_.traverse(adapterWithDefaultSchemas.toRawEvents(_, SpecHelpers.client)))
      .map(
        _ must beValid(
          Some(
            Validated.Valid(
              NonEmptyList.one(
                RawEvent(
                  Shared.api,
                  Expected.static ++ Map("ue_pr" -> expectedJson).toOpt,
                  None,
                  Shared.source,
                  Shared.context
                )
              )
            )
          )
        )
      )
  }

  def e8 = {
    val params = SpecHelpers.toNameValuePairs()
    val payload =
      CollectorPayload(
        Shared.api,
        params,
        None,
        "2013-10-07\t23:35:30\tc\t\t".some,
        Shared.source,
        Shared.context
      )

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client)
      .map(
        _ must beInvalid(
          NonEmptyList
            .one(
              FailureDetails.AdapterFailure.InputData(
                "body",
                "2013-10-07	23:35:30	c		".some,
                "access log contained 5 fields, expected 12, 15, 18, 19, 23, 24 or 26"
              )
            )
        )
      )
  }

  def e9 = {
    val params = SpecHelpers.toNameValuePairs()
    val payload =
      CollectorPayload(
        Shared.api,
        params,
        None,
        s"a\tb\tc\td\te\tf\tg\th\ti\t$url\tk\t$doubleEncodedQs".some,
        Shared.source,
        Shared.context
      )

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client)
      .map(
        _ must beInvalid(
          NonEmptyList.of(
            FailureDetails.AdapterFailure.InputData(
              "dateTime",
              "a b".some,
              """could not convert access log timestamp: Invalid format: "aTb+00:00""""
            ),
            FailureDetails.AdapterFailure
              .InputData("scBytes", "d".some, "cannot be converted to Int")
          )
        )
      )
  }
}

object CloudfrontAccessLogAdapterSpec {

  final case class TsvLoader(adapter: String) {
    private val CollectorName = "tsv"
    private val CollectorEncoding = "UTF-8"

    /**
     * Converts the source TSV into a ValidatedMaybeCollectorPayload.
     *
     * @param line A TSV
     * @return either a set of validation errors or an Option-boxed CanonicalInput object, wrapped in
     *         a ValidatedNel.
     */
    def toCollectorPayload(line: String, processor: Processor): ValidatedNel[BadRow.CPFormatViolation, Option[CollectorPayload]] =
      // Throw away the first two lines of Cloudfront web distribution access logs
      if (line.startsWith("#Version:") || line.startsWith("#Fields:"))
        None.valid
      else
        CollectorPayload
          .parseApi(adapter)
          .map { api =>
            val source = CollectorPayload.Source(CollectorName, CollectorEncoding, None)
            val context = CollectorPayload.Context(None, None, None, None, Nil, None)
            CollectorPayload(api, Nil, None, Some(line), source, context).some
          }
          .leftMap(f =>
            BadRow.CPFormatViolation(
              processor,
              Failure.CPFormatViolation(Instant.now(), CollectorName, f),
              BadrowPayload.RawPayload(line)
            )
          )
          .toValidatedNel
  }
}
