/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry

import java.util.Base64

import cats.effect.testing.specs2.CatsEffect
import cats.syntax.option._

import org.joda.time.DateTime

import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers

class CloudfrontFirehoseAdapterSpec extends Specification with DataTables with ValidatedMatchers with CatsEffect {

  def is = s2"""
  toRawEvents should return a RawEvent for a valid single-record Firehose payload           $e1
  toRawEvents should return multiple RawEvents for multiple records                         $e2
  toRawEvents should map dash fields to null-like                                           $e3
  toRawEvents should correctly construct the page URL                                       $e4
  toRawEvents should fail for an empty body                                                 $e5
  toRawEvents should fail for a wrong content type                                          $e6
  toRawEvents should fail for invalid JSON                                                  $e7
  toRawEvents should fail when no valid log entries are found                               $e8
  toRawEvents should expose timestamp(ms) as the stm parameter                              $e9
  toRawEvents should fail when the records field is missing from the envelope               $e10
  toRawEvents should fail when the records field is not a JSON array                        $e11
  toRawEvents should fail when a record is missing its data field                           $e12
  toRawEvents should fail when a record's data field is not valid base64                    $e13
  toRawEvents should fail when the records list is empty                                    $e14
  toRawEvents should not emit a url parameter when x-host-header is absent                  $e15
  toRawEvents should let aid in the request querystring override the default               $e16
  toRawEvents should set context.ipAddress to None when c-ip is absent                       $e17
  """

  val adapter = CloudfrontFirehoseAdapter()

  object Shared {
    val api = CollectorPayload.Api("com.amazon.aws.cloudfront", "firehose")
    val source = CollectorPayload.Source("ssc-2.0.0-kinesis", "UTF-8", "collector.example.com".some)
    val context = CollectorPayload.Context(
      DateTime.parse("2020-01-01T00:00:00.000Z"),
      "1.2.3.4".some,
      None,
      None,
      Nil,
      None
    )
  }

  private def encode(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes("UTF-8"))

  private def firehosePayload(records: List[String]): String = {
    val recs = records.map(r => s"""{"data":"${encode(r)}"}""").mkString(",")
    s"""{"requestId":"test-request-id","timestamp":1578090901599,"records":[$recs]}"""
  }

  private def mkPayload(
    body: Option[String],
    contentType: Option[String] = Some("application/json")
  ): CollectorPayload =
    SpecHelpers.collectorPayload(
      api = Shared.api,
      querystring = Nil,
      contentType = contentType,
      body = body,
      source = Shared.source,
      context = Shared.context
    )

  private def logEntry(
    timestampMs: String = "1578090901599",
    uriStem: String = "/images/cat.jpg",
    uriQuery: String = "key=value",
    hostHeader: String = "d111111abcdef8.cloudfront.net",
    referer: String = "https://example.com/page",
    userAgent: String = "claude-code/1.0",
    cIp: String = "203.0.113.42"
  ): String =
    s"""|{
        |"timestamp(ms)": $timestampMs,
        |"cs-uri-stem": "$uriStem",
        |"cs-uri-query": "$uriQuery",
        |"x-host-header": "$hostHeader",
        |"cs(Referer)": "$referer",
        |"cs(User-Agent)": "$userAgent",
        |"c-ip": "$cIp"
        |}""".stripMargin

  // e1: Happy path
  def e1 = {
    val body = firehosePayload(List(logEntry()))
    val payload = mkPayload(body.some)

    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map { result =>
        result must beValid.like {
          case events =>
            events.size must_== 1
            events.head.parameters.get("tv") must beSome(Some("com.amazon.aws.cloudfront/firehose"))
            events.head.parameters.get("e") must beSome(Some("pv"))
            events.head.parameters.get("p") must beSome(Some("srv"))
            events.head.parameters.get("url") must beSome(
              Some("https://d111111abcdef8.cloudfront.net/images/cat.jpg?key=value")
            )
            events.head.parameters.get("refr") must beSome(Some("https://example.com/page"))
            events.head.parameters.get("aid") must beSome(Some("cloudfront"))
            events.head.context.ipAddress must beSome("203.0.113.42")
        }
      }
  }

  // e2: Multiple records
  def e2 = {
    val entry1 = logEntry(hostHeader = "host1.cloudfront.net", uriStem = "/page1")
    val entry2 = logEntry(hostHeader = "host2.cloudfront.net", uriStem = "/page2")
    val body = firehosePayload(List(entry1, entry2))
    val payload = mkPayload(body.some)

    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map { result =>
        result must beValid.like {
          case events => events.size must_== 2
        }
      }
  }

  // e3: Dash fields
  def e3 = {
    val entry = logEntry(referer = "-", uriQuery = "-")
    val body = firehosePayload(List(entry))
    val payload = mkPayload(body.some)

    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map { result =>
        result must beValid.like {
          case events =>
            events.size must_== 1
            events.head.parameters.get("refr") must beNone
            events.head.parameters.get("url") must beSome(
              Some("https://d111111abcdef8.cloudfront.net/images/cat.jpg")
            )
        }
      }
  }

  // e4: URL construction
  def e4 = {
    val entry = logEntry(
      hostHeader = "cdn.example.com",
      uriStem = "/images/cat.jpg",
      uriQuery = "size=large&color=blue"
    )
    val body = firehosePayload(List(entry))
    val payload = mkPayload(body.some)

    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map { result =>
        result must beValid.like {
          case events =>
            events.head.parameters.get("url") must beSome(
              Some("https://cdn.example.com/images/cat.jpg?size=large&color=blue")
            )
        }
      }
  }

  // e5: Empty body
  def e5 = {
    val payload = mkPayload(None)

    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map(_ must beInvalid)
  }

  // e6: Wrong content type
  def e6 = {
    val body = firehosePayload(List(logEntry()))
    val payload = mkPayload(body.some, contentType = Some("text/plain"))

    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map(_ must beInvalid)
  }

  // e7: Invalid JSON body
  def e7 = {
    val payload = mkPayload("not valid json".some)

    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map(_ must beInvalid)
  }

  // e8: No valid log entries (records with non-JSON base64 data)
  def e8 = {
    val body = firehosePayload(List("not json at all"))
    val payload = mkPayload(body.some)

    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map(_ must beInvalid)
  }

  // e9: timestamp(ms) — exposed as the `stm` parameter; context.timestamp stays as the collector's
  def e9 = {
    val entry = logEntry(timestampMs = "\"1578090901599\"")
    val body = firehosePayload(List(entry))
    val payload = mkPayload(body.some)

    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map { result =>
        result must beValid.like {
          case events =>
            events.size must_== 1
            events.head.parameters.get("stm") must beSome(Some("1578090901599"))
            events.head.context.timestamp must_== Shared.context.timestamp
        }
      }
  }

  // e10: envelope JSON has no `records` field
  def e10 = {
    val payload = mkPayload("""{"requestId":"abc"}""".some)
    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map(_ must beInvalid)
  }

  // e11: `records` is present but is not a JSON array
  def e11 = {
    val payload = mkPayload("""{"requestId":"abc","records":"not-an-array"}""".some)
    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map(_ must beInvalid)
  }

  // e12: a record is missing its `data` field
  def e12 = {
    val payload = mkPayload("""{"requestId":"abc","records":[{"foo":"bar"}]}""".some)
    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map(_ must beInvalid)
  }

  // e13: a record's `data` value contains characters outside the base64 alphabet
  def e13 = {
    val payload = mkPayload("""{"requestId":"abc","records":[{"data":"!!!!"}]}""".some)
    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map(_ must beInvalid)
  }

  // e14: envelope's records list is empty - nothing to convert to events
  def e14 = {
    val payload = mkPayload("""{"requestId":"abc","records":[]}""".some)
    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map(_ must beInvalid)
  }

  // e15: log entry with x-host-header set to "-" yields no url parameter
  def e15 = {
    val entry = logEntry(hostHeader = "-")
    val body = firehosePayload(List(entry))
    val payload = mkPayload(body.some)

    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map { result =>
        result must beValid.like {
          case events => events.head.parameters.get("url") must beNone
        }
      }
  }

  // e17: c-ip set to "-" (CloudFront's null marker) → context.ipAddress is None
  def e17 = {
    val entry = logEntry(cIp = "-")
    val body = firehosePayload(List(entry))
    val payload = mkPayload(body.some)

    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map { result =>
        result must beValid.like {
          case events => events.head.context.ipAddress must beNone
        }
      }
  }

  // e16: `aid` from the collector URL (e.g. /com.amazon.aws.cloudfront/firehose?aid=cf-test)
  // is lifted onto every record in the batch.
  def e16 = {
    val body = firehosePayload(List(logEntry(uriStem = "/page1"), logEntry(uriStem = "/page2")))
    val payload = SpecHelpers.collectorPayload(
      api = Shared.api,
      querystring = SpecHelpers.toNameValuePairs("aid" -> "cf-test"),
      contentType = Some("application/json"),
      body = body.some,
      source = Shared.source,
      context = Shared.context
    )

    adapter
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, 40)
      .map { result =>
        result must beValid.like {
          case events =>
            events.size must_== 2
            events.toList.foreach { e =>
              e.parameters.get("aid") must beSome(Some("cf-test"))
            }
            events.head.parameters.get("aid") must beSome(Some("cf-test"))
        }
      }
  }
}
