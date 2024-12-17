/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.kinesis

import org.specs2.mutable.Specification

class SinkSpec extends Specification {

  private def getSize(record: String) = record.length

  "group" should {
    "leave a list untouched if its number of records and its size are below the limits" in {
      val records = List("a", "b", "c")
      val expected = List(records)
      val actual = Sink.group(records, 3, 4, getSize)
      actual.reverse.map(_.reverse) must beEqualTo(expected)
    }

    "split a list if it reaches recordLimit" in {
      val expected = List(List("a", "b", "c"), List("d", "e", "f"), List("g"))
      val records = expected.flatten
      val actual = Sink.group(records, 3, 4, getSize)
      actual.reverse.map(_.reverse) must beEqualTo(expected)
    }

    "split a list if it reaches sizeLimit" in {
      val expected = List(List("aa", "bb", "cc"), List("dd", "ee", "ff"), List("g"))
      val records = expected.flatten
      val actual = Sink.group(records, 3, 6, getSize)
      actual.reverse.map(_.reverse) must beEqualTo(expected)
    }

    "split a list if it reaches recordLimit and then sizeLimit" in {
      val expected = List(List("a", "b", "c"), List("dd", "ee", "ff"), List("g"))
      val records = expected.flatten
      val actual = Sink.group(records, 3, 6, getSize)
      actual.reverse.map(_.reverse) must beEqualTo(expected)
    }
  }
}
