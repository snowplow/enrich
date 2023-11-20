/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
