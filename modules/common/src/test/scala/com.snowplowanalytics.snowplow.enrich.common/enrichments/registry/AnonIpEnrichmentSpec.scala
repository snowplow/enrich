/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import org.specs2.{ScalaCheck, Specification}
import org.specs2.matcher.DataTables
import org.scalacheck._
import org.scalacheck.Prop.forAll
import java.net.{Inet4Address, Inet6Address}
import com.google.common.net.{InetAddresses => GuavaInetAddress}

/**
 * Tests the anonymzeIp function
 */
class AnonIpEnrichmentSpec extends Specification with DataTables with ScalaCheck {

  def is = s2"""
  Anonymizing across a variety of IP addresses (v4 and v6) should work $e1
  Given valid IPv4 address and N octets to anonymize, the final value always contains N xs $e2
  Given valid IPv6 address and N segments to anonymize, the final value always contains N xs $e3
  Given valid shortened IPv6 address and N segments to anonymize, the final value always contains N xs $e4"""

  def e1 =
    "SPEC NAME" || "IP ADDRESS" | "ANONYMIZE IPv4 OCTETS" | "ANONYMIZE IPv6 PIECES" | "EXPECTED OUTPUT" |
      "valid, anonymize 1" !! "0.23.0.20" ! AnonIPv4Octets(1) ! AnonIPv6Segments(1) ! "0.23.0.x" |
      "valid, anonymize 2" !! "168.192.102.4" ! AnonIPv4Octets(2) ! AnonIPv6Segments(2) ! "168.192.x.x" |
      "valid, anonymize 3" !! "54.242.102.43" ! AnonIPv4Octets(3) ! AnonIPv6Segments(3) ! "54.x.x.x" |
      "valid, anonymize 4" !! "94.15.213.171" ! AnonIPv4Octets(4) ! AnonIPv6Segments(4) ! "x.x.x.x" |
      "invalid, anonymize 1" !! "777.2" ! AnonIPv4Octets(1) ! AnonIPv6Segments(1) ! "777.2" |
      "invalid, anonymize 4" !! "777.2.23" ! AnonIPv4Octets(4) ! AnonIPv6Segments(4) ! "x.x.x" |
      "invalid, anonymize 3" !! "999.123.777.2" ! AnonIPv4Octets(3) ! AnonIPv6Segments(3) ! "999.x.x.x" |
      "invalid, anonymize 3" !! "999.aaa.bbb.c" ! AnonIPv4Octets(3) ! AnonIPv6Segments(3) ! "999.x.x.x" |
      "invalid, anonymize 3" !! "hello;goodbye" ! AnonIPv4Octets(3) ! AnonIPv6Segments(3) ! "hello;goodbye" |
      "null, anonymize 2" !! null ! AnonIPv4Octets(2) ! AnonIPv6Segments(2) ! null |
      "empty, anonymize 4" !! "" ! AnonIPv4Octets(4) ! AnonIPv6Segments(4) ! "x" |
      "ipv6, anonymize 1" !! "4b0c:0:0:0:880c:99a8:4b0:4411" ! AnonIPv4Octets(1) ! AnonIPv6Segments(
        1
      ) ! "4b0c:0:0:0:880c:99a8:4b0:x" |
      "ipv6, anonymize 2" !! "4b0c::880c:99a8:4b0:4411" ! AnonIPv4Octets(2) ! AnonIPv6Segments(2) ! "4b0c:0:0:0:880c:99a8:x:x" |
      "ipv6, anonymize 3" !! "2605:2700:0:3:0:0:4713:93e3" ! AnonIPv4Octets(3) ! AnonIPv6Segments(3) ! "2605:2700:0:3:0:x:x:x" |
      "ipv6, anonymize 4" !! "2605:2700:0:3::4713:93e3" ! AnonIPv4Octets(4) ! AnonIPv6Segments(4) ! "2605:2700:0:3:x:x:x:x" |
      "ipv6, anonymize 5" !! "2605:2700:0:3::4713:93e3" ! AnonIPv4Octets(4) ! AnonIPv6Segments(5) ! "2605:2700:0:x:x:x:x:x" |
      "ipv6, anonymize 6" !! "2605:2700:0:3::4713:93e3" ! AnonIPv4Octets(4) ! AnonIPv6Segments(6) ! "2605:2700:x:x:x:x:x:x" |
      "ipv6, anonymize 7" !! "2605:2700:0:3::4713:93e3" ! AnonIPv4Octets(4) ! AnonIPv6Segments(7) ! "2605:x:x:x:x:x:x:x" |
      "ipv6, anonymize 8" !! "2605:2700:0:3::4713:93e3" ! AnonIPv4Octets(4) ! AnonIPv6Segments(8) ! "x:x:x:x:x:x:x:x" |
      "ipv6, anonymize 8" !! "2605:2700::4713:93e3" ! AnonIPv4Octets(4) ! AnonIPv6Segments(8) ! "x:x:x:x:x:x:x:x" |
      "ipv6, anonymize 8" !! "2700::4713" ! AnonIPv4Octets(4) ! AnonIPv6Segments(8) ! "x:x:x:x:x:x:x:x" |
      "ipv6, anonymize 8" !! "2700:zzzz::gggg" ! AnonIPv4Octets(4) ! AnonIPv6Segments(6) ! "2700:zzzz:x:x:x:x:x:x" |
      "hybrid ipv6 + ipv4" !! "f334::40cb:152.16.24.142" ! AnonIPv4Octets(4) ! AnonIPv6Segments(2) ! "f334:0:0:0:0:40cb:x:x" |
      "hybrid ipv6 + ipv4" !! "f334::40cb:152.16.24.142" ! AnonIPv4Octets(4) ! AnonIPv6Segments(4) ! "f334:0:0:0:x:x:x:x" |
      "ipv6, compat address" !! "::192.168.0.1" ! AnonIPv4Octets(4) ! AnonIPv6Segments(1) ! "0:0:0:0:0:0:c0a8:x" |
      "ipv4 mapped address" !! "::FFFF:152.16.24.123" ! AnonIPv4Octets(2) ! AnonIPv6Segments(4) ! "::FFFF:152.16.x.x" |
      "ipv4 mapped address" !! "::FFFF:152.16.24.123" ! AnonIPv4Octets(4) ! AnonIPv6Segments(4) ! "::FFFF:x.x.x.x" |> {
      (_, ip, octets, segments, expected) =>
        AnonIpEnrichment(octets, segments).anonymizeIp(ip) must_== expected
    }

  val ipv4Gen = Gen.listOfN(4, Gen.choose(0, 255)).map(_.mkString("."))
  val octetsNumGen = Gen.choose(1, 4)

  val hexCharGen =
    for {
      num <- Gen.numChar
      hex <- Gen.choose('a', 'f')
      hexNum <- Gen.oneOf(num, hex)
    } yield hexNum

  val segmentGen = Gen.listOfN(4, hexCharGen).map(_.mkString)

  //not shortened
  val ipv6Gen = Gen.listOfN(8, segmentGen).map(_.mkString(":"))
  val segmentsNumGen = Gen.choose(1, 8)

  val shortenedIPv6Gen =
    (for {
      segNum <- Gen.choose(2, 7)
      ip <- Gen.listOfN(segNum, segmentGen)
    } yield ip.updated(segNum - 1, s":${ip(segNum - 1)}")).map(_.mkString(":"))

  def e2 =
    forAll(ipv4Gen, octetsNumGen) {
      case (ip, octetsNum) =>
        val anon = AnonIpEnrichment(AnonIPv4Octets(octetsNum), AnonIPv6Segments(1)).anonymizeIp(ip)
        val countProp = anon.count(_ == 'x') ==== octetsNum
        val validIpProp = GuavaInetAddress
          .forString(anon.replace("x", "1"))
          .isInstanceOf[Inet4Address] ==== true
        countProp and validIpProp
    }

  def e3 =
    forAll(ipv6Gen, segmentsNumGen) {
      case (ip, segemntsNum) =>
        val anon =
          AnonIpEnrichment(AnonIPv4Octets(1), AnonIPv6Segments(segemntsNum)).anonymizeIp(ip)
        val countProp = anon.count(_ == 'x') ==== segemntsNum
        val validIpProp = GuavaInetAddress
          .forString(anon.replace("x", "1"))
          .isInstanceOf[Inet6Address] ==== true
        countProp and validIpProp
    }

  def e4 =
    forAll(shortenedIPv6Gen, segmentsNumGen) {
      case (ip, segemntsNum) =>
        val anon =
          AnonIpEnrichment(AnonIPv4Octets(1), AnonIPv6Segments(segemntsNum)).anonymizeIp(ip)
        val countProp = anon.count(_ == 'x') ==== segemntsNum
        val validIpProp = GuavaInetAddress
          .forString(anon.replace("x", "1"))
          .isInstanceOf[Inet6Address] ==== true
        countProp and validIpProp
    }

}
