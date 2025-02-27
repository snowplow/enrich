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
package com.snowplowanalytics.snowplow.enrich.common.utils

import java.net.{Inet6Address, InetAddress, URI}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import cats.syntax.either._
import cats.syntax.option._

import org.scalacheck.Gen
import org.scalacheck.Arbitrary._

import org.specs2.{ScalaCheck, Specification}
import org.specs2.mutable.{Specification => MSpecification}
import org.specs2.matcher.DataTables

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

class StringToUriSpec extends MSpecification with DataTables {

  /** Helper to generate URLs with `chars` at different places in the path and in the query string, doubled, tripled, etc. */
  private def generateUrlsWithChars(chars: String): List[String] =
    List(
      s"http://www.example.com/a/b/$chars",
      s"http://www.example.com/a$chars",
      s"http://www.example.com/a$chars$chars",
      s"http://www.example.com/a$chars$chars${chars}b/c",
      s"http://www.example.com/a${chars}/c$chars${chars}d",
      s"http://www.example.com/a${chars}b/456?d=e$chars${chars}f&g=h${chars}i&j=k",
      s"http://www.example.com/a${chars}b/c?d=e&f=g$chars$chars${chars}h"
    )

  "Parsing string into URI should" >> {
    "work with null" >> {
      ConversionUtils.stringToUri(null) must_== None.asRight
    }

    "work with hostname having underscore" >> {
      val url = "http://www.ex_ample.com"
      ConversionUtils.stringToUri(url) must_== Some(URI.create(url)).asRight
    }

    "work with basic URL and not modify it" >> {
      (List(
        "http://www.example.com",
        "http://www.example.com/",
        "http://www.example.com/a",
        "http://www.google.com/se+arch?q=gateway+oracle+cards+denise+linn&hl=en&client=safari"
      ) ++ generateUrlsWithChars(""))
        .map(url => ConversionUtils.stringToUri(url) must_== Some(URI.create(url)).asRight)
    }

    "work with URL with space and encode spaces as %20" >> {
      val url = "http://www.example.com/sp a ce"
      ConversionUtils.stringToUri(url) must_== Some(URI.create(url.replaceAll(" ", "%20"))).asRight
    }

    "work with correctly percent-encoded URL and not modify it" >> {
      val url = "www.example.com/a%23b/?c=d%24e"
      ConversionUtils.stringToUri(url) must_== Some(URI.create(url)).asRight
    }

    s"work with URL containing special characters or macros" >> {

      /** Helper that encodes a URI in the same way as scala-uri. */
      def encode(str: String) = {
        val encoded = str
          .replaceAll("%", "%25")
          .replaceAll(" ", "%20")
          .replaceAll("\\|", "%7C")
          .replaceAll("\\$", "%24")
          .replaceAll("\\{", "%7B")
          .replaceAll("\\}", "%7D")
          .replaceAll("\\[", "%5B")
          .replaceAll("\\]", "%5D")
          // 1st # is not encoded, all subsequent are
          .replaceAll("#", "%23")
          .replaceFirst("%23", "#")

        // after a #, / ? = & are also encoded
        encoded.indexOf("#") match {
          case -1 => encoded
          case i =>
            val untilSharp = encoded.substring(0, i)
            val afterSharpEncoded = encoded
              .substring(i)
              .replaceAll("\\?", "%3F")
              .replaceAll("/", "%2F")
              .replaceAll("=", "%3D")
              .replaceAll("&", "%26")
            untilSharp + afterSharpEncoded
        }
      }

      val urls = generateUrlsWithChars("|") ++
        generateUrlsWithChars("${a}") ++
        generateUrlsWithChars(s"$${a b}") ++
        generateUrlsWithChars("$[a]") ++
        generateUrlsWithChars("$[a b]") ++
        generateUrlsWithChars("#{a}") ++
        generateUrlsWithChars("#{a b}") ++
        generateUrlsWithChars("#{{a}}") ++
        generateUrlsWithChars("#{{a b}}") ++
        generateUrlsWithChars("#a#") ++
        generateUrlsWithChars("#a b#") ++
        generateUrlsWithChars("##a##") ++
        generateUrlsWithChars("##a b##") ++
        generateUrlsWithChars("%a%") ++
        generateUrlsWithChars("%a b%") ++
        generateUrlsWithChars("%%a%%") ++
        generateUrlsWithChars("%%a b%%") ++
        generateUrlsWithChars("%%%a%%%") ++
        generateUrlsWithChars("%%%a b%%%")

      urls
        .map(url => ConversionUtils.stringToUri(url) must_== Some(URI.create(encode(url))).asRight)
    }
  }
}

class ExplodeUriSpec extends Specification with DataTables {

  def is = s2"""
  Exploding URIs into their component pieces with explodeUri should work $e1
  """

  def e1 =
    "SPEC NAME" || "URI" | "EXP. SCHEME" | "EXP. HOST" | "EXP. PORT" | "EXP. PATH" | "EXP. QUERY" | "EXP. FRAGMENT" |
      "With path, qs & #" !! "http://www.psychicbazaar.com/oracles/119-psycards-deck.html?view=print#detail" ! "http" ! "www.psychicbazaar.com" ! 80 ! Some(
        "/oracles/119-psycards-deck.html"
      ) ! Some("view=print") ! Some("detail") |
      "With path & space in qs" !! "http://psy.bz/genre/all/type/all?utm_source=google&utm_medium=cpc&utm_term=buy%2Btarot&utm_campaign=spring_sale" ! "http" ! "psy.bz" ! 80 ! Some(
        "/genre/all/type/all"
      ) ! Some("utm_source=google&utm_medium=cpc&utm_term=buy%2Btarot&utm_campaign=spring_sale") ! None |
      "With path & no www" !! "http://snowplowanalytics.com/analytics/index.html" ! "http" ! "snowplowanalytics.com" ! 80 ! Some(
        "/analytics/index.html"
      ) ! None ! None |
      "Port specified" !! "http://www.nbnz.co.nz:440/login.asp" ! "http" ! "www.nbnz.co.nz" ! 440 ! Some(
        "/login.asp"
      ) ! None ! None |
      "HTTPS & #" !! "https://www.lancs.ac.uk#footer" ! "https" ! "www.lancs.ac.uk" ! 443 ! None ! None ! Some(
        "footer"
      ) |
      "www2 & trailing /" !! "https://www2.williamhill.com/" ! "https" ! "www2.williamhill.com" ! 443 ! Some(
        "/"
      ) ! None ! None |
      "Tab & newline in qs" !! "http://www.ebay.co.uk/sch/i.html?_from=R40&_trksid=m570.l2736&_nkw=%09+Clear+Quartz+Point+Rock+Crystal%0ADowsing+Pendulum" ! "http" ! "www.ebay.co.uk" ! 80 ! Some(
        "/sch/i.html"
      ) ! Some(
        "_from=R40&_trksid=m570.l2736&_nkw=%09+Clear+Quartz+Point+Rock+Crystal%0ADowsing+Pendulum"
      ) ! None |
      "Tab & newline in path" !! "https://snowplowanalytics.com/analytic%0As/index%09nasty.html" ! "https" ! "snowplowanalytics.com" ! 443 ! Some(
        "/analytic%0As/index%09nasty.html"
      ) ! None ! None |
      "Tab & newline in #" !! "http://psy.bz/oracles/psycards.html?view=print#detail%09is%0Acorrupted" ! "http" ! "psy.bz" ! 80 ! Some(
        "/oracles/psycards.html"
      ) ! Some("view=print") ! Some("detail%09is%0Acorrupted") |> { (_, uri, scheme, host, port, path, query, fragment) =>
      val actual = ConversionUtils.explodeUri(new URI(uri))
      val expected = ConversionUtils.UriComponents(scheme, host, port, path, query, fragment)
      actual must_== expected

    }
}

class FixTabsNewlinesSpec extends Specification with DataTables {

  val SafeTab = "    "

  def is = s2"""
  Replacing tabs, newlines and control characters with fixTabsNewlines should work $e1
  """

  def e1 =
    "SPEC NAME" || "INPUT STR" | "EXPECTED" |
      "Empty string" !! "" ! None |
      "String with true-tab" !! "	" ! SafeTab.some |
      "String with \\t" !! "\t" ! SafeTab.some |
      "String with \\\\t" !! "\\\t" ! "\\%s".format(SafeTab).some |
      "String with \\b" !! "\b" ! None |
      "String ending in newline" !! "Hello\n" ! "Hello".some |
      "String with control char" !! "\u0002" ! None |
      "String with space" !! "\u0020" ! " ".some |
      "String with black diamond" !! "�" ! "�".some |
      "String with everything" !! "Hi	\u0002�\u0020\bJo\t\u0002" ! "Hi%s� Jo%s"
        .format(SafeTab, SafeTab)
        .some |> { (_, str, expected) =>
      ConversionUtils.fixTabsNewlines(str) must_== expected
    }
}

// TODO: note that we have some functionality tweaks planned.
// See comments on ConversionUtils.decodeBase64Url for details.
class DecodeBase64UrlSpec extends Specification with DataTables with ScalaCheck {
  def is = s2"""
  decodeBase64Url should return failure if passed a null                          $e1
  decodeBase64Url should not return failure on any other string                   $e2
  decodeBase64Url should correctly decode valid Base64 (URL-safe) encoded strings $e3
  """

  // Only way of getting a failure currently
  def e1 = ConversionUtils.decodeBase64Url(null) must beLeft("Could not base64 decode: null")

  // No string creates a failure
  def e2 =
    prop { (str: String) =>
      ConversionUtils.decodeBase64Url(str) must beRight
    }

  // Taken from:
  // 1. Lua Tracker's base64_spec.lua
  // 2. Manual tests of the JavaScript Tracker's trackUnstructEvent()
  // 3. Misc edge cases worth checking
  def e3 =
    "SPEC NAME" || "ENCODED STRING" | "EXPECTED" |
      "Lua Tracker String #1" !! "Sm9oblNtaXRo" ! "JohnSmith" |
      "Lua Tracker String #2" !! "am9obitzbWl0aA" ! "john+smith" |
      "Lua Tracker String #3" !! "Sm9obiBTbWl0aA" ! "John Smith" |
      "Lua Tracker JSON #1" !! "eyJhZ2UiOjIzLCJuYW1lIjoiSm9obiJ9" ! """{"age":23,"name":"John"}""" |
      "Lua Tracker JSON #2" !! "eyJteVRlbXAiOjIzLjMsIm15VW5pdCI6ImNlbHNpdXMifQ" ! """{"myTemp":23.3,"myUnit":"celsius"}""" |
      "Lua Tracker JSON #3" !! "eyJldmVudCI6InBhZ2VfcGluZyIsIm1vYmlsZSI6dHJ1ZSwicHJvcGVydGllcyI6eyJtYXhfeCI6OTYwLCJtYXhfeSI6MTA4MCwibWluX3giOjAsIm1pbl95IjotMTJ9fQ" ! """{"event":"page_ping","mobile":true,"properties":{"max_x":960,"max_y":1080,"min_x":0,"min_y":-12}}""" |
      "Lua Tracker JSON #4" !! "eyJldmVudCI6ImJhc2tldF9jaGFuZ2UiLCJwcmljZSI6MjMuMzksInByb2R1Y3RfaWQiOiJQQlowMDAzNDUiLCJxdWFudGl0eSI6LTIsInRzdGFtcCI6MTY3ODAyMzAwMH0" ! """{"event":"basket_change","price":23.39,"product_id":"PBZ000345","quantity":-2,"tstamp":1678023000}""" |
      "JS Tracker JSON #1" !! "eyJwcm9kdWN0X2lkIjoiQVNPMDEwNDMiLCJjYXRlZ29yeSI6IkRyZXNzZXMiLCJicmFuZCI6IkFDTUUiLCJyZXR1cm5pbmciOnRydWUsInByaWNlIjo0OS45NSwic2l6ZXMiOlsieHMiLCJzIiwibCIsInhsIiwieHhsIl0sImF2YWlsYWJsZV9zaW5jZSRkdCI6MTU4MDF9" ! """{"product_id":"ASO01043","category":"Dresses","brand":"ACME","returning":true,"price":49.95,"sizes":["xs","s","l","xl","xxl"],"available_since$dt":15801}""" |
      "Unescaped characters" !! "äöü - &" ! "" |
      "Blank string" !! "" ! "" |> { (_, str, expected) =>
      ConversionUtils.decodeBase64Url(str) must beRight(expected)
    }
}

class ValidateUuidSpec extends Specification with DataTables with ScalaCheck {
  def is = s2"""
  validateUuid should return a lowercased UUID for a valid lower/upper-case UUID       $e1
  validateUuid should fail if the supplied String is not a valid lower/upper-case UUID $e2
  """

  val FieldName = "uuid"

  def e1 =
    "SPEC NAME" || "INPUT STR" | "EXPECTED" |
      "Lowercase UUID #1" !! "f732d278-120e-4ab6-845b-c1f11cd85dc7" ! "f732d278-120e-4ab6-845b-c1f11cd85dc7" |
      "Lowercase UUID #2" !! "a729d278-110a-4ac6-845b-d1f12ce45ac7" ! "a729d278-110a-4ac6-845b-d1f12ce45ac7" |
      "Uppercase UUID #1" !! "A729D278-110A-4AC6-845B-D1F12CE45AC7" ! "a729d278-110a-4ac6-845b-d1f12ce45ac7" |
      "Uppercase UUID #2" !! "F732D278-120E-4AB6-845B-C1F11CD85DC7" ! "f732d278-120e-4ab6-845b-c1f11cd85dc7" |> {
      // Note: MS-style {GUID} is not supported

      (_, str, expected) =>
        ConversionUtils.validateUuid(FieldName, str) must beRight(expected)
    }

  // A bit of fun: the chances of generating a valid UUID at random are
  // so low that we can just use ScalaCheck here. Checks null too
  def e2 =
    prop { (str: String) =>
      ConversionUtils.validateUuid(FieldName, str) must beLeft(
        AtomicError.ParseError("Not a valid UUID", FieldName, Option(str))
      )
    }
}

class ValidateIntegerSpec extends Specification {
  def is = s2"""
  validateInteger should return the original string if it contains an integer                     $e1
  validateInteger should return an enrichment failure for a string not containing a valid integer $e2
  """

  val FieldName = "integer"

  def e1 = ConversionUtils.validateInteger(FieldName, "123") must beRight("123")

  def e2 = {
    val str = "abc"
    ConversionUtils.validateInteger(FieldName, str) must beLeft(
      AtomicError.ParseError("Not a valid integer", FieldName, Some(str))
    )
  }
}

class DecodeStringSpec extends Specification {
  def is = s2"""
  decodeString should decode a correctly URL-encoded string            $e1
  decodeString should fail decoding a string not correctly URL-encoded $e2
  """

  val utf8 = StandardCharsets.UTF_8

  def e1 = {
    val clear = "12 ++---=&&3abc%%%34%2234%$#@%^PLLPbgfxbf$#%$@#@^"
    val encoded = ConversionUtils.encodeString(utf8.toString(), clear)
    ConversionUtils.decodeString(utf8, encoded) must beRight(clear)
  }

  def e2 =
    ConversionUtils.decodeString(utf8, "%%23") must beLeft
}

class StringToDoubleLikeSpec extends Specification with DataTables {
  def is = s2"""
  stringToDoublelike should fail if the supplied String is not parseable as a number                    $e1
  stringToDoublelike should convert numeric Strings to 'Double-like' Strings loadable by Redshift       $e2
  stringToDoublelike will alas *not* fail numbers having more significant digits than Redshift supports $e3
  """

  val FieldName = "val"
  def err(value: String): AtomicError =
    AtomicError.ParseError("Cannot be converted to Double-like", FieldName, Option(value))

  def e1 =
    "SPEC NAME" || "INPUT STR" | "EXPECTED" |
      "Empty string" !! "" ! err("") |
      "Number with commas" !! "19,999.99" ! err("19,999.99") |
      "Hexadecimal number" !! "0x54" ! err("0x54") |
      "Bad sci. notation" !! "-7.51E^9" ! err("-7.51E^9") |
      "German number" !! "1.000,3932" ! err("1.000,3932") |
      "NaN" !! "NaN" ! err("NaN") |
      "English string" !! "hi & bye" ! err("hi & bye") |
      "Vietnamese name" !! "Trịnh Công Sơn" ! err("Trịnh Công Sơn") |> { (_, str, expected) =>
      ConversionUtils.stringToDoubleLike(FieldName, str) must beLeft(expected)
    }

  def e2 =
    "SPEC NAME" || "INPUT STR" | "EXPECTED" |
      "Integer #1" !! "23" ! "23" |
      "Integer #2" !! "23." ! "23" |
      "Negative integer" !! "-2012103" ! "-2012103" |
      "Null value (raw)" !! null ! null |
      "Null value (String)" !! "null" ! null |
      "Arabic number" !! "٤٥٦٧.٦٧" ! "4567.67" |
      "Floating point #1" !! "1999.99" ! "1999.99" |
      "Floating point #2" !! "1999.00" ! "1999.00" |
      "Floating point #3" !! "78694353.00001" ! "78694353.00001" |
      "Floating point #4" !! "-78694353.00001" ! "-78694353.00001" |
      "Sci. notation #1" !! "4.321768E3" ! "4321.768" |
      "Sci. notation #2" !! "6.72E9" ! "6720000000" |
      "Sci. notation #3" !! "7.51E-9" ! "0.00000000751" |> { (_, str, expected) =>
      ConversionUtils.stringToDoubleLike(FieldName, str) must beRight(expected)
    }

  val BigNumber = "78694235323.00000001" // Redshift only supports 15 significant digits for a Double
  def e3 = ConversionUtils.stringToDoubleLike(FieldName, BigNumber) must beRight(BigNumber)

}

class StringToJIntegerSpec extends Specification with DataTables {
  def is = s2"""
  stringToJInteger should fail if the supplied String is not parseable as an Integer $e1
  stringToJInteger should convert valid Strings to Java Integers                     $e2
  """

  val err: String = "Cannot be converted to java.lang.Integer"

  def e1 =
    "SPEC NAME" || "INPUT STR" | "EXPECTED" |
      "Empty string" !! "" ! err |
      "Floating point #1" !! "1999." ! err |
      "Floating point #2" !! "1999.00" ! err |
      "Hexadecimal number" !! "0x54" ! err |
      "NaN" !! "NaN" ! err |
      "Sci. notation" !! "6.72E5" ! err |> { (_, str, expected) =>
      ConversionUtils.stringToJInteger(str) must beLeft(expected)
    }

  def e2 =
    "SPEC NAME" || "INPUT STR" | "EXPECTED" |
      "Integer #1" !! "0" ! 0 |
      "Integer #2" !! "23" ! 23 |
      "Negative integer #1" !! "-2012103" ! -2012103 |
      "Negative integer #2" !! "-1" ! -1 |
      "Null" !! null ! null |> { (_, str, expected) =>
      ConversionUtils.stringToJInteger(str) must beRight(expected)
    }
}

class StringToBooleanLikeJByteSpec extends Specification with DataTables {
  def is = s2"""
  stringToBooleanlikeJByte should fail if the supplied String is not parseable as a 1 or 0 JByte           $e1
  stringToBooleanlikeJByte should convert '1' or '0' Strings to 'Boolean-like' JBytes loadable by Redshift $e2
  """

  val FieldName = "val"
  def err(value: String): AtomicError =
    AtomicError.ParseError("Cannot be converted to Boolean-like java.lang.Byte", FieldName, Option(value))

  def e1 =
    "SPEC NAME" || "INPUT STR" | "EXPECTED" |
      "Empty string" !! "" ! err("") |
      "Small number" !! "2" ! err("2") |
      "Negative number" !! "-1" ! err("-1") |
      "Floating point number" !! "0.0" ! err("0.0") |
      "Large number" !! "19,999.99" ! err("19,999.99") |
      "Text #1" !! "a" ! err("a") |
      "Text #2" !! "0x54" ! err("0x54") |> { (_, str, expected) =>
      ConversionUtils.stringToBooleanLikeJByte(FieldName, str) must beLeft(expected)
    }

  def e2 =
    "SPEC NAME" || "INPUT STR" | "EXPECTED" |
      "True aka 1" !! "1" ! 1.toByte |
      "False aka 0" !! "0" ! 0.toByte |> { (_, str, expected) =>
      ConversionUtils.stringToBooleanLikeJByte(FieldName, str) must beRight(expected)
    }
}

class ExtractQueryStringSpec extends Specification {
  import java.nio.charset.StandardCharsets.UTF_8
  val baseUri = "http://foo.bar?"

  def is = s2"""
  extractQuerystring should extract a query string param          $e1
  extractQuerystring should assign None to a param without value  $e2
  extractQuerystring should assign "" to a param with empty value $e3
  extractQuerystring should return several tuples with same key   $e4
  """

  def e1 =
    ConversionUtils.extractQuerystring(new URI(s"${baseUri}a=b"), UTF_8) must beRight(List(("a" -> Some("b"))))

  def e2 =
    ConversionUtils.extractQuerystring(new URI(s"${baseUri}a"), UTF_8) must beRight(List(("a" -> None)))

  def e3 =
    ConversionUtils.extractQuerystring(new URI(s"${baseUri}a="), UTF_8) must beRight(List(("a" -> Some(""))))

  def e4 =
    ConversionUtils.extractQuerystring(new URI(s"${baseUri}a=b&a=c"), UTF_8) must beRight(List(("a" -> Some("b")), ("a" -> Some("c"))))
}

class ExtractInetAddressSpec extends Specification with ScalaCheck {
  def is = s2"""
  extractInetAddress should return None on invalid string $e1
  extractInetAddress should return Some on every valid IPv6 $e2
  """

  def e1 =
    ConversionUtils.extractInetAddress("unknown") must beNone

  def e2 =
    prop { (ip: String) =>
      ConversionUtils.extractInetAddress(ip) must beSome
    }.setGen(ipv6Gen.map(_.toInet6Address.getHostAddress))

  // Implementation taken from http4s tests suite
  private case class Ipv6Address(
    a: Short,
    b: Short,
    c: Short,
    d: Short,
    e: Short,
    f: Short,
    g: Short,
    h: Short
  ) {
    def toInet6Address: Inet6Address = {
      val byteBuffer = ByteBuffer.allocate(16)
      byteBuffer.putShort(a)
      byteBuffer.putShort(b)
      byteBuffer.putShort(c)
      byteBuffer.putShort(d)
      byteBuffer.putShort(e)
      byteBuffer.putShort(f)
      byteBuffer.putShort(g)
      byteBuffer.putShort(h)
      InetAddress.getByAddress(byteBuffer.array).asInstanceOf[Inet6Address]
    }
  }

  private val ipv6Gen: Gen[Ipv6Address] =
    for {
      a <- Gen.chooseNum(Short.MinValue, Short.MaxValue)
      b <- Gen.chooseNum(Short.MinValue, Short.MaxValue)
      c <- Gen.chooseNum(Short.MinValue, Short.MaxValue)
      d <- Gen.chooseNum(Short.MinValue, Short.MaxValue)
      e <- Gen.chooseNum(Short.MinValue, Short.MaxValue)
      f <- Gen.chooseNum(Short.MinValue, Short.MaxValue)
      g <- Gen.chooseNum(Short.MinValue, Short.MaxValue)
      h <- Gen.chooseNum(Short.MinValue, Short.MaxValue)
    } yield Ipv6Address(a, b, c, d, e, f, g, h)
}

class GetPiiEventSpec extends MSpecification {

  "Extracting a Pii event should" >> {
    "return None if the pii field is null" >> {
      val event = {
        val e = new EnrichedEvent
        e.platform = "web"
        e
      }
      val processor = Processor("sce-test-suite", "1.0.0")
      ConversionUtils.getPiiEvent(processor, event) shouldEqual None
    }
    "return a new event if the pii field is present" in {
      val event = {
        val e = new EnrichedEvent
        e.pii = "pii"
        e.event_id = "id"
        e
      }
      val processor = Processor("sce-test-suite", "1.0.0")
      val Some(e) = ConversionUtils.getPiiEvent(processor, event)
      e.unstruct_event shouldEqual "pii"
      e.platform shouldEqual "srv"
      e.event shouldEqual "pii_transformation"
      e.event_vendor shouldEqual "com.snowplowanalytics.snowplow"
      e.event_format shouldEqual "jsonschema"
      e.event_name shouldEqual "pii_transformation"
      e.event_version shouldEqual "1-0-0"
      e.v_etl shouldEqual "sce-test-suite-1.0.0"
      e.contexts must contain(
        """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/parent_event/jsonschema/1-0-0","data":{"parentEventId":"id"}"""
      )
    }
  }
}

class TabSeparatedEnrichedEventSpec extends MSpecification {

  "make a tabSeparatedEnrichedEvent function available" >> {
    "which tsv format an enriched event" >> {
      val event = {
        val e = new EnrichedEvent
        e.platform = "web"
        e
      }
      ConversionUtils.tabSeparatedEnrichedEvent(event) must contain("web")
    }
    "which filter the pii field" in {
      val event = {
        val e = new EnrichedEvent
        e.platform = "web"
        e.pii = "pii"
        e
      }
      ConversionUtils.tabSeparatedEnrichedEvent(event) must not(contain("pii"))
    }
  }
}
