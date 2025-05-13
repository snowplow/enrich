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

import java.lang.{Byte => JByte, Integer => JInteger}
import java.math.{BigDecimal => JBigDecimal}
import java.net.{InetAddress, URI, URLDecoder, URLEncoder}
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import java.io.{PrintWriter, StringWriter}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

import cats.syntax.either._
import cats.syntax.option._

import inet.ipaddr.HostName

import io.circe.Json
import io.lemonlabs.uri.{Uri, Url}
import io.lemonlabs.uri.config.UriConfig
import io.lemonlabs.uri.decoding.PercentDecoder
import io.lemonlabs.uri.encoding.percentEncode

import org.apache.commons.codec.binary.Base64
import org.apache.http.client.utils.URLEncodedUtils
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.badrows.{FailureDetails, Processor}

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.MiscEnrichments
import com.snowplowanalytics.snowplow.enrich.common.QueryStringParameters

/** General-purpose utils to help the ETL process along. */
object ConversionUtils {
  private val UrlSafeBase64 = new Base64(true) // true means "url safe"

  /** Simple case class wrapper around the components of a URI. */
  final case class UriComponents(
    // Required
    scheme: String,
    host: String,
    port: JInteger,
    // Optional
    path: Option[String],
    query: Option[String],
    fragment: Option[String]
  )

  /**
   * Explodes a URI into its 6 components pieces. Simple code but we use it in multiple places
   * @param uri The URI to explode into its constituent pieces
   * @return The 6 components in a UriComponents case class
   */
  def explodeUri(uri: URI): UriComponents = {
    val port = uri.getPort

    // TODO: should we be using decodeString below instead?
    // Trouble is we can't be sure of the querystring's encoding.
    val query = fixTabsNewlines(uri.getRawQuery)
    val path = fixTabsNewlines(uri.getRawPath)
    val fragment = fixTabsNewlines(uri.getRawFragment)

    UriComponents(
      scheme = uri.getScheme,
      host = uri.getHost,
      port =
        if (port == -1 && uri.getScheme == "https")
          443
        else if (port == -1)
          80
        else
          port,
      path = path,
      query = query,
      fragment = fragment
    )
  }

  /**
   * Quick helper to make sure our Strings are TSV-safe, i.e. don't include tabs, special
   * characters, newlines, etc.
   * @param str The string we want to make safe
   * @return a safe String
   */
  def makeTsvSafe(str: String): String =
    fixTabsNewlines(str).orNull

  /**
   * Replaces tabs with four spaces and removes newlines altogether.
   * Useful to prepare user-created strings for fragile storage formats like TSV.
   * @param str The String to fix
   * @return The String with tabs and newlines fixed.
   */
  def fixTabsNewlines(str: String): Option[String] =
    Option(str)
      .map(_.replaceAll("\\t", "    ").replaceAll("\\p{Cntrl}", ""))
      .flatMap {
        case "" => None
        case other => Some(other)
      }

  /**
   * Decodes a URL-safe Base64 string.
   * For details on the Base 64 Encoding with URL and Filename Safe Alphabet see:
   * http://tools.ietf.org/html/rfc4648#page-7
   * @param str The encoded string to be decoded
   * @return a Scalaz Validation, wrapping either an
   * an error String or the decoded String
   */
  // TODO: probably better to change the functionality and signature
  // a little:
  // 1. Signature -> : Validation[String, Option[String]]
  // 2. Functionality:
  // 1. If passed in null or "", return Success(None)
  // 2. If passed in a non-empty string but result == "", then return a Failure, because we have failed to decode something meaningful
  def decodeBase64Url(str: String): Either[String, String] =
    Either
      .catchNonFatal {
        val decodedBytes = UrlSafeBase64.decode(str)
        val result = new String(decodedBytes, UTF_8) // Must specify charset (EMR uses US_ASCII)
        result
      }
      .leftMap(e => s"Could not base64 decode: ${e.getMessage}")

  /**
   * Encodes a URL-safe Base64 string.
   * For details on the Base 64 Encoding with URL and Filename Safe Alphabet see:
   * http://tools.ietf.org/html/rfc4648#page-7
   * @param str The string to be encoded
   * @return the string encoded in URL-safe Base64
   */
  def encodeBase64Url(str: String): String = {
    val bytes = UrlSafeBase64.encode(str.getBytes)
    new String(bytes, UTF_8).trim // Newline being appended by some Base64 versions
  }

  /**
   * Validates that the given field contains a valid UUID.
   * @param field The name of the field being validated
   * @param str The String hopefully containing a UUID
   * @return either the original String, or an error String
   */
  val validateUuid: (String, String) => Either[AtomicError.ParseError, String] =
    (field, str) => {
      def check(s: String)(u: UUID): Boolean = u != null && s.toLowerCase == u.toString
      val uuid = Try(UUID.fromString(str)).toOption.filter(check(str))
      uuid match {
        case Some(_) => str.toLowerCase.asRight
        case None => AtomicError.ParseError("Not a valid UUID", field, Option(str)).asLeft
      }
    }

  /**
   * @param field The name of the field being validated
   * @param str The String hopefully parseable as an integer
   * @return either the original String, or an error String
   */
  val validateInteger: (String, String) => Either[AtomicError.ParseError, String] =
    (field, str) => {
      Either
        .catchNonFatal { str.toInt; str }
        .leftMap { _ =>
          AtomicError.ParseError("Not a valid integer", field, Option(str))
        }
    }

  /**
   * Decodes a String in the specific encoding, also removing:
   * * Newlines - because they will break Hive
   * * Tabs - because they will break non-Hive
   *          targets (e.g. Infobright)
   * IMPLDIFF: note that this version, unlike the Hive serde version, does not call
   * cleanUri. This is because we cannot assume that str is a URI which needs 'cleaning'.
   * TODO: simplify this when we move to a more robust output format (e.g. Avro) - as then
   * no need to remove line breaks, tabs etc
   * @param enc The encoding of the String
   * @param str The String to decode
   * @return either an error String or the decoded String
   */
  val decodeString: (Charset, String) => Either[String, String] = (enc, str) =>
    try {
      // TODO: switch to style of fixTabsNewlines above
      // TODO: potentially switch to using fixTabsNewlines too to avoid duplication
      val s = Option(str).getOrElse("")
      val d = URLDecoder.decode(s, enc.toString)
      d.replaceAll("(\\r|\\n)", "").replaceAll("\\t", "    ").asRight
    } catch {
      case NonFatal(e) => s"Exception URL-decoding (encoding $enc): ${e.getMessage}".asLeft
    }

  /**
   * On 17th August 2013, Amazon made an unannounced change to their CloudFront
   * log format - they went from always encoding % characters, to only encoding % characters
   * which were not previously encoded. For a full discussion of this see:
   * https://forums.aws.amazon.com/thread.jspa?threadID=134017&tstart=0#
   * On 14th September 2013, Amazon rolled out a further fix, from which point onwards all fields,
   * including the referer and useragent, would have %s double-encoded.
   * This causes issues, because the ETL process expects referers and useragents to be only
   * single-encoded.
   * This function turns a double-encoded percent (%) into a single-encoded one.
   * Examples:
   * 1. "page=Celestial%25Tarot"          -   no change (only single encoded)
   * 2. "page=Dreaming%2520Way%2520Tarot" -> "page=Dreaming%20Way%20Tarot"
   * 3. "loading 30%2525 complete"        -> "loading 30%25 complete"
   * Limitation of this approach: %2588 is ambiguous. Is it a:
   * a) A double-escaped caret "ˆ" (%2588 -> %88 -> ^), or:
   * b) A single-escaped "%88" (%2588 -> %88)
   * This code assumes it's a).
   * @param str The String which potentially has double-encoded %s
   * @return the String with %s now single-encoded
   */
  def singleEncodePcts(str: String): String =
    str.replaceAll("%25([0-9a-fA-F][0-9a-fA-F])", "%$1") // Decode %25XX to %XX

  /**
   * Decode double-encoded percents, then percent decode
   * @param str The String to decode
   * @return a Scalaz Validation, wrapping either an error String or the decoded String
   */
  def doubleDecode(str: String): Either[String, String] =
    decodeString(UTF_8, singleEncodePcts(str))

  /**
   * Encodes a string in the specified encoding
   * @param enc The encoding to be used
   * @param str The string which needs to be URLEncoded
   * @return a URL encoded string
   */
  def encodeString(enc: String, str: String): String =
    URLEncoder.encode(str, enc)

  /**
   * Parses a string to create a URI.
   * Parsing is relaxed, i.e. even if a URL is not correctly percent-encoded or not RFC 3986-compliant, it can be parsed.
   * @param uri String containing the URI to parse.
   * @return either:
   * - the parsed URI if there was no error or with None if the input was `null`.
   * - the error message if something went wrong.
   */
  def stringToUri(uri: String): Either[String, Option[URI]] =
    Either
      .catchNonFatal(
        Option(uri) // to handle null
          .map(_.replaceAll(" ", "%20"))
          .map(URI.create)
      )
      .leftFlatMap { javaErr =>
        implicit val c =
          UriConfig(
            decoder = PercentDecoder(ignoreInvalidPercentEncoding = true),
            encoder = percentEncode -- '+'
          )
        Uri
          .parseTry(uri)
          .map(_.toJavaURI) match {
          case util.Success(javaURI) =>
            Some(javaURI).asRight
          case util.Failure(scalaErr) =>
            "Provided URI [%s] could not be parsed, neither by Java parsing (error: [%s]) nor by Scala parsing (error: [%s])."
              .format(uri, javaErr.getMessage, scalaErr.getMessage)
              .asLeft
        }
      }

  /**
   * Attempt to extract the querystring from a URI
   * @param uri URI containing the querystring
   * @param encoding Encoding of the URI
   */
  def extractQuerystring(uri: URI, encoding: Charset): Either[FailureDetails.EnrichmentFailure, QueryStringParameters] =
    Try(URLEncodedUtils.parse(uri, encoding).asScala.map(p => (p.getName -> Option(p.getValue))))
      .recoverWith {
        case NonFatal(_) =>
          Try(Url.parse(uri.toString).query.params)
      } match {
      case util.Success(params) => params.toList.asRight
      case util.Failure(e) =>
        val msg = s"Could not parse uri. Error: [$e]."
        val f = FailureDetails.EnrichmentFailureMessage.InputData(
          "uri",
          Option(uri).map(_.toString()),
          msg
        )
        FailureDetails.EnrichmentFailure(None, f).asLeft
    }

  /**
   * Extract a Scala Int from a String, or error.
   * @param str The String which we hope is an Int
   * @return either a Failure String or a Success JInt
   */
  val stringToJInteger: String => Either[String, JInteger] = str =>
    Option(str) match {
      case None =>
        null.asInstanceOf[JInteger].asRight
      case Some(s) if s.toLowerCase == "null" =>
        null.asInstanceOf[JInteger].asRight
      case Some(s) =>
        try {
          val jint: JInteger = s.toInt
          jint.asRight
        } catch {
          case _: NumberFormatException =>
            "Cannot be converted to java.lang.Integer".asLeft
        }
    }

  val stringToJInteger2: (String, String) => Either[AtomicError.ParseError, JInteger] =
    (field, str) =>
      stringToJInteger(str).leftMap { e =>
        AtomicError.ParseError(e, field, Option(str))
      }

  val stringToJBigDecimal: String => Either[String, JBigDecimal] = str =>
    Option(str) match {
      case None =>
        null.asInstanceOf[JBigDecimal].asRight
      case Some(s) if s.toLowerCase == "null" =>
        null.asInstanceOf[JBigDecimal].asRight
      case Some(s) =>
        Either
          .catchNonFatal(new JBigDecimal(s))
          .flatMap { bd =>
            if (bd.scale < 0)
              // Make sure the big integer will be serialized without scientific notation
              Either.catchNonFatal(bd.setScale(0))
            else
              Right(bd)
          }
          .leftMap(e => s"Cannot be converted to java.math.BigDecimal. Error : ${e.getMessage}")
    }

  val stringToJBigDecimal2: (String, String) => Either[AtomicError.ParseError, JBigDecimal] =
    (field, str) =>
      stringToJBigDecimal(str).leftMap { e =>
        AtomicError.ParseError(e, field, Option(str))
      }

  /**
   * Convert a String to a String containing a Redshift-compatible Double.
   * Necessary because Redshift does not support all Java Double syntaxes e.g. "3.4028235E38"
   * Note that this code does NOT check that the value will fit within a Redshift Double -
   * meaning Redshift may silently round this number on load.
   * @param str The String which we hope contains a Double
   * @param field The name of the field we are validating. To use in our error message
   * @return either a failure or a String
   */
  val stringToDoubleLike: (String, String) => Either[AtomicError.ParseError, String] =
    (field, str) =>
      Either
        .catchNonFatal {
          if (Option(str).isEmpty || str == "null")
            // "null" String check is LEGACY to handle a bug in the JavaScript tracker
            null.asInstanceOf[String]
          else {
            val jbigdec = new JBigDecimal(str)
            jbigdec.toPlainString // Strip scientific notation
          }
        }
        .leftMap { _ =>
          val msg = "Cannot be converted to Double-like"
          AtomicError.ParseError(msg, field, Option(str))
        }

  /**
   * Convert a String to a Double
   * @param str The String which we hope contains a Double
   * @param field The name of the field we are validating. To use in our error message
   * @return a Scalaz Validation, being either a Failure String or a Success Double
   */
  def stringToMaybeDouble(field: String, str: String): Either[AtomicError.ParseError, Option[Double]] =
    Either
      .catchNonFatal {
        if (Option(str).isEmpty || str == "null")
          // "null" String check is LEGACY to handle a bug in the JavaScript tracker
          None
        else {
          val jbigdec = new JBigDecimal(str)
          jbigdec.doubleValue().some
        }
      }
      .leftMap(_ => AtomicError.ParseError("Cannot be converted to Double", field, Option(str)))

  /** Convert a java BigDecimal to a Double */
  def jBigDecimalToDouble(field: String, f: JBigDecimal): Either[AtomicError.ParseError, Option[Double]] =
    Either
      .catchNonFatal {
        Option(f).map(_.doubleValue)
      }
      .leftMap(_ => AtomicError.ParseError("Cannot be converted to Double", field, Option(f).map(_.toString)))

  /** Convert a java BigDecimal to a Double */
  def jBigDecimalToDouble(
    field: String,
    f: JBigDecimal,
    enrichmentInfo: FailureDetails.EnrichmentInformation
  ): Either[FailureDetails.EnrichmentFailure, Option[Double]] =
    jBigDecimalToDouble(field, f)
      .leftMap { error =>
        FailureDetails.EnrichmentFailure(
          Some(enrichmentInfo),
          FailureDetails.EnrichmentFailureMessage.InputData(
            error.field,
            Option(f).map(_.toString),
            error.message
          )
        )
      }

  /** Convert a Double to a java BigDecimal */
  def doubleToJBigDecimal(field: String, d: Option[Double]): Either[AtomicError.ParseError, Option[JBigDecimal]] =
    Either
      .catchNonFatal {
        d.map(dd => new JBigDecimal(dd))
      }
      .leftMap(_ => AtomicError.ParseError("Cannot be converted to java BigDecimal", field, d.map(_.toString)))

  /**
   * Converts a String to a Double with two decimal places. Used to honor schemas with
   * multipleOf 0.01.
   * Takes a field name and a string value and return a validated double.
   */
  val stringToTwoDecimals: String => Either[String, Double] = str =>
    try BigDecimal(str).setScale(2, BigDecimal.RoundingMode.HALF_EVEN).toDouble.asRight
    catch {
      case _: NumberFormatException => "Cannot be converted to Double".asLeft
    }

  /**
   * Converts a String to a Double.
   * Takes a field name and a string value and return a validated float.
   */
  val stringToDouble: String => Either[String, Double] = str =>
    Either
      .catchNonFatal(BigDecimal(str).toDouble)
      .leftMap(_ => s"Cannot be converted to Double")

  /**
   * Extract a Java Byte representing 1 or 0 only from a String, or error.
   * @param str The String which we hope is an Byte
   * @param field The name of the field we are trying to process. To use in our error message
   * @return either a Failure String or a Success Byte
   */
  val stringToBooleanLikeJByte: (String, String) => Either[AtomicError.ParseError, JByte] =
    (field, str) =>
      str match {
        case "1" => (1.toByte: JByte).asRight
        case "0" => (0.toByte: JByte).asRight
        case _ =>
          val msg = "Cannot be converted to Boolean-like java.lang.Byte"
          AtomicError.ParseError(msg, field, Option(str)).asLeft
      }

  /**
   * Converts a String of value "1" or "0" to true or false respectively.
   * @param str The String to convert
   * @return True for "1", false for "0", or an error message for any other value, all boxed in a
   * Scalaz Validation
   */
  val stringToBoolean: String => Either[String, Boolean] = str =>
    if (str == "1")
      true.asRight
    else if (str == "0")
      false.asRight
    else
      s"Cannot be converted to boolean, only 1 or 0 are supported".asLeft

  /**
   * Truncates a String - useful for making sure Strings can't overflow a database field.
   * @param str The String to truncate
   * @param length The maximum length of the String to keep
   * @return the truncated String
   */
  def truncate(str: String, length: Int): String =
    if (str == null)
      null
    else
      str.take(length)

  /**
   * Helper to convert a Boolean value to a Byte. Does not require any validation.
   * @param bool The Boolean to convert into a Byte
   * @return 0 if false, 1 if true
   */
  def booleanToJByte(bool: Boolean): JByte =
    (if (bool) 1 else 0).toByte

  def parseUrlEncodedForm(s: String): Either[String, Map[String, Option[String]]] =
    for {
      r <- Either
             .catchNonFatal(URLEncodedUtils.parse(URI.create("http://localhost/?" + s), UTF_8))
             .leftMap(_.getMessage)
      nvps = r.asScala.toList
      pairs = nvps.map(p => p.getName() -> Option(p.getValue()))
    } yield pairs.toMap

  /** Extract valid IP (v4 or v6) address from a string */
  def extractInetAddress(arg: String): Option[InetAddress] =
    Option(new HostName(arg).asInetAddress)

  private val eeTstampFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")

  /** Creates a PII event from the pii field of an existing event. */
  def getPiiEvent(processor: Processor, originalEvent: EnrichedEvent): Option[EnrichedEvent] =
    originalEvent.pii
      .map { pii =>
        val piiEvent = new EnrichedEvent
        piiEvent.unstruct_event = Some(pii)
        piiEvent.app_id = originalEvent.app_id
        piiEvent.platform = "srv"
        piiEvent.etl_tstamp = originalEvent.etl_tstamp
        piiEvent.collector_tstamp = originalEvent.collector_tstamp
        piiEvent.event = "pii_transformation"
        piiEvent.event_id = UUID.randomUUID().toString
        piiEvent.derived_tstamp = eeTstampFormatter.print(DateTime.now(DateTimeZone.UTC))
        piiEvent.true_tstamp = piiEvent.derived_tstamp
        piiEvent.event_vendor = "com.snowplowanalytics.snowplow"
        piiEvent.event_format = "jsonschema"
        piiEvent.event_name = "pii_transformation"
        piiEvent.event_version = "1-0-0"
        piiEvent.v_etl = MiscEnrichments.etlVersion(processor)
        piiEvent.contexts = getContextParentEvent(originalEvent.event_id)
        piiEvent
      }

  private def getContextParentEvent(eventId: String): List[SelfDescribingData[Json]] =
    List(
      SelfDescribingData(
        SchemaKey("com.snowplowanalytics.snowplow", "parent_event", "jsonschema", SchemaVer.Full(1, 0, 0)),
        Json.obj("parentEventId" -> Json.fromString(eventId))
      )
    )

  /** Format an EnrichedEvent as a TSV. */
  def tabSeparatedEnrichedEvent(e: EnrichedEvent): String =
    Array[String](
      forTsv(e.app_id),
      forTsv(e.platform),
      forTsv(e.etl_tstamp),
      forTsv(e.collector_tstamp),
      forTsv(e.dvce_created_tstamp),
      forTsv(e.event),
      forTsv(e.event_id),
      forTsv(e.txn_id),
      forTsv(e.name_tracker),
      forTsv(e.v_tracker),
      forTsv(e.v_collector),
      forTsv(e.v_etl),
      forTsv(e.user_id),
      forTsv(e.user_ipaddress),
      forTsv(e.user_fingerprint),
      forTsv(e.domain_userid),
      forTsv(e.domain_sessionidx),
      forTsv(e.network_userid),
      forTsv(e.geo_country),
      forTsv(e.geo_region),
      forTsv(e.geo_city),
      forTsv(e.geo_zipcode),
      forTsv(e.geo_latitude),
      forTsv(e.geo_longitude),
      forTsv(e.geo_region_name),
      forTsv(e.ip_isp),
      forTsv(e.ip_organization),
      forTsv(e.ip_domain),
      forTsv(e.ip_netspeed),
      forTsv(e.page_url),
      forTsv(e.page_title),
      forTsv(e.page_referrer),
      forTsv(e.page_urlscheme),
      forTsv(e.page_urlhost),
      forTsv(e.page_urlport),
      forTsv(e.page_urlpath),
      forTsv(e.page_urlquery),
      forTsv(e.page_urlfragment),
      forTsv(e.refr_urlscheme),
      forTsv(e.refr_urlhost),
      forTsv(e.refr_urlport),
      forTsv(e.refr_urlpath),
      forTsv(e.refr_urlquery),
      forTsv(e.refr_urlfragment),
      forTsv(e.refr_medium),
      forTsv(e.refr_source),
      forTsv(e.refr_term),
      forTsv(e.mkt_medium),
      forTsv(e.mkt_source),
      forTsv(e.mkt_term),
      forTsv(e.mkt_content),
      forTsv(e.mkt_campaign),
      forTsv(e.getContexts()),
      forTsv(e.se_category),
      forTsv(e.se_action),
      forTsv(e.se_label),
      forTsv(e.se_property),
      forTsv(e.se_value),
      forTsv(e.getUnstruct_event()),
      forTsv(e.tr_orderid),
      forTsv(e.tr_affiliation),
      forTsv(e.tr_total),
      forTsv(e.tr_tax),
      forTsv(e.tr_shipping),
      forTsv(e.tr_city),
      forTsv(e.tr_state),
      forTsv(e.tr_country),
      forTsv(e.ti_orderid),
      forTsv(e.ti_sku),
      forTsv(e.ti_name),
      forTsv(e.ti_category),
      forTsv(e.ti_price),
      forTsv(e.ti_quantity),
      forTsv(e.pp_xoffset_min),
      forTsv(e.pp_xoffset_max),
      forTsv(e.pp_yoffset_min),
      forTsv(e.pp_yoffset_max),
      forTsv(e.useragent),
      forTsv(e.br_name),
      forTsv(e.br_family),
      forTsv(e.br_version),
      forTsv(e.br_type),
      forTsv(e.br_renderengine),
      forTsv(e.br_lang),
      forTsv(e.br_features_pdf),
      forTsv(e.br_features_flash),
      forTsv(e.br_features_java),
      forTsv(e.br_features_director),
      forTsv(e.br_features_quicktime),
      forTsv(e.br_features_realplayer),
      forTsv(e.br_features_windowsmedia),
      forTsv(e.br_features_gears),
      forTsv(e.br_features_silverlight),
      forTsv(e.br_cookies),
      forTsv(e.br_colordepth),
      forTsv(e.br_viewwidth),
      forTsv(e.br_viewheight),
      forTsv(e.os_name),
      forTsv(e.os_family),
      forTsv(e.os_manufacturer),
      forTsv(e.os_timezone),
      forTsv(e.dvce_type),
      forTsv(e.dvce_ismobile),
      forTsv(e.dvce_screenwidth),
      forTsv(e.dvce_screenheight),
      forTsv(e.doc_charset),
      forTsv(e.doc_width),
      forTsv(e.doc_height),
      forTsv(e.tr_currency),
      forTsv(e.tr_total_base),
      forTsv(e.tr_tax_base),
      forTsv(e.tr_shipping_base),
      forTsv(e.ti_currency),
      forTsv(e.ti_price_base),
      forTsv(e.base_currency),
      forTsv(e.geo_timezone),
      forTsv(e.mkt_clickid),
      forTsv(e.mkt_network),
      forTsv(e.etl_tags),
      forTsv(e.dvce_sent_tstamp),
      forTsv(e.refr_domain_userid),
      forTsv(e.refr_dvce_tstamp),
      forTsv(e.getDerived_contexts()),
      forTsv(e.domain_sessionid),
      forTsv(e.derived_tstamp),
      forTsv(e.event_vendor),
      forTsv(e.event_name),
      forTsv(e.event_format),
      forTsv(e.event_version),
      forTsv(e.event_fingerprint),
      forTsv(e.true_tstamp)
    ).mkString("\t")

  private def forTsv(v: String): String =
    if (v != null) v else ""

  private def forTsv(v: java.lang.Integer): String =
    if (v != null) v.toString else ""

  private def forTsv(v: java.lang.Byte): String =
    if (v != null) v.toString else ""

  private def forTsv(v: java.lang.Float): String =
    if (v != null) v.toString else ""

  private def forTsv(v: java.math.BigDecimal): String =
    if (v != null) v.toString else ""

  def cleanStackTrace(t: Throwable): String = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
}
