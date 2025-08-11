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
package com.snowplowanalytics.snowplow.enrich.common.loaders

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.ByteBuffer
import java.time.Instant
import java.util.{Base64, UUID}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import cats.data.ValidatedNel
import cats.implicits._

import org.joda.time.{DateTime, DateTimeZone}

import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TByteBuffer, TTransportException}

import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.{CollectorPayload => CollectorPayload1}

import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.FailureDetails.CPFormatViolationMessage

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}

/** Loader for Thrift SnowplowRawEvent objects. */
object ThriftLoader extends Loader[Array[Byte]] {

  private[loaders] val ExpectedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "CollectorPayload", "thrift", 1, 0)

  private def cpViolationForWrongSchemaKey(key: String): FailureDetails.CPFormatViolationMessage = {
    val str = s"verifying record as ${ExpectedSchema.asString} failed: found $key"
    FailureDetails.CPFormatViolationMessage.Fallback(str)
  }

  /**
   * Converts the source string into a [[CollectorPayload]] (always `Some`)
   * Checks the version of the raw event and calls the appropriate method.
   * @param line A serialized Thrift object Byte array mapped to a String. The method calling this
   * should encode the serialized object with `snowplowRawEventBytes.map(_.toChar)`.
   * Reference: http://stackoverflow.com/questions/5250324/
   * @param processor Processing asset
   * @return either a set of validation errors or an Option-boxed CanonicalInput object, wrapped in
   * a ValidatedNel.
   */
  override def toCollectorPayload(
    bytes: Array[Byte],
    processor: Processor,
    etlTstamp: Instant
  ): ValidatedNel[BadRow.CPFormatViolation, CollectorPayload] =
    toCollectorPayload(ByteBuffer.wrap(bytes), processor, etlTstamp)

  def toCollectorPayload(
    buffer: ByteBuffer,
    processor: Processor,
    etlTstamp: Instant
  ): ValidatedNel[BadRow.CPFormatViolation, CollectorPayload] = {
    buffer.mark()

    def createViolation(message: FailureDetails.CPFormatViolationMessage) = {
      buffer.reset()
      BadRow.CPFormatViolation(
        processor,
        Failure.CPFormatViolation(etlTstamp, "thrift", message),
        Payload.RawPayload(toBase64String(buffer))
      )
    }

    val collectorPayload =
      try convertSchema1(buffer)
      catch {
        case tte: TTransportException if tte.getType === TTransportException.END_OF_FILE =>
          FailureDetails.CPFormatViolationMessage
            .Fallback(s"error deserializing raw event: Reached end of bytes when parsing as thrift format")
            .invalidNel
        case NonFatal(e) =>
          FailureDetails.CPFormatViolationMessage
            .Fallback(s"error deserializing raw event: ${e.getMessage}")
            .invalidNel
      }

    collectorPayload.leftMap { messages =>
      messages.map(createViolation)
    }
  }

  /**
   * Converts the source string into a ValidatedMaybeCollectorPayload.
   * Assumes that the byte array is a serialized CollectorPayload, version 1.
   * @param line A serialized Thrift object Byte array mapped to a String. The method calling this
   * should encode the serialized object with`snowplowRawEventBytes.map(_.toChar)`.
   * Reference: http://stackoverflow.com/questions/5250324/
   * @return either a set of validation errors or an Option-boxed CanonicalInput object, wrapped in
   * a ValidatedNel.
   */
  private def convertSchema1(
    input: ByteBuffer
  ): ValidatedNel[FailureDetails.CPFormatViolationMessage, CollectorPayload] = {
    val collectorPayload = new CollectorPayload1
    val transport = new TByteBuffer(input)
    val protocol = new TBinaryProtocol(transport)
    collectorPayload.read(protocol)

    val querystring = parseQuerystring(
      Option(collectorPayload.querystring),
      Charset.forName(collectorPayload.encoding)
    )

    val hostname = Option(collectorPayload.hostname)
    val userAgent = Option(collectorPayload.userAgent)
    val refererUri = Option(collectorPayload.refererUri)
    val networkUserId =
      Option(collectorPayload.networkUserId).traverse(parseNetworkUserId).toValidatedNel

    val headers = Option(collectorPayload.headers).map(_.asScala.toList).getOrElse(Nil)

    val ip = Option(IpAddressExtractor.extractIpAddress(headers, collectorPayload.ipAddress)) // Required

    val api = Option(collectorPayload.path) match {
      case None =>
        FailureDetails.CPFormatViolationMessage
          .InputData("path", None, "request does not contain a path")
          .invalidNel
      case Some(p) => CollectorPayload.parseApi(p).toValidatedNel
    }

    val schemaKeyStr = collectorPayload.getSchema

    val validatedSchema = for {
      schemaKey <- SchemaKey.fromUri(schemaKeyStr).leftMap(_ => cpViolationForWrongSchemaKey(schemaKeyStr))
      _ <- if (ExpectedSchema.matches(schemaKey)) Right(()) else Left(cpViolationForWrongSchemaKey(schemaKeyStr))
    } yield ()

    (querystring.toValidatedNel, api, networkUserId, validatedSchema.toValidatedNel).mapN { (q, a, nuid, _) =>
      val source =
        CollectorPayload.Source(collectorPayload.collector, collectorPayload.encoding, hostname)
      val context = CollectorPayload.Context(
        new DateTime(collectorPayload.timestamp, DateTimeZone.UTC),
        ip,
        userAgent,
        refererUri,
        headers,
        nuid
      )
      CollectorPayload(
        a,
        q,
        Option(collectorPayload.contentType),
        Option(collectorPayload.body),
        source,
        context
      )
    }
  }

  private def parseNetworkUserId(str: String): Either[FailureDetails.CPFormatViolationMessage, UUID] =
    Either
      .catchOnly[IllegalArgumentException](UUID.fromString(str))
      .leftMap(_ => CPFormatViolationMessage.InputData("networkUserId", Some(str), "not valid UUID"))

  private def toBase64String(
    bb: ByteBuffer
  ): String =
    StandardCharsets.UTF_8.decode(Base64.getEncoder.encode(bb)).toString

}
