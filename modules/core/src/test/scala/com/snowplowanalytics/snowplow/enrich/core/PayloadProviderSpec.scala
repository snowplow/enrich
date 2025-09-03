/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADED, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.core

import cats.syntax.option._
import cats.effect.testing.specs2.CatsEffect
import cats.effect.IO
import fs2.Stream
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.streams.TokenedEvents
import com.snowplowanalytics.snowplow.enrich.core.CompressionTestUtils._

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class PayloadProviderSpec extends Specification with CatsEffect {
  import PayloadProviderSpec._

  def is = s2"""
  PayloadProvider should
    handle uncompressed payloads only                                     $handleUncompressedOnly
    handle ZSTD compressed payloads only                                  $handleZstdOnly
    handle GZIP compressed payloads only                                  $handleGzipOnly
    handle mixture of compressed and uncompressed payloads                $handleMixedPayloads
    handle empty input                                                    $handleEmptyInput
    emit multiple batches when decompressed size exceeds maxBytesInBatch  $handleBatchSizeLimits
    reject payloads exceeding maxBytesSinglePayload size limit            $handlePayloadSizeLimit
    handle corrupt compressed data                                        $handleCorruptCompression
    handle unsupported compression header versions                        $handleUnsupportedVersions
    handle malformed compression signatures                               $handleMalformedSignatures
    handle very small buffers that don't match compression signatures     $handleSmallBuffers
    handle alternating valid and invalid compressed messages              $handleAlternatingValidInvalid
    handle large number of small payloads                                 $handleLargeNumberSmallPayloads
    handle zero-length payloads                                           $handleZeroLengthPayloads
  """

  def handleUncompressedOnly: IO[MatchResult[Any]] = {
    val payloads = List("payload1", "payload2", "payload3").map(s => ByteBuffer.wrap(s.getBytes))

    createTokenedEvents(payloads).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 3) and
          (results.head.payloads must containTheSameElementsAs(payloads)) and
          (results.head.bad must beEmpty) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  def handleZstdOnly: IO[MatchResult[Any]] =
    testSingleCompressionType(List("zstd payload 1", "zstd payload 2"), createZstdCompressedStream(_))

  def handleGzipOnly: IO[MatchResult[Any]] =
    testSingleCompressionType(List("gzip payload 1", "gzip payload 2"), createGzipCompressedStream(_))

  private def testSingleCompressionType(originalPayloads: List[String], compressionFn: List[String] => ByteBuffer): IO[MatchResult[Any]] = {
    val compressedPayload = compressionFn(originalPayloads)

    createTokenedEvents(List(compressedPayload)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 2) and
          (results.head.payloads.map(extractString) must containTheSameElementsAs(originalPayloads)) and
          (results.head.bad must beEmpty) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  def handleMixedPayloads: IO[MatchResult[Any]] = {
    val uncompressedPayloads = List("uncompressed1", "uncompressed2").map(s => ByteBuffer.wrap(s.getBytes))
    val zstdPayloads = List("zstd1", "zstd2")
    val gzipPayloads = List("gzip1", "gzip2")

    val compressedZstd = createZstdCompressedStream(zstdPayloads)
    val compressedGzip = createGzipCompressedStream(gzipPayloads)

    val allPayloads = uncompressedPayloads ++ List(compressedZstd, compressedGzip)

    createTokenedEvents(allPayloads).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 3) and
          (results(0).payloads must have length 2) and
          (results(0).payloads must containTheSameElementsAs(uncompressedPayloads)) and
          (results(0).bad must beEmpty) and
          (results(0).ack must beNone) and
          (results(1).payloads must have length 2) and
          (results(1).payloads.map(extractString) must containTheSameElementsAs(zstdPayloads)) and
          (results(1).bad must beEmpty) and
          (results(1).ack must beNone) and
          (results(2).payloads must have length 2) and
          (results(2).payloads.map(extractString) must containTheSameElementsAs(gzipPayloads)) and
          (results(2).bad must beEmpty) and
          (results(2).ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  def handleEmptyInput: IO[MatchResult[Any]] =
    createTokenedEvents(List.empty).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must beEmpty) and
          (results.head.bad must beEmpty) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }

  def handleBatchSizeLimits: IO[MatchResult[Any]] = {
    val largePayload1 = "x" * 300 // Each payload is 300 bytes
    val payloads = (1 to 20).map(_ => largePayload1).toList // Total would be 6000 bytes, exceeding 2000 byte limit

    val compressedPayload = createZstdCompressedStream(payloads)

    createTokenedEvents(List(compressedPayload)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 3) and // Should split into 3 batches
          (results(0).payloads must have length 7) and // First batch gets first payload
          (results(0).bad must beEmpty) and
          (results(0).ack must beNone) and // First batch should not have ack token
          (results(1).payloads must have length 7) and // Second batch gets second payload
          (results(1).bad must beEmpty) and
          (results(1).ack must beNone) and // Second batch should not have ack token
          (results(2).payloads must have length 6) and // Third batch gets third payload
          (results(2).bad must beEmpty) and
          (results(2).ack must beSome) // Third batch should have ack token
      }
    }
  }

  def handlePayloadSizeLimit: IO[MatchResult[Any]] = {
    val oversizedPayload = "x" * 1500 // Exceeds maxBytesSinglePayload (500)
    val validPayload = "valid payload"

    val compressedPayload = createZstdCompressedStream(List(oversizedPayload, validPayload))

    createTokenedEvents(List(compressedPayload)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 1) and // Only valid payload
          (results.head.payloads.map(extractString) must contain(validPayload)) and
          (results.head.bad must have length 1) and // One bad row for oversized payload
          (results.head.bad.head must beAnInstanceOf[BadRow.SizeViolation]) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  def handleCorruptCompression: IO[MatchResult[Any]] = {
    val corruptZstdPayload = createIncompleteCompressedStream(CompressionType.ZSTD, false)

    createTokenedEvents(List(corruptZstdPayload)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must beEmpty) and
          (results.head.bad must have length 1) and
          (results.head.bad.head must beAnInstanceOf[BadRow.CPFormatViolation]) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  def handleUnsupportedVersions: IO[MatchResult[Any]] = {
    val unsupportedVersionPayload = createZstdCompressedStreamWithVersions(
      List("test"),
      formatVersion = 99,
      payloadVersion = 1
    )

    createTokenedEvents(List(unsupportedVersionPayload)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must beEmpty) and
          (results.head.bad must have length 1) and
          (results.head.bad.head must beAnInstanceOf[BadRow.CPFormatViolation]) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  def handleMalformedSignatures: IO[MatchResult[Any]] = {
    // Create buffer that starts with partial ZSTD signature but isn't actually compressed
    val malformedBuffer = ByteBuffer.wrap(Array[Int](0x28, 0xb5, 0x2f).map(_.toByte)) // Incomplete ZSTD signature
    val payloads = List(malformedBuffer)

    createTokenedEvents(payloads).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 1) and // Treated as uncompressed
          (results.head.payloads must containTheSameElementsAs(payloads)) and
          (results.head.bad must beEmpty) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  def handleSmallBuffers: IO[MatchResult[Any]] = {
    val tinyBuffer = ByteBuffer.wrap(Array[Byte](0x01)) // Single byte, too small to match any signature
    val payloads = List(tinyBuffer)

    createTokenedEvents(payloads).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 1) and // Treated as uncompressed
          (results.head.payloads must containTheSameElementsAs(payloads)) and
          (results.head.bad must beEmpty) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  def handleAlternatingValidInvalid: IO[MatchResult[Any]] = {
    val validPayload = createZstdCompressedStream(List("valid"))
    val corruptPayload = createIncompleteCompressedStream(CompressionType.ZSTD, false)
    val validPayload2 = createZstdCompressedStream(List("valid2"))

    createTokenedEvents(List(validPayload, corruptPayload, validPayload2)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 2) and // Two valid payloads
          (results.head.payloads.map(extractString) must containTheSameElementsAs(List("valid", "valid2"))) and
          (results.head.bad must have length 1) and // One corrupt payload
          (results.head.bad.head must beAnInstanceOf[BadRow.CPFormatViolation]) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  def handleLargeNumberSmallPayloads: IO[MatchResult[Any]] = {
    val payloads = (1 to 50).map(i => s"small$i").toList
    val compressedPayload = createZstdCompressedStream(payloads)

    createTokenedEvents(List(compressedPayload)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 50) and
          (results.head.payloads.map(extractString) must containTheSameElementsAs(payloads)) and
          (results.head.bad must beEmpty) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  def handleZeroLengthPayloads: IO[MatchResult[Any]] = {
    val payloads = List("", "content", "") // Mix of empty and non-empty
    val compressedPayload = createZstdCompressedStream(payloads)

    createTokenedEvents(List(compressedPayload)).flatMap { tokenedEvents =>
      runPayloadProvider(tokenedEvents, testConfig).map { results =>
        (results must have length 1) and
          (results.head.payloads must have length 3) and
          (results.head.payloads.map(extractString) must containTheSameElementsAs(payloads)) and
          (results.head.bad must beEmpty) and
          (results.head.ack must beEqualTo(tokenedEvents.ack.some))
      }
    }
  }

  private def runPayloadProvider(tokenedEvents: TokenedEvents, config: Config.Decompression): IO[List[PayloadProvider.Result]] =
    Stream.emit[IO, TokenedEvents](tokenedEvents).through(PayloadProvider.pipe(testBadRowProcessor, config)).compile.toList

  private def extractString(buffer: ByteBuffer): String =
    new String(buffer.array(), buffer.position(), buffer.remaining(), StandardCharsets.UTF_8)
}

object PayloadProviderSpec {
  val testConfig = Config.Decompression(maxBytesInBatch = 2000, maxBytesSinglePayload = 500)

  val testBadRowProcessor = BadRowProcessor("test-enrich", "test-version")

  def createZstdCompressedStream(
    payloads: List[String],
    formatVersion: Byte = 1,
    payloadVersion: Byte = 1
  ): ByteBuffer =
    createCompressedStream(payloads.map(_.getBytes), CompressionType.ZSTD, formatVersion, payloadVersion)

  def createGzipCompressedStream(
    payloads: List[String],
    formatVersion: Byte = 1,
    payloadVersion: Byte = 1
  ): ByteBuffer =
    createCompressedStream(payloads.map(_.getBytes), CompressionType.GZIP, formatVersion, payloadVersion)

  def createZstdCompressedStreamWithVersions(
    payloads: List[String],
    formatVersion: Int,
    payloadVersion: Int
  ): ByteBuffer =
    createCompressedStream(payloads.map(_.getBytes), CompressionType.ZSTD, formatVersion.toByte, payloadVersion.toByte)
}
