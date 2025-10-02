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
package com.snowplowanalytics.snowplow.enrich.core

import com.github.luben.zstd.ZstdOutputStream

import cats.effect.IO
import cats.effect.kernel.Unique

import fs2.{Chunk, Stream}

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.zip.GZIPOutputStream

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.streams.TokenedEvents

object CompressionTestUtils {

  sealed trait CompressionType
  object CompressionType {
    case object GZIP extends CompressionType
    case object ZSTD extends CompressionType
  }

  def createCompressedStream(
    records: List[Array[Byte]],
    compressionType: CompressionType,
    formatVersion: Byte = 1,
    payloadVersion: Byte = 1
  ): ByteBuffer =
    createCompressedStreamWithVersions(records, compressionType, formatVersion, payloadVersion)

  def createCompressedStreamWithVersions(
    records: List[Array[Byte]],
    compressionType: CompressionType,
    formatVersion: Byte,
    payloadVersion: Byte
  ): ByteBuffer = {
    val baos = new ByteArrayOutputStream()
    val compressionStream: OutputStream = compressionType match {
      case CompressionType.GZIP => new GZIPOutputStream(baos)
      case CompressionType.ZSTD => new ZstdOutputStream(baos)
    }

    // Write header
    compressionStream.write(formatVersion.toInt)
    compressionStream.write(payloadVersion.toInt)

    // Write records
    records.foreach { record =>
      val sizeBytes = ByteBuffer.allocate(4)
      sizeBytes.order(ByteOrder.BIG_ENDIAN)
      sizeBytes.putInt(record.length)
      compressionStream.write(sizeBytes.array())
      compressionStream.write(record)
    }

    compressionStream.close()
    ByteBuffer.wrap(baos.toByteArray)
  }

  def createIncompleteCompressedStream(
    compressionType: CompressionType,
    incompleteSize: Boolean
  ): ByteBuffer = {
    val baos = new ByteArrayOutputStream()
    val compressionStream: OutputStream = compressionType match {
      case CompressionType.GZIP => new GZIPOutputStream(baos)
      case CompressionType.ZSTD => new ZstdOutputStream(baos)
    }

    // Write header
    compressionStream.write(1) // format version
    compressionStream.write(1) // payload version

    if (incompleteSize)
      // Write incomplete size (only 2 bytes instead of 4)
      compressionStream.write(Array[Byte](0, 1))
    else {
      // Write complete size but incomplete data
      val sizeBytes = ByteBuffer.allocate(4)
      sizeBytes.order(ByteOrder.BIG_ENDIAN)
      sizeBytes.putInt(10) // Claim 10 bytes
      compressionStream.write(sizeBytes.array())
      compressionStream.write(Array[Byte](1, 2, 3)) // Only write 3 bytes
    }

    compressionStream.close()
    ByteBuffer.wrap(baos.toByteArray)
  }

  def createTokenedEvents(payloads: List[ByteBuffer]): IO[TokenedEvents] =
    Unique[IO].unique.map(t => TokenedEvents(Chunk.from(payloads), t))

  case class CompressedBatch(value: List[CollectorPayload], compressionType: CompressionType)

  def mkCompressedStream(batches: (CompressedBatch, Unique.Token)*): Stream[IO, TokenedEvents] =
    Stream.emits(batches).map {
      case (batch, ack) =>
        val compressedBytes = createCompressedStream(batch.value.map(_.toRaw), batch.compressionType)
        TokenedEvents(Chunk.singleton(compressedBytes), ack)
    }

  /**
   * Creates a zstd stream that will cause ZstdIOException during record reading.
   */
  def createCorruptedZstdWithTrailingNull(): ByteBuffer = {
    // Create a minimal valid zstd stream with proper Snowplow format header
    val baos = new ByteArrayOutputStream()
    val compressionStream = new ZstdOutputStream(baos)

    // Write valid Snowplow header per the spec
    compressionStream.write(1) // format version
    compressionStream.write(1) // payload version
    compressionStream.close()

    val validBytes = baos.toByteArray

    // Add corrupted data that will cause ZstdIOException during record parsing
    val corruptedData = Array[Byte](0, 0, 0, 5) ++ // Fake record size (5 bytes)
      Array.fill[Byte](20)(0) // Null bytes that cause ZstdIOException

    ByteBuffer.wrap(validBytes ++ corruptedData)
  }
}
