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

import cats.effect.testing.specs2.CatsEffect
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import scala.util.Random

import com.snowplowanalytics.snowplow.enrich.core.Decompressor._
import com.snowplowanalytics.snowplow.enrich.core.CompressionTestUtils._

class DecompressorSpec extends Specification with CatsEffect {

  def is = s2"""
  Decompressor should
    decompress single record from compressed streams                             $extractSingleRecord
    decompress multiple records from compressed streams                          $extractMultipleRecords
    reject record exceeding max allowed byte limit and continue with next        $exceedMaxByteLimitAndContinue
    continue to return EndOfRecords after first EndOfRecords return              $continueToReturnEndOfRecords
    handle unsupported header versions                                           $ensureUnsupportedVersionsRejected
    handle corrupt input with incomplete size header                             $ensureIncompleteSize
    handle corrupt input with incomplete record data                             $ensureIncompleteRecord
    handle empty compressed stream after header                                  $ensureEmptyStreamHandled
    handle zero-length record                                                    $ensureZeroLengthRecordSupported
    handle very large number of small records                                    $ensureLargeVolumeProcessing
    handle mixed record sizes                                                    $ensureMixedSizesSupported
  """

  def extractSingleRecord = {
    val record = "test record data"
    testBothCompressionTypes { compressionType =>
      val compressed = createCompressedStream(List(record.getBytes("UTF-8")), compressionType)
      val factory = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike {
        case FactorySuccess(decompressor) =>
          decompressor.getNextRecord must beLike {
            case Record(data) =>
              new String(data, "UTF-8") must beEqualTo(record)
          }
      }
    }
  }

  def extractMultipleRecords = {
    val expectedStrings = List("first record", "second record", "third record")
    val records = expectedStrings.map(_.getBytes("UTF-8"))
    testBothCompressionTypes { compressionType =>
      validateRecordsInOrder(records, compressionType)
    }
  }

  def exceedMaxByteLimitAndContinue = {
    val smallRecord1 = "small record"
    val smallRecord2 = "another small record"
    val oversizedRecordSize = 2000
    val oversizedRecord = new Array[Byte](oversizedRecordSize)
    val records = List(
      smallRecord1.getBytes("UTF-8"),
      oversizedRecord,
      smallRecord2.getBytes("UTF-8")
    )
    Random.nextBytes(oversizedRecord)
    testBothCompressionTypes { compressionType =>
      val compressed = createCompressedStream(records, compressionType)
      val factory = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike {
        case FactorySuccess(decompressor) =>
          val results = List(
            decompressor.getNextRecord,
            decompressor.getNextRecord,
            decompressor.getNextRecord,
            decompressor.getNextRecord
          )
          results must beLike {
            case List(Record(r1), RecordTooBig(size), Record(r3), EndOfRecords) =>
              (new String(r1, "UTF-8") must beEqualTo(smallRecord1)) and
                (size must beEqualTo(oversizedRecordSize)) and
                (new String(r3, "UTF-8") must beEqualTo(smallRecord2))
          }
      }
    }
  }

  def continueToReturnEndOfRecords = {
    val record1 = "test record data 1"
    val record2 = "test record data 2"
    val records = List(record1, record2).map(_.getBytes("UTF-8"))
    testBothCompressionTypes { compressionType =>
      val compressed = createCompressedStream(records, compressionType)
      val factory = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike {
        case FactorySuccess(decompressor) =>
          val results = List(
            decompressor.getNextRecord,
            decompressor.getNextRecord,
            decompressor.getNextRecord,
            decompressor.getNextRecord,
            decompressor.getNextRecord
          )
          results must beLike {
            case List(Record(r1), Record(r3), EndOfRecords, EndOfRecords, EndOfRecords) =>
              (new String(r1, "UTF-8") must beEqualTo(record1)) and
                (new String(r3, "UTF-8") must beEqualTo(record2))
          }
      }
    }
  }

  def ensureUnsupportedVersionsRejected =
    testBothCompressionTypes { compressionType =>
      val compressed = createCompressedStreamWithVersions(
        List("test".getBytes("UTF-8")),
        compressionType,
        formatVersion = 2, // Unsupported version
        payloadVersion = 1
      )
      val factory = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike {
        case UnsupportedVersionsInHeader(v1, v2) =>
          (v1 must beEqualTo(2)) and (v2 must beEqualTo(1))
      }
    }

  def ensureIncompleteSize =
    testBothCompressionTypes { compressionType =>
      val compressed = createIncompleteCompressedStream(compressionType, incompleteSize = true)
      val factory = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike {
        case FactorySuccess(decompressor) =>
          decompressor.getNextRecord must beEqualTo(CorruptInput)
      }
    }

  def ensureIncompleteRecord =
    testBothCompressionTypes { compressionType =>
      val compressed = createIncompleteCompressedStream(compressionType, incompleteSize = false)
      val factory = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike {
        case FactorySuccess(decompressor) =>
          decompressor.getNextRecord must beEqualTo(CorruptInput)
      }
    }

  def ensureEmptyStreamHandled =
    testBothCompressionTypes { compressionType =>
      val compressed = createCompressedStream(List.empty[Array[Byte]], compressionType)
      val factory = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike {
        case FactorySuccess(decompressor) =>
          decompressor.getNextRecord must beEqualTo(EndOfRecords)
      }
    }

  def ensureZeroLengthRecordSupported = {
    val records = List(new Array[Byte](0)) // Zero-length record
    testBothCompressionTypes { compressionType =>
      val compressed = createCompressedStream(records, compressionType)
      val factory = getFactory(compressionType, maxBytesSinglePayload = 1000)
      factory.build(compressed) must beLike {
        case FactorySuccess(decompressor) =>
          decompressor.getNextRecord must beLike {
            case Record(data) => data.length must beEqualTo(0)
          }
      }
    }
  }

  def ensureLargeVolumeProcessing = {
    val records = (1 to 100).map(i => s"record$i".getBytes("UTF-8")).toList
    testBothCompressionTypes { compressionType =>
      validateRecordsInOrder(records, compressionType)
    }
  }

  def ensureMixedSizesSupported = {
    val records = List("tiny", "medium " * 10, "large " * 50, "x").map(_.getBytes("UTF-8"))
    testBothCompressionTypes { compressionType =>
      validateRecordsInOrder(records, compressionType)
    }
  }

  private def validateRecordsInOrder(
    records: List[Array[Byte]],
    compressionType: CompressionType,
    maxBytesSinglePayload: Int = 1000
  ): MatchResult[FactoryResult] = {
    val compressed = createCompressedStream(records, compressionType)
    val factory = getFactory(compressionType, maxBytesSinglePayload)
    factory.build(compressed) must beLike {
      case FactorySuccess(decompressor) =>
        val results = (1 to records.length + 1).map(_ => decompressor.getNextRecord).toList
        val (recordResults, endResult) = results.splitAt(records.length)

        (recordResults.zip(records).forall {
          case (Record(actual), expected) => actual.sameElements(expected)
          case _ => false
        } must beTrue) and
          (endResult must beEqualTo(List(EndOfRecords)))
    }
  }

  private def testBothCompressionTypes[T](testFn: CompressionType => MatchResult[T]): MatchResult[T] =
    testFn(CompressionType.GZIP) and testFn(CompressionType.ZSTD)

  private def getFactory(compressionType: CompressionType, maxBytesSinglePayload: Int): Decompressor.Factory =
    compressionType match {
      case CompressionType.GZIP => new Decompressor.Gzip(maxBytesSinglePayload)
      case CompressionType.ZSTD => new Decompressor.Zstd(maxBytesSinglePayload)
    }
}
