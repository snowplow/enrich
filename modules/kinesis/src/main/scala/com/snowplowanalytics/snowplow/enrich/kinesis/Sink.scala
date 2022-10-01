/*
 * Copyright (c) 2021-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.kinesis

import java.nio.ByteBuffer
import java.util.UUID

import scala.collection.JavaConverters._

import cats.implicits._
import cats.{Applicative, Parallel}

import cats.effect.{Blocker, Concurrent, ContextShift, Resource, Sync, Timer}
import cats.effect.concurrent.Ref

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import retry.syntax.all._
import retry.RetryPolicies._
import retry.RetryPolicy

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration

import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}

import com.snowplowanalytics.snowplow.enrich.common.fs2.{AttributedByteSink, AttributedData, ByteSink}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{BackoffPolicy, Output}

object Sink {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def init[F[_]: Concurrent: ContextShift: Parallel: Timer](
    blocker: Blocker,
    output: Output
  ): Resource[F, ByteSink[F]] =
    for {
      sink <- initAttributed(blocker, output)
    } yield records => sink(records.map(AttributedData(_, Map.empty)))

  def initAttributed[F[_]: Concurrent: ContextShift: Parallel: Timer](
    blocker: Blocker,
    output: Output
  ): Resource[F, AttributedByteSink[F]] =
    output match {
      case o: Output.Kinesis =>
        o.region.orElse(getRuntimeRegion) match {
          case Some(region) =>
            for {
              producer <- Resource.eval[F, AmazonKinesis](mkProducer(o, region))
            } yield records => writeToKinesis(blocker, o, producer, toKinesisRecords(records))
          case None =>
            Resource.eval(Sync[F].raiseError(new RuntimeException(s"Region not found in the config and in the runtime")))
        }
      case o =>
        Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Output $o is not Kinesis")))
    }

  private def mkProducer[F[_]: Sync](
    config: Output.Kinesis,
    region: String
  ): F[AmazonKinesis] =
    for {
      builder <- Sync[F].delay(AmazonKinesisClientBuilder.standard)
      withEndpoint <- config.customEndpoint match {
                        case Some(endpoint) =>
                          Sync[F].delay(builder.withEndpointConfiguration(new EndpointConfiguration(endpoint.toString, region)))
                        case None =>
                          Sync[F].delay(builder.withRegion(region))
                      }
      kinesis <- Sync[F].delay(withEndpoint.build())
      _ <- streamExists(kinesis, config.streamName)
    } yield kinesis

  private def streamExists[F[_]: Sync](kinesis: AmazonKinesis, stream: String): F[Unit] =
    for {
      described <- Sync[F].delay(kinesis.describeStream(stream))
      status = described.getStreamDescription.getStreamStatus
      exists <- status match {
                  case "ACTIVE" | "UPDATING" =>
                    Sync[F].unit
                  case _ =>
                    Sync[F].raiseError[Unit](new IllegalArgumentException(s"Stream $stream doesn't exist or can't be accessed"))
                }
    } yield exists

  private def getRetryPolicyForErrors[F[_]: Applicative](config: BackoffPolicy): RetryPolicy[F] =
    capDelay[F](config.maxBackoff, fullJitter[F](config.minBackoff))
      .join(limitRetries(config.maxRetries))

  private def getRetryPolicyForThrottling[F[_]: Applicative](config: BackoffPolicy): RetryPolicy[F] =
    capDelay[F](config.maxBackoff, fullJitter[F](config.minBackoff))

  private def writeToKinesis[F[_]: ContextShift: Parallel: Sync: Timer](
    blocker: Blocker,
    config: Output.Kinesis,
    kinesis: AmazonKinesis,
    records: List[PutRecordsRequestEntry]
  ): F[Unit] = {
    val policyForErrors = getRetryPolicyForErrors[F](config.backoffPolicy)
    val policyForThrottling = getRetryPolicyForThrottling[F](config.backoffPolicy)

    def runOnce(ref: Ref[F, List[PutRecordsRequestEntry]]): F[List[PutRecordsRequestEntry]] =
      for {
        records <- ref.get
        failures <- group(records, config.recordLimit, config.byteLimit, getRecordSize)
                      .parTraverse(g => tryWriteToKinesis(blocker, config, kinesis, g, policyForErrors))
        flattened = failures.flatten
        _ <- ref.set(flattened)
      } yield flattened

    for {
      ref <- Ref.of(records)
      _ <- runOnce(ref)
             .retryingOnFailures(
               policy = policyForThrottling,
               wasSuccessful = _.isEmpty,
               onFailure = {
                 case (result, retryDetails) =>
                   Logger[F]
                     .warn(
                       s"${result.size} records failed writing to ${config.streamName} (${retryDetails.retriesSoFar} retries from cats-retry)"
                     )
               }
             )
    } yield ()
  }

  /**
   *  This function takes a list of records and splits it into several lists,
   *  where each list is as big as possible with respecting the record limit and
   *  the size limit.
   */
  private[kinesis] def group[A](
    records: List[A],
    recordLimit: Int,
    sizeLimit: Int,
    getRecordSize: A => Int
  ): List[List[A]] = {
    case class Batch(
      size: Int,
      count: Int,
      records: List[A]
    )

    records
      .foldLeft(List.empty[Batch]) {
        case (acc, record) =>
          val recordSize = getRecordSize(record)
          acc match {
            case head :: tail =>
              if (head.count + 1 > recordLimit || head.size + recordSize > sizeLimit)
                List(Batch(recordSize, 1, List(record))) ++ List(head) ++ tail
              else
                List(Batch(head.size + recordSize, head.count + 1, record :: head.records)) ++ tail
            case Nil =>
              List(Batch(recordSize, 1, List(record)))
          }
      }
      .map(_.records)
  }

  private def getRecordSize(record: PutRecordsRequestEntry) =
    record.getData.array.size + record.getPartitionKey.getBytes.size

  /** Returns a list of the failures, i.e. when throttled */
  private def tryWriteToKinesis[F[_]: ContextShift: Sync: Timer](
    blocker: Blocker,
    config: Output.Kinesis,
    kinesis: AmazonKinesis,
    records: List[PutRecordsRequestEntry],
    retryPolicy: RetryPolicy[F]
  ): F[List[PutRecordsRequestEntry]] =
    Logger[F].debug(s"Writing ${records.size} records to ${config.streamName}") *>
      blocker
        .blockOn(Sync[F].delay(putRecords(kinesis, config.streamName, records)))
        .retryingOnFailuresAndAllErrors(
          policy = retryPolicy,
          wasSuccessful = { result =>
            // It's a success if _any_ record has a status which we want to handle ourselves.
            // This includes if we got throttled by Kinesis.
            result.getFailedRecordCount.toInt === 0 || result.getRecords.asScala
              .flatMap(record => Option(record.getErrorCode))
              .exists(_ === "ProvisionedThroughputExceededException")
          },
          onFailure = {
            case (result, retryDetails) =>
              val codes = result.getRecords.asScala.flatMap(record => Option(record.getErrorCode)).toSet.mkString(",")
              Logger[F]
                .error(
                  s"Writing ${records.size} records to ${config.streamName} errored with codes [$codes] (${retryDetails.retriesSoFar} retries from cats-retry)"
                )
          },
          onError = (exception, retryDetails) =>
            Logger[F]
              .error(exception)(
                s"Writing ${records.size} records to ${config.streamName} errored (${retryDetails.retriesSoFar} retries from cats-retry)"
              )
        )
        .flatMap { putRecordsResult =>
          if (putRecordsResult.getFailedRecordCount.toInt =!= 0) {
            val failurePairs = records.zip(putRecordsResult.getRecords.asScala).filter(_._2.getErrorMessage != null)
            val (failedRecords, failedResults) = failurePairs.unzip
            logErrorsSummary(getErrorsSummary(failedResults)).as(failedRecords)
          } else
            Sync[F].pure(Nil)
        }

  private def toKinesisRecords(records: List[AttributedData[Array[Byte]]]): List[PutRecordsRequestEntry] =
    records.map { r =>
      val partitionKey =
        r.attributes.toList match { // there can be only one attribute : the partition key
          case head :: Nil => head._2
          case _ => UUID.randomUUID().toString
        }
      val data = ByteBuffer.wrap(r.data)
      val prre = new PutRecordsRequestEntry()
      prre.setPartitionKey(partitionKey)
      prre.setData(data)
      prre
    }

  private def putRecords(
    kinesis: AmazonKinesis,
    streamName: String,
    records: List[PutRecordsRequestEntry]
  ): PutRecordsResult = {
    val putRecordsRequest = {
      val prr = new PutRecordsRequest()
      prr.setStreamName(streamName)
      prr.setRecords(records.asJava)
      prr
    }
    kinesis.putRecords(putRecordsRequest)
  }

  private def getErrorsSummary(badResponses: List[PutRecordsResultEntry]): Map[String, (Long, String)] =
    badResponses.foldLeft(Map[String, (Long, String)]())((counts, r) =>
      if (counts.contains(r.getErrorCode))
        counts + (r.getErrorCode -> (counts(r.getErrorCode)._1 + 1 -> r.getErrorMessage))
      else
        counts + (r.getErrorCode -> ((1, r.getErrorMessage)))
    )

  private def logErrorsSummary[F[_]: Sync](errorsSummary: Map[String, (Long, String)]): F[Unit] =
    errorsSummary
      .map {
        case (errorCode, (count, sampleMessage)) =>
          Logger[F].debug(
            s"$count records failed with error code ${errorCode}. Example error message: ${sampleMessage}"
          )
      }
      .toList
      .sequence_

}
