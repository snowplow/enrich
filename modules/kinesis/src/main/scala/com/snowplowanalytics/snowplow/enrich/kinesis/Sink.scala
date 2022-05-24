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

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import retry.syntax.all._
import retry.RetryPolicies._
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration

import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}

import com.snowplowanalytics.snowplow.enrich.common.fs2.{AttributedByteSink, AttributedData, ByteSink}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Output

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
              retryPolicy = getRetryPolicy[F](o.backoffPolicy)
            } yield records =>
              records
                .grouped(o.recordLimit)
                .toList
                .parTraverse_(g => writeToKinesis(blocker, o, producer, toKinesisRecords(g), retryPolicy))
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

  private def getRetryPolicy[F[_]: Applicative](config: Output.BackoffPolicy): RetryPolicy[F] =
    capDelay[F](config.maxBackoff, fullJitter[F](config.minBackoff))
      .join(limitRetries(config.maxRetries))

  private def writeToKinesis[F[_]: ContextShift: Sync: Timer](
    blocker: Blocker,
    config: Output.Kinesis,
    kinesis: AmazonKinesis,
    records: List[KinesisRecord],
    retryPolicy: RetryPolicy[F],
    retryStatus: RetryStatus = RetryStatus.NoRetriesYet
  ): F[Unit] = {
    val withoutRetry = blocker.blockOn(Sync[F].delay(putRecords(kinesis, config.streamName, records)))

    val withRetry =
      withoutRetry.retryingOnAllErrors(
        policy = retryPolicy,
        onError = (exception, retryDetails) =>
          Logger[F]
            .error(exception)(
              s"Writing ${records.size} records to ${config.streamName} errored (${retryDetails.retriesSoFar} retries from cats-retry)"
            )
      )

    for {
      _ <- retryStatus match {
             case RetryStatus.NoRetriesYet =>
               Logger[F].debug(s"Writing ${records.size} records to ${config.streamName}")
             case retry =>
               Logger[F].debug(
                 s"Waiting for ${retry.cumulativeDelay} and retrying to write ${records.size} records to ${config.streamName}"
               ) *>
                 Timer[F].sleep(retry.cumulativeDelay)
           }
      putRecordsResult <- withRetry
      failuresRetried <- if (putRecordsResult.getFailedRecordCount != 0) {
                           val failurePairs = records.zip(putRecordsResult.getRecords.asScala).filter(_._2.getErrorMessage != null)
                           val (failedRecords, failedResults) = failurePairs.unzip
                           val logging = logErrorsSummary(getErrorsSummary(failedResults))
                           val maybeRetrying =
                             for {
                               nextRetry <- retryPolicy.decideNextRetry(retryStatus)
                               maybeRetried <- nextRetry match {
                                                 case PolicyDecision.DelayAndRetry(delay) =>
                                                   writeToKinesis(
                                                     blocker,
                                                     config,
                                                     kinesis,
                                                     failedRecords,
                                                     retryPolicy,
                                                     retryStatus.addRetry(delay)
                                                   )
                                                 case PolicyDecision.GiveUp =>
                                                   Sync[F].raiseError[Unit](
                                                     new RuntimeException(
                                                       s"Maximum number of retries reached for ${failedRecords.size} records"
                                                     )
                                                   )
                                               }
                             } yield maybeRetried
                           logging *> maybeRetrying
                         } else
                           Sync[F].unit
    } yield failuresRetried
  }

  private def toKinesisRecords(records: List[AttributedData[Array[Byte]]]): List[KinesisRecord] =
    records.map { r =>
      val partitionKey =
        r.attributes.toList match { // there can be only one attribute : the partition key
          case head :: Nil => head._2
          case _ => UUID.randomUUID().toString
        }
      val data = ByteBuffer.wrap(r.data)
      KinesisRecord(data, partitionKey)
    }

  private def putRecords(
    kinesis: AmazonKinesis,
    streamName: String,
    records: List[KinesisRecord]
  ): PutRecordsResult = {
    val putRecordsRequest = {
      val prr = new PutRecordsRequest()
      prr.setStreamName(streamName)
      val putRecordsRequestEntryList = records.map { r =>
        val prre = new PutRecordsRequestEntry()
        prre.setPartitionKey(r.partitionKey)
        prre.setData(r.data)
        prre
      }
      prr.setRecords(putRecordsRequestEntryList.asJava)
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
          Logger[F].error(
            s"$count records failed with error code ${errorCode}. Example error message: ${sampleMessage}"
          )
      }
      .toList
      .sequence_

  private case class KinesisRecord(data: ByteBuffer, partitionKey: String)
}
