/*
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.pubsub

import java.nio.charset.StandardCharsets

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.Parallel
import cats.implicits._

import cats.effect.kernel.{Async, Resource, Sync}

import com.permutive.pubsub.producer.PubsubProducer
import com.permutive.pubsub.producer.Model.{ProjectId, Topic}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}

import com.snowplowanalytics.snowplow.enrich.common.fs2.{AttributedByteSink, AttributedData, ByteSink}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.Output

object Sink {

  private val maxAttributeBytesLength = 1024

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def init[F[_]: Async: Parallel](
    output: Output
  ): Resource[F, ByteSink[F]] =
    for {
      sink <- initAttributed(output)
    } yield (records: List[Array[Byte]]) => sink(records.map(AttributedData(_, "", Map.empty)))

  def initAttributed[F[_]: Async: Parallel](
    output: Output
  ): Resource[F, AttributedByteSink[F]] =
    output match {
      case o: Output.PubSub =>
        pubsubSink[F, Array[Byte]](o)
      case o =>
        Resource.eval(Sync[F].raiseError(new IllegalArgumentException(s"Output $o is not PubSub")))
    }

  private def pubsubSink[F[_]: Async: Parallel, A: MessageEncoder](
    output: Output.PubSub
  ): Resource[F, List[AttributedData[A]] => F[Unit]] = {
    val config = PubsubProducerConfig[F](
      batchSize = output.maxBatchSize,
      requestByteThreshold = Some(output.maxBatchBytes),
      delayThreshold = output.delayThreshold,
      onFailedTerminate = err => Logger[F].error(err)("PubSub sink termination error"),
      customizePublisher = Some(_.setHeaderProvider(Utils.createPubsubUserAgentHeader(output.gcpUserAgent)))
    )

    GooglePubsubProducer
      .of[F, A](ProjectId(output.project), Topic(output.name), config)
      .map(sinkBatch[F, A])
  }

  private def sinkBatch[F[_]: Async: Parallel, A](producer: PubsubProducer[F, A])(records: List[AttributedData[A]]): F[Unit] =
    records.parTraverse_ { r =>
      val attributes = dropOversizedAttributes(r)
      producer.produce(r.data, attributes)
    }.void

  private def dropOversizedAttributes[A](r: AttributedData[A]): Map[String, String] =
    r.attributes.filter {
      case (_, value) =>
        value.getBytes(StandardCharsets.UTF_8).length <= maxAttributeBytesLength
    }
}
