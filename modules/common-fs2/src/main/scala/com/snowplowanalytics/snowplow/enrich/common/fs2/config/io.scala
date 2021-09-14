/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.fs2.config

import java.nio.file.{InvalidPathException, Path, Paths}
import java.time.Instant
import java.net.URI

import cats.syntax.either._

import scala.concurrent.duration.{Duration, FiniteDuration}

import _root_.io.circe.{Decoder, Encoder, Json}
import _root_.io.circe.generic.extras.semiauto._
import _root_.io.circe.config.syntax._

object io {

  implicit val javaPathDecoder: Decoder[Path] =
    Decoder[String].emap { s =>
      Either.catchOnly[InvalidPathException](Paths.get(s)).leftMap(_.getMessage)
    }
  implicit val javaPathEncoder: Encoder[Path] =
    Encoder[String].contramap(_.toString)

  implicit val javaUriDecoder: Decoder[URI] =
    Decoder[String].emap { s =>
      Either.catchOnly[IllegalArgumentException](URI.create(s)).leftMap(err => s"error while parsing URI $s: ${err.getMessage}")
    }

  implicit val javaUriEncoder: Encoder[URI] =
    Encoder.encodeString.contramap[URI](_.toString)

  import ConfigFile.finiteDurationEncoder

  /** Source of raw collector data (only PubSub supported atm) */
  sealed trait Input

  object Input {

    case class PubSub private (
      subscription: String,
      parallelPullCount: Option[Int],
      maxQueueSize: Option[Int]
    ) extends Input {
      val (project, name) =
        subscription.split("/").toList match {
          case List("projects", project, "subscriptions", name) =>
            (project, name)
          case _ =>
            throw new IllegalArgumentException(s"Cannot construct Input.PubSub from $subscription")
        }
    }
    case class FileSystem(dir: Path) extends Input
    case class Kinesis private (
      appName: String,
      streamName: String,
      region: Option[String],
      initialPosition: Kinesis.InitPosition,
      retrievalMode: Kinesis.Retrieval,
      customEndpoint: Option[URI],
      dynamodbCustomEndpoint: Option[URI]
    ) extends Input

    object Kinesis {
      sealed trait InitPosition
      object InitPosition {
        case object Latest extends InitPosition
        case object TrimHorizon extends InitPosition
        case class AtTimestamp(timestamp: Instant) extends InitPosition

        implicit val initPositionDecoder: Decoder[InitPosition] =
          Decoder.decodeJson.emap { json =>
            json.asString.map(_.toUpperCase) match {
              case Some("TRIM_HORIZON") => TrimHorizon.asRight
              case Some("LATEST") => Latest.asRight
              case Some(other) =>
                s"Initial position $other is not supported. Possible string values are TRIM_HORIZON and LATEST. AT_TIMESTAMP must be provided as object".asLeft
              case None =>
                json.asObject match {
                  case Some(obj) =>
                    val map = obj.toMap
                    if (map.get("type") == Some(Json.fromString("AT_TIMESTAMP")))
                      map.get("timestamp") match {
                        case Some(timestampStr) =>
                          timestampStr
                            .as[Instant]
                            .map(instant => AtTimestamp(instant))
                            .leftMap(err => s"couldn't not parse timestamp. Error: ${err.getMessage}")
                        case None =>
                          s"${json.toString} does not contain timestamp field".asLeft
                      }
                    else
                      s"type is not AT_TIMESTAMP in ${json.toString}".asLeft
                  case None =>
                    s"${json.toString} is neither string nor object".asLeft
                }
            }
          }
        implicit val initPositionEncoder: Encoder[InitPosition] = deriveConfiguredEncoder[InitPosition]
      }

      sealed trait Retrieval
      object Retrieval {
        case class Polling(maxRecords: Int) extends Retrieval
        case object FanOut extends Retrieval

        implicit val retrievalDecoder: Decoder[Retrieval] =
          Decoder.decodeJson.emap { json =>
            json.asString.map(_.toUpperCase) match {
              case Some("FanOut") => FanOut.asRight
              case Some(other) =>
                s"Retrieval mode $other is not supported. Only possible string value is FanOut. Polling must be provided as object".asLeft
              case None =>
                json.asObject match {
                  case Some(obj) =>
                    val map = obj.toMap
                    if (map.get("type") == Some(Json.fromString("Polling")))
                      map.get("maxRecords") match {
                        case Some(nb) =>
                          nb.as[Int].map(max => Polling(max)).leftMap(_ => s"couldn't not parse $nb as Int")
                        case None =>
                          s"${json.toString} does not contain maxRecords".asLeft
                      }
                    else
                      s"type is not Polling in ${json.toString}".asLeft
                  case None =>
                    s"${json.toString} is neither string nor object".asLeft
                }
            }
          }
        implicit val retrievalEncoder: Encoder[Retrieval] = deriveConfiguredEncoder[Retrieval]
      }

      implicit val kinesisDecoder: Decoder[Kinesis] = deriveConfiguredDecoder[Kinesis]
      implicit val kinesisEncoder: Encoder[Kinesis] = deriveConfiguredEncoder[Kinesis]
    }

    implicit val inputDecoder: Decoder[Input] =
      deriveConfiguredDecoder[Input]
        .emap {
          case s @ PubSub(sub, _, _) =>
            sub.split("/").toList match {
              case List("projects", _, "subscriptions", _) =>
                s.asRight
              case _ =>
                s"Subscription must conform projects/project-name/subscriptions/subscription-name format, $s given".asLeft
            }
          case other => other.asRight
        }
        .emap {
          case PubSub(_, Some(p), _) if p < 0 =>
            "PubSub parallelPullCount must be > 0".asLeft
          case PubSub(_, _, Some(m)) if m < 0 =>
            "PubSub maxQueueSize must be > 0".asLeft
          case other =>
            other.asRight
        }
    implicit val inputEncoder: Encoder[Input] =
      deriveConfiguredEncoder[Input]
  }

  case class Outputs(good: Output, pii: Option[Output], bad: Output)
  object Outputs {
    implicit val outputsDecoder: Decoder[Outputs] = deriveConfiguredDecoder[Outputs]
    implicit val outputsEncoder: Encoder[Outputs] = deriveConfiguredEncoder[Outputs]
  }

  sealed trait Output

  object Output {
    case class PubSub private (
      topic: String,
      attributes: Option[Set[String]],
      delayThreshold: Option[FiniteDuration],
      maxBatchSize: Option[Long],
      maxBatchBytes: Option[Long],
      numCallbackExecutors: Option[Int]
    ) extends Output {
      val (project, name) =
        topic.split("/").toList match {
          case List("projects", project, "topics", name) =>
            (project, name)
          case _ =>
            throw new IllegalArgumentException(s"Cannot construct Output.PubSub from $topic")
        }
    }
    case class FileSystem(file: Path, maxBytes: Option[Long]) extends Output
    case class Kinesis(
      streamName: String,
      region: Option[String],
      partitionKey: Option[String],
      backoffPolicy: BackoffPolicy,
      maxBufferedTime: FiniteDuration,
      collection: Collection,
      aggregation: Option[Aggregation]
    ) extends Output

    case class BackoffPolicy(minBackoff: FiniteDuration, maxBackoff: FiniteDuration)
    object BackoffPolicy {
      implicit def backoffPolicyDecoder: Decoder[BackoffPolicy] =
        deriveConfiguredDecoder[BackoffPolicy]
      implicit def backoffPolicyEncoder: Encoder[BackoffPolicy] =
        deriveConfiguredEncoder[BackoffPolicy]
    }

    case class Collection(maxCount: Long, maxSize: Long)
    object Collection {
      implicit def collectionDecoder: Decoder[Collection] =
        deriveConfiguredDecoder[Collection]
      implicit def collectionEncoder: Encoder[Collection] =
        deriveConfiguredEncoder[Collection]
    }

    case class Aggregation(maxCount: Long, maxSize: Long)
    object Aggregation {
      implicit def aggregationDecoder: Decoder[Aggregation] =
        deriveConfiguredDecoder[Aggregation]
      implicit def aggregationEncoder: Encoder[Aggregation] =
        deriveConfiguredEncoder[Aggregation]
    }

    implicit val outputDecoder: Decoder[Output] =
      deriveConfiguredDecoder[Output]
        .emap {
          case s @ PubSub(top, _, _, _, _, _) =>
            top.split("/").toList match {
              case List("projects", _, "topics", _) =>
                s.asRight
              case _ =>
                s"Topic must conform projects/project-name/topics/topic-name format, $top given".asLeft
            }
          case Kinesis(s, r, _, _, _, _, _) if(s.isEmpty && r.nonEmpty) =>
            "streamName needs to be set".asLeft
          case other => other.asRight
        }
        .emap {
          case PubSub(_, _, Some(d), _, _, _) if d < Duration.Zero =>
            "PubSub delay threshold cannot be less than 0".asLeft
          case PubSub(_, _, _, Some(m), _, _) if m < 0 =>
            "PubSub max batch size cannot be less than 0".asLeft
          case PubSub(_, _, _, _, Some(m), _) if m < 0 =>
            "PubSub max batch bytes cannot be less than 0".asLeft
          case PubSub(_, _, _, _, _, Some(m)) if m < 0 =>
            "PubSub callback executors cannot be less than 0".asLeft
          case other =>
            other.asRight
        }

    implicit val outputEncoder: Encoder[Output] =
      deriveConfiguredEncoder[Output]
  }

  final case class Concurrency(enrich: Long, sink: Int)

  object Concurrency {
    implicit val concurrencyDecoder: Decoder[Concurrency] =
      deriveConfiguredDecoder[Concurrency]
    implicit val concurrencyEncoder: Encoder[Concurrency] =
      deriveConfiguredEncoder[Concurrency]
  }

  final case class MetricsReporters(
    statsd: Option[MetricsReporters.StatsD],
    stdout: Option[MetricsReporters.Stdout],
    cloudwatch: Option[Boolean]
  )

  object MetricsReporters {
    final case class Stdout(period: FiniteDuration, prefix: Option[String])
    final case class StatsD(
      hostname: String,
      port: Int,
      tags: Map[String, String],
      period: FiniteDuration,
      prefix: Option[String]
    )

    implicit val stdoutDecoder: Decoder[Stdout] =
      deriveConfiguredDecoder[Stdout].emap { stdout =>
        if (stdout.period < Duration.Zero)
          "metrics report period in config file cannot be less than 0".asLeft
        else
          stdout.asRight
      }

    implicit val statsDecoder: Decoder[StatsD] =
      deriveConfiguredDecoder[StatsD].emap { statsd =>
        if (statsd.period < Duration.Zero)
          "metrics report period in config file cannot be less than 0".asLeft
        else
          statsd.asRight
      }

    implicit val metricsReportersDecoder: Decoder[MetricsReporters] =
      deriveConfiguredDecoder[MetricsReporters]

    implicit val stdoutEncoder: Encoder[Stdout] =
      deriveConfiguredEncoder[Stdout]

    implicit val statsdEncoder: Encoder[StatsD] =
      deriveConfiguredEncoder[StatsD]

    implicit val metricsReportersEncoder: Encoder[MetricsReporters] =
      deriveConfiguredEncoder[MetricsReporters]

    def normalizeMetric(prefix: Option[String], metric: String): String =
      s"${prefix.getOrElse(DefaultPrefix).stripSuffix(".")}.$metric".stripPrefix(".")

    val DefaultPrefix = "snowplow.enrich"
  }

  case class Monitoring(sentry: Option[Sentry], metrics: Option[MetricsReporters])

  object Monitoring {
    implicit val monitoringDecoder: Decoder[Monitoring] =
      deriveConfiguredDecoder[Monitoring]
    implicit val monitoringEncoder: Encoder[Monitoring] =
      deriveConfiguredEncoder[Monitoring]
  }

}
