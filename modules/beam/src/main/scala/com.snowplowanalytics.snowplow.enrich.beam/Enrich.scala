/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.beam

import scala.collection.JavaConverters._

import io.circe.Json

import io.sentry.Sentry

import cats.Id
import cats.data.Validated
import cats.implicits._

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}
import com.snowplowanalytics.snowplow.enrich.common.EtlPipeline
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.loaders.ThriftLoader
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.ConversionUtils

import com.spotify.scio._
import com.spotify.scio.pubsub._
import com.spotify.scio.coders.Coder
import com.spotify.scio.pubsub.PubSubAdmin
import com.spotify.scio.values.{DistCache, SCollection}

import org.apache.beam.sdk.io.gcp.pubsub.{PubsubMessage, PubsubOptions}
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import java.net.URI

import com.snowplowanalytics.snowplow.enrich.beam.config._
import com.snowplowanalytics.snowplow.enrich.beam.singleton._
import com.snowplowanalytics.snowplow.enrich.beam.utils._

/** Enrich job using the Beam API through SCIO */
object Enrich {

  private val logger = LoggerFactory.getLogger(this.getClass)

  implicit val badRowScioCodec: Coder[BadRow] = Coder.kryo[BadRow]

  // the maximum record size in Google PubSub is 10Mb
  // the maximum PubSubIO size is 7Mb to overcome base64-encoding
  private val MaxRecordSize = 6900000
  private val MetricsNamespace = "snowplow"

  val enrichedEventSizeDistribution =
    ScioMetrics.distribution(MetricsNamespace, "enriched_event_size_bytes")
  val timeToEnrichDistribution =
    ScioMetrics.distribution(MetricsNamespace, "time_to_enrich_ms")

  val processor = Processor(generated.BuildInfo.name, generated.BuildInfo.version)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val config = for {
      conf <- EnrichConfig(args)
      _ = sc.setJobName(conf.jobName)
      _ <- checkTopicExists(sc, conf.enriched)
      _ <- checkTopicExists(sc, conf.bad)
      _ <- conf.pii.map(checkTopicExists(sc, _)).getOrElse(().asRight)
    } yield conf

    config match {
      case Left(e) =>
        System.err.println(e)
        System.exit(1)
      case Right(c) =>
        run(sc, c)
        sc.run()
        ()
    }
  }

  def run(sc: ScioContext, config: EnrichConfig): Unit = {
    if (config.labels.nonEmpty)
      sc.optionsAs[DataflowPipelineOptions].setLabels(config.labels.asJava)

    val cachedFiles: DistCache[List[Either[String, String]]] =
      buildDistCache(sc, config.enrichmentConfs)

    val in = sc.read[PubsubMessage](PubsubIO.pubsub[PubsubMessage](config.raw))(PubsubIO.ReadParam(PubsubIO.Subscription))

    val raw: SCollection[Array[Byte]] = in.map(_.getPayload)

    val enriched: SCollection[Validated[BadRow, EnrichedEvent]] =
      enrichEvents(raw, config.resolver, config.enrichmentConfs, cachedFiles, config.sentryDSN, config.metrics)

    val (failures, successes): (SCollection[BadRow], SCollection[EnrichedEvent]) = {
      val enrichedPartitioned = enriched.withName("split-enriched-good-bad").partition(_.isValid)

      val successes = enrichedPartitioned._1
        .withName("get-enriched-good")
        .collect { case Validated.Valid(enriched) => enriched }

      val failures = enrichedPartitioned._2
        .withName("get-enriched-bad")
        .collect[BadRow] { case Validated.Invalid(row) => row }
        .withName("resize-bad-rows")
        .map(resizeBadRow(_, MaxRecordSize, processor))

      (failures, successes)
    }

    val (tooBigSuccesses, properlySizedSuccesses) = formatEnrichedEvents(config.metrics, successes)
    properlySizedSuccesses
      .withName("get-properly-sized-enriched")
      .map(_._1)
      .withName("write-enriched-to-pubsub")
      .write(PubsubIO.string(config.enriched))(PubsubIO.WriteParam())

    val resizedEnriched: SCollection[BadRow] = tooBigSuccesses
      .withName("resize-oversized-enriched")
      .map {
        case (event, size) => resizeEnrichedEvent(event, size, MaxRecordSize, processor)
      }

    val piis = generatePiiEvents(successes, config.enrichmentConfs)

    val allResized: SCollection[BadRow] = (piis, config.pii) match {
      case (Some((tooBigPiis, properlySizedPiis)), Some(topicPii)) =>
        properlySizedPiis
          .withName("get-properly-sized-pii")
          .map(_._1)
          .withName("write-pii-to-pubsub")
          .write(PubsubIO.string(topicPii))(PubsubIO.WriteParam())

        tooBigPiis
          .withName("resize-oversized-pii")
          .map {
            case (event, size) =>
              resizeEnrichedEvent(event, size, MaxRecordSize, processor)
          }
          .withName("join-bad-resized")
          .union(resizedEnriched)

      case _ => resizedEnriched
    }

    val allBadRows: SCollection[BadRow] =
      allResized
        .withName("join-bad-all")
        .union(failures)

    allBadRows
      .withName("serialize-bad-rows")
      .map(_.compact)
      .withName("write-bad-rows-to-pubsub")
      .write(PubsubIO.string(config.bad))(PubsubIO.WriteParam())

    ()
  }

  /**
   * Turns a collection of byte arrays into a collection of either bad rows of enriched events.
   * @param raw collection of events
   * @param resolver Json representing the iglu resolver
   * @param enrichmentConfs list of enabled enrichment configuration
   * @param cachedFiles list of files to cache
   */
  private def enrichEvents(
    raw: SCollection[Array[Byte]],
    resolver: Json,
    enrichmentConfs: List[EnrichmentConf],
    cachedFiles: DistCache[List[Either[String, String]]],
    sentryDSN: Option[URI],
    metrics: Boolean
  ): SCollection[Validated[BadRow, EnrichedEvent]] =
    raw
      .withName("enrich")
      .map { rawEvent =>
        cachedFiles()
        val (enriched, time) = timeMs {
          enrich(
            rawEvent,
            EnrichmentRegistrySingleton.get(enrichmentConfs),
            ClientSingleton.get(resolver),
            sentryDSN
          )
        }
        if (metrics) timeToEnrichDistribution.update(time) else ()
        enriched
      }
      .withName("flatten-enriched")
      .flatten

  /**
   * Turns successfully enriched events into TSV partitioned by whether or no they exceed the
   * maximum size.
   * @param enriched collection of events that went through the enrichment phase
   * @return a collection of properly-sized enriched events and another of oversized ones
   */
  private def formatEnrichedEvents(
    metrics: Boolean,
    enriched: SCollection[EnrichedEvent]
  ): (SCollection[(String, Int)], SCollection[(String, Int)]) =
    enriched
      .withName("format-enriched")
      .map { enrichedEvent =>
        if (metrics)
          getEnrichedEventMetrics(enrichedEvent).foreach(metric => ScioMetrics.counter(MetricsNamespace, metric).inc())
        else ()
        val formattedEnrichedEvent = ConversionUtils.tabSeparatedEnrichedEvent(enrichedEvent)
        val size = getSize(formattedEnrichedEvent)
        if (metrics) enrichedEventSizeDistribution.update(size.toLong) else ()
        (formattedEnrichedEvent, size)
      }
      .withName("split-oversized")
      .partition(_._2 >= MaxRecordSize)

  /**
   * Generates PII transformation events depending on the configuration of the PII enrichment.
   * @param enriched collection of events that went through the enrichment phase
   * @param confs list of enrichment configuration
   * @return a collection of properly-sized enriched events and another of oversized ones wrapped
   * in an option depending on whether the PII enrichment is configured to emit PII transformation
   * events
   */
  private def generatePiiEvents(
    enriched: SCollection[EnrichedEvent],
    confs: List[EnrichmentConf]
  ): Option[(SCollection[(String, Int)], SCollection[(String, Int)])] =
    if (emitPii(confs)) {
      val (tooBigPiis, properlySizedPiis) = enriched
        .withName("generate-pii-events")
        .map { enrichedEvent =>
          ConversionUtils
            .getPiiEvent(processor, enrichedEvent)
            .map(ConversionUtils.tabSeparatedEnrichedEvent)
            .map(formatted => (formatted, getSize(formatted)))
        }
        .withName("flatten-pii-events")
        .flatten
        .withName("split-oversized-pii")
        .partition(_._2 >= MaxRecordSize)
      Some((tooBigPiis, properlySizedPiis))
    } else
      None

  /**
   * Enrich a collector payload into a list of [[EnrichedEvent]].
   * @param data serialized collector payload
   * @return a list of either [[EnrichedEvent]] or [[BadRow]]
   */
  private def enrich(
    data: Array[Byte],
    enrichmentRegistry: EnrichmentRegistry[Id],
    client: Client[Id, Json],
    sentryDSN: Option[URI]
  ): List[Validated[BadRow, EnrichedEvent]] = {
    val collectorPayload = ThriftLoader.toCollectorPayload(data, processor)
    Either.catchNonFatal(
      EtlPipeline.processEvents(
        new AdapterRegistry,
        enrichmentRegistry,
        client,
        processor,
        new DateTime(System.currentTimeMillis),
        collectorPayload
      )
    ) match {
      case Left(throwable) =>
        logger.error(
          s"Problem occured while processing CollectorPayload [$collectorPayload]",
          throwable
        )
        sentryDSN.foreach { dsn =>
          System.setProperty("sentry.dsn", dsn.toString())
          Sentry.capture(throwable)
        }
        Nil
      case Right(events) => events
    }
  }

  /**
   * Builds a Scio's [[DistCache]] which downloads the needed files and create the necessary
   * symlinks.
   * @param sc Scio context
   * @param enrichmentConfs list of enrichment configurations
   * @return a properly build [[DistCache]]
   */
  private def buildDistCache(sc: ScioContext, enrichmentConfs: List[EnrichmentConf]): DistCache[List[Either[String, String]]] = {
    val filesToCache: List[(String, String)] = enrichmentConfs
      .flatMap(_.filesToCache)
      .map { case (uri, sl) => (uri.toString, sl) }
    sc.distCache(filesToCache.map(_._1)) { files =>
      val symLinks = files.toList
        .zip(filesToCache.map(_._2))
        .map { case (file, symLink) => createSymLink(file, symLink) }
      symLinks.zip(files).foreach {
        case (Right(p), file) => logger.info(s"File $file cached at $p")
        case (Left(e), file) => logger.warn(s"File $file could not be cached: $e")
      }
      symLinks.map(_.map(_.toString))
    }
  }

  /**
   * Checks a PubSub topic exists before launching the job.
   * @param sc Scio Context
   * @param topicName name of the topic to check for existence, projects/{project}/topics/{topic}
   * @return Right if it exists, left otherwise
   */
  private def checkTopicExists(sc: ScioContext, topicName: String): Either[String, Unit] =
    if (sc.isTest)
      ().asRight
    else
      PubSubAdmin.topic(sc.options.as(classOf[PubsubOptions]), topicName) match {
        case scala.util.Success(_) => ().asRight
        case scala.util.Failure(e) =>
          s"Output topic $topicName couldn't be retrieved: ${e.getMessage}".asLeft
      }
}
