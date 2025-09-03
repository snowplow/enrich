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

import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.duration._

import cats.implicits._
import cats.data.Validated

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource, Unique}

import org.http4s.client.{Client => Http4sClient}
import org.http4s.dsl.io._

import io.circe.parser
import io.circe.literal._

import fs2.Stream
import fs2.io.readInputStream

import org.http4s.Uri

import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.iglu.core.SelfDescribingData

import com.snowplowanalytics.iglu.client.{IgluCirceClient, Resolver}

import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo}
import com.snowplowanalytics.snowplow.runtime.processing.Coldswap
import com.snowplowanalytics.snowplow.streams.{
  EventProcessingConfig,
  EventProcessor,
  ListOfList,
  Sink,
  Sinkable,
  SourceAndAck,
  TokenedEvents
}

import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.RemoteAdapter
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.IpLookupExecutionContext
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery.SqlExecutionContext
import com.snowplowanalytics.snowplow.enrich.common.enrichments.AtomicFields
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.{HttpClient => CommonHttpClient}

import com.snowplowanalytics.snowplow.enrich.cloudutils.core.HttpBlobClient

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers

case class MockEnvironment(state: Ref[IO, Vector[MockEnvironment.Action]], environment: Environment[IO])

object MockEnvironment {

  sealed trait Action
  object Action {
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    /* Output events */
    case class SentToEnriched(enriched: List[Enriched]) extends Action
    case class SentToFailed(failed: List[Failed]) extends Action
    case class SentToBad(bad: List[Bad]) extends Action
    /* Metrics */
    case class AddedRawCountMetric(count: Int) extends Action
    case class AddedEnrichedCountMetric(count: Int) extends Action
    case class AddedFailedCountMetric(count: Int) extends Action
    case class AddedBadCountMetric(count: Int) extends Action
    case class AddedDroppedCountMetric(count: Int) extends Action
    case class AddedInvalidCountMetric(count: Int) extends Action
    case class SetLatencyMetric(latency: FiniteDuration) extends Action
    case class SetE2ELatencyMetric(e2eLatency: FiniteDuration) extends Action
    /* Health */
    case class BecameUnhealthy(service: RuntimeService) extends Action
    case class BecameHealthy(service: RuntimeService) extends Action
    /* Metadata */
    case class AddedMetadata(aggregates: Metadata.Aggregates) extends Action
  }
  import Action._

  def build(
    inputs: Stream[IO, TokenedEvents],
    enrichmentsConfs: List[EnrichmentConf] = Nil,
    mocks: Mocks = Mocks.default,
    exitOnJsCompileError: Boolean = true,
    decompressionConfig: Config.Decompression
  ): Resource[IO, MockEnvironment] =
    for {
      state <- Resource.eval(Ref[IO].of(Vector.empty[Action]))
      httpClient <- Resource.eval(mockHttpClient)
      blobClients = List(HttpBlobClient.wrapHttp4sClient(httpClient.mock))
      assets = Assets.fromEnrichmentConfs(enrichmentsConfs)
      _ <- Resource.eval(Assets.downloadAssets(assets, blobClients))
      apiEnrichmentClient = CommonHttpClient.fromHttp4sClient[IO](httpClient.mock)
      ipLookupEC <- IpLookupExecutionContext.mk[IO]
      sqlEC <- SqlExecutionContext.mk[IO]
      enrichmentRegistry <- Coldswap.make(
                              Environment.mkEnrichmentRegistry[IO](
                                enrichmentsConfs,
                                apiEnrichmentClient,
                                ipLookupEC,
                                sqlEC,
                                exitOnJsCompileError
                              )
                            )
      _ <- Resource.eval(enrichmentRegistry.opened.use_)
      igluClient <- Resource.eval(IgluCirceClient.fromResolver(Resolver[IO](Nil, None), 0, 40))
    } yield {
      val env = Environment(
        appInfo = appInfo,
        source = testSourceAndAck(inputs, state),
        appHealth = testAppHealth(state),
        enrichedSink = testSink(
          mocks.enrichedSinkResponse,
          parseEnriched,
          (enriched: List[Enriched]) => state.update(_ :+ SentToEnriched(enriched))
        ),
        failedSink = Some(
          testSink(
            mocks.failedSinkResponse,
            parseFailed,
            (failed: List[Failed]) => state.update(_ :+ SentToFailed(failed))
          )
        ),
        badSink = testSink(
          mocks.badSinkResponse,
          parseBad,
          (bad: List[Bad]) => state.update(_ :+ SentToBad(bad))
        ),
        metrics = testMetrics(state),
        cpuParallelism = 2,
        sinkParallelism = 1,
        sinkMaxSize = 1024 * 1024,
        adapterRegistry = testAdapterRegistry,
        assets = assets,
        blobClients = blobClients,
        enrichmentRegistry = enrichmentRegistry,
        igluClient = igluClient,
        httpClient = httpClient.mock,
        registryLookup = SpecHelpers.registryLookup,
        validation = Config.Validation(
          acceptInvalid = false,
          atomicFieldsLimits = AtomicFields.from(SpecHelpers.atomicFieldLimitsDefaults),
          maxJsonDepth = SpecHelpers.DefaultMaxJsonDepth,
          exitOnJsCompileError = true
        ),
        partitionKeyField = None,
        attributeFields = List(EnrichedEvent.atomicFields.find(_.getName === "app_id").get),
        metadata = Some(testMetadataReporter(state)),
        identity = Some(testIdentityApi),
        assetsUpdatePeriod = 3.seconds,
        decompressionConfig = decompressionConfig
      )
      MockEnvironment(state, env)
    }

  sealed trait Response[+A]
  object Response {
    final case class Success[A](value: A) extends Response[A]
    final case class ExceptionThrown(value: Throwable) extends Response[Nothing]
  }

  final case class Mocks(
    enrichedSinkResponse: Response[Unit],
    failedSinkResponse: Response[Unit],
    badSinkResponse: Response[Unit]
  )

  object Mocks {
    val default: Mocks = Mocks(
      enrichedSinkResponse = Response.Success(()),
      failedSinkResponse = Response.Success(()),
      badSinkResponse = Response.Success(())
    )
  }

  val appInfo = new AppInfo {
    def name = "enrich-test"
    def version = "0.0.0"
    def dockerAlias = "snowplow/enrich-test:0.0.0"
    def cloud = "OnPrem"
  }

  private def testSourceAndAck(inputs: Stream[IO, TokenedEvents], state: Ref[IO, Vector[Action]]): SourceAndAck[IO] =
    new SourceAndAck[IO] {
      def stream(config: EventProcessingConfig[IO], processor: EventProcessor[IO]): Stream[IO, Nothing] =
        inputs
          .through(processor)
          .chunks
          .evalMap { tokens =>
            state.update(_ :+ Checkpointed(tokens.toList))
          }
          .drain

      override def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO.pure(SourceAndAck.Healthy)

      def currentStreamLatency: IO[Option[FiniteDuration]] =
        IO.pure(None)
    }

  private def testAppHealth(ref: Ref[IO, Vector[Action]]): AppHealth.Interface[IO, String, RuntimeService] =
    new AppHealth.Interface[IO, String, RuntimeService] {
      def beHealthyForSetup: IO[Unit] =
        IO.unit
      def beUnhealthyForSetup(alert: String): IO[Unit] =
        IO.unit
      def beHealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        ref.update(_ :+ BecameHealthy(service))
      def beUnhealthyForRuntimeService(service: RuntimeService): IO[Unit] =
        ref.update(_ :+ BecameUnhealthy(service))
    }

  private def testSink[A](
    mockedResponse: Response[Unit],
    parse: Sinkable => IO[A],
    updateState: List[A] => IO[Unit]
  ): Sink[IO] =
    new Sink[IO] {
      override def sink(batch: ListOfList[Sinkable]): IO[Unit] =
        mockedResponse match {
          case Response.Success(_) =>
            batch.asIterable.toList.traverse(parse).flatMap(updateState)
          case Response.ExceptionThrown(value) =>
            IO.raiseError(value)
        }

      override def isHealthy: IO[Boolean] = IO.pure(true)
    }

  private def testMetrics(ref: Ref[IO, Vector[Action]]): Metrics[IO] =
    new Metrics[IO] {
      def addRaw(count: Int): IO[Unit] =
        ref.update(_ :+ AddedRawCountMetric(count))

      def addEnriched(count: Int): IO[Unit] =
        ref.update(_ :+ AddedEnrichedCountMetric(count))

      def addFailed(count: Int): IO[Unit] =
        ref.update(_ :+ AddedFailedCountMetric(count))

      def addBad(count: Int): IO[Unit] =
        ref.update(_ :+ AddedBadCountMetric(count))

      def addDropped(count: Int): IO[Unit] =
        ref.update(_ :+ AddedDroppedCountMetric(count))

      def addInvalid(count: Int): IO[Unit] =
        ref.update(_ :+ AddedInvalidCountMetric(count))

      def setLatency(latency: FiniteDuration): IO[Unit] =
        ref.update(_ :+ SetLatencyMetric(latency))

      def setE2ELatency(e2eLatency: FiniteDuration): IO[Unit] =
        ref.update(_ :+ SetE2ELatencyMetric(e2eLatency))

      def report: Stream[IO, Nothing] = Stream.never[IO]
    }

  private val testAdapterRegistry = new AdapterRegistry(Map.empty[(String, String), RemoteAdapter[IO]], SpecHelpers.adaptersSchemas)

  case class Enriched(
    event: Event,
    partitionKey: Option[String],
    attributes: Map[String, String]
  )

  private def parseEnriched(sinkable: Sinkable): IO[Enriched] =
    for {
      parsed <- IO(Event.parse(new String(sinkable.bytes, UTF_8)))
      enriched <- parsed match {
                    case Validated.Valid(e) => IO.pure(e)
                    case Validated.Invalid(err) => IO.raiseError(new RuntimeException(s"Can't parse enriched event: $err"))
                  }
    } yield Enriched(enriched, sinkable.partitionKey, sinkable.attributes)

  case class Failed(
    event: Event,
    partitionKey: Option[String],
    attributes: Map[String, String]
  )

  private def parseFailed(sinkable: Sinkable): IO[Failed] =
    parseEnriched(sinkable).map(e => Failed(e.event, sinkable.partitionKey, sinkable.attributes))

  case class Bad(
    bad: BadRow,
    partitionKey: Option[String],
    attributes: Map[String, String]
  )

  private def parseBad(sinkable: Sinkable): IO[Bad] =
    for {
      str <- IO(new String(sinkable.bytes, UTF_8))
      json <- parser.parse(str) match {
                case Right(j) => IO.pure(j)
                case Left(err) => IO.raiseError(new RuntimeException(s"Can't parse bad row: $err"))
              }
      bad <- json.as[SelfDescribingData[BadRow]] match {
               case Right(sdd) => IO.pure(sdd.data)
               case Left(err) => IO.raiseError(new RuntimeException(s"Can't decode bad row: $err"))
             }
    } yield Bad(bad, sinkable.partitionKey, sinkable.attributes)

  private def testMetadataReporter(ref: Ref[IO, Vector[Action]]): MetadataReporter[IO] =
    new MetadataReporter[IO] {
      def add(aggregates: Metadata.Aggregates): IO[Unit] =
        ref.update(_ :+ AddedMetadata(aggregates))
    }

  val mockHttpServerUri = "http://mockserver"

  def mockHttpClient: IO[MockHttpClient] =
    Ref[IO].of(Map.empty[Uri, Int]).map(new MockHttpClient(_))

  class MockHttpClient(val requestsCounts: Ref[IO, Map[Uri, Int]]) {

    def mock: Http4sClient[IO] =
      Http4sClient { req =>
        Resource.eval {
          for {
            counts <- requestsCounts.updateAndGet(_ |+| Map(req.uri -> 1))
            count = counts(req.uri)
            response <- if (req.uri == Uri.unsafeFromString(s"$mockHttpServerUri/maxmind/GeoIP2-City.mmdb")) {
                          val filename = count match {
                            case 1 => "/GeoIP2-City.mmdb"
                            case _ => "/GeoIP2-City-updated.mmdb"
                          }
                          val is = IO(getClass.getResourceAsStream(filename))
                          Ok(readInputStream[IO](is, 256).compile.to(Array))
                        } else if (req.uri.toString.startsWith(s"$mockHttpServerUri/enrichment/api")) {
                          val quantity = req.uri.path.segments.last.toString.toInt
                          Ok(json"""{"record":{"sku":"pedals","quantity":$quantity}}""".noSpaces)
                        } else
                          NotFound("Endpoint not implemented")
          } yield response
        }
      }
  }

  private def testIdentityApi: Identity.Api[IO] =
    new Identity.Api[IO] {
      def post(inputs: List[Identity.BatchIdentifiers]): IO[List[Identity.BatchIdentity]] =
        IO.realTimeInstant.map { now =>
          inputs.map { batchIdentifiers =>
            Identity.BatchIdentity(
              merged = Nil,
              createdAt = now.toString,
              eventId = batchIdentifiers.eventId,
              snowplowId = batchIdentifiers.eventId
            )
          }
        }

      def concurrency: Int = 1
    }

}
