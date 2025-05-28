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
package com.snowplowanalytics.snowplow.enrich.streams.common

import java.lang.reflect.Field

import scala.concurrent.duration._

import cats.implicits._

import cats.effect.{Async, Resource, Sync}

import org.http4s.client.Client

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import io.sentry.Sentry

import com.snowplowanalytics.snowplow.badrows.{Processor => BadRowProcessor}

import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, RegistryLookup}
import com.snowplowanalytics.iglu.client.IgluCirceClient

import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo, HealthProbe}
import com.snowplowanalytics.snowplow.runtime.HttpClient.{Config => HttpClientConfig}

import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.ApiRequestConf
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.IpLookupExecutionContext
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery.SqlExecutionContext
import com.snowplowanalytics.snowplow.enrich.common.utils.{HttpClient => CommonHttpClient}

import com.snowplowanalytics.snowplow.enrich.cloudutils.core.BlobClient
import com.snowplowanalytics.snowplow.enrich.cloudutils.core.HttpBlobClient

/**
 * Resources and runtime-derived configuration needed for processing events
 *
 * @param cpuParallelism
 *   The processing Pipe involves several steps, some of which are cpu-intensive. We run
 *   cpu-intensive steps in parallel, so that on big instances we can take advantage of all cores.
 *   For each of those cpu-intensive steps, `cpuParallelism` controls the parallelism of that step.
 */
case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  appHealth: AppHealth.Interface[F, String, RuntimeService],
  enrichedSink: Sink[F],
  failedSink: Option[Sink[F]],
  badSink: Sink[F],
  metrics: Metrics[F],
  cpuParallelism: Int,
  sinkMaxSize: Int,
  adapterRegistry: AdapterRegistry[F],
  enrichmentRegistry: EnrichmentRegistry[F],
  igluClient: IgluCirceClient[F],
  httpClient: Client[F],
  registryLookup: RegistryLookup[F],
  validation: Config.Validation,
  partitionKeyField: Option[Field],
  attributeFields: List[Field],
  metadata: Option[MetadataReporter[F]]
) {
  def badRowProcessor = BadRowProcessor(appInfo.name, appInfo.version)
}

object Environment {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def fromConfig[F[_]: Async, SourceConfig, SinkConfig, BlobClientsConfig](
    config: Config.Full[SourceConfig, SinkConfig, BlobClientsConfig],
    appInfo: AppInfo,
    toSource: SourceConfig => F[SourceAndAck[F]],
    toSink: SinkConfig => Resource[F, Sink[F]],
    toBlobClients: BlobClientsConfig => List[BlobClient[F]]
  ): Resource[F, Environment[F]] =
    for {
      _ <- enableSentry[F](appInfo, config.main.monitoring.sentry)
      sourceAndAck <- Resource.eval(toSource(config.main.input))
      sourceReporter = sourceAndAck.isHealthy(config.main.monitoring.healthProbe.unhealthyLatency).map(_.showIfUnhealthy)
      appHealth <- Resource.eval(AppHealth.init[F, String, RuntimeService](List(sourceReporter)))
      _ <- HealthProbe.resource(config.main.monitoring.healthProbe.port, appHealth)
      enrichedSink <- toSink(config.main.output.enriched.sink).onError {
                        case _ => Resource.eval(appHealth.beUnhealthyForRuntimeService(RuntimeService.EnrichedSink))
                      }
      failedSink <- config.main.output.failed.traverse { sinkConfig =>
                      toSink(sinkConfig.sink)
                        .onError {
                          case _ => Resource.eval(appHealth.beUnhealthyForRuntimeService(RuntimeService.FailedSink))
                        }
                    }
      badSink <- toSink(config.main.output.bad.sink).onError {
                   case _ => Resource.eval(appHealth.beUnhealthyForRuntimeService(RuntimeService.BadSink))
                 }
      metrics <- Resource.eval(Metrics.build(config.main.monitoring.metrics))
      cpuParallelism = chooseCpuParallelism(config.main)
      adapterRegistry = new AdapterRegistry(Map.empty, config.main.adaptersSchemas)
      resolver <- mkResolver[F](config.iglu)
      igluClient <- Resource.eval(IgluCirceClient.fromResolver(resolver, config.iglu.cacheSize, config.main.validation.maxJsonDepth))
      httpClient <- HttpClient.resource[F](
                      config.main.http.client
                    )
      registryLookup = Http4sRegistryLookup(httpClient)
      enrichmentsConfs <- Resource.eval {
                            EnrichmentRegistry
                              .parse[F](config.enrichments, igluClient, false, registryLookup)
                              .map(
                                _.toEither.valueOr(errors =>
                                  throw new IllegalArgumentException(s"Can't decode enrichments configs: [${errors.mkString_("], [")}]")
                                )
                              )
                          }
      blobClients = HttpBlobClient.wrapHttp4sClient(httpClient) :: toBlobClients(config.main.blobClients)
      enrichmentRegistry <-
        mkEnrichmentRegistry(enrichmentsConfs, blobClients, config.main.http.client, config.main.validation.exitOnJsCompileError)
      metadata <- config.main.metadata.traverse(MetadataReporter.build[F](_, appInfo, httpClient))
    } yield Environment(
      appInfo = appInfo,
      source = sourceAndAck,
      appHealth = appHealth,
      enrichedSink = enrichedSink,
      failedSink = failedSink,
      badSink = badSink,
      metrics = metrics,
      cpuParallelism = cpuParallelism,
      sinkMaxSize = config.main.output.enriched.maxRecordSize,
      adapterRegistry = adapterRegistry,
      enrichmentRegistry = enrichmentRegistry,
      igluClient = igluClient,
      httpClient = httpClient,
      registryLookup = registryLookup,
      validation = config.main.validation,
      partitionKeyField = config.main.output.enriched.partitionKey,
      attributeFields = config.main.output.enriched.attributes,
      metadata = metadata
    )

  private def enableSentry[F[_]: Sync](appInfo: AppInfo, config: Option[Config.Sentry]): Resource[F, Unit] =
    config match {
      case Some(c) =>
        val acquire = Sync[F].delay {
          Sentry.init { options =>
            options.setDsn(c.dsn)
            options.setRelease(appInfo.version)
            c.tags.foreach {
              case (k, v) =>
                options.setTag(k, v)
            }
          }
        }

        Resource.makeCase(acquire) {
          case (_, Resource.ExitCase.Errored(e)) => Sync[F].delay(Sentry.captureException(e)).void
          case _ => Sync[F].unit
        }
      case None =>
        Resource.unit[F]
    }

  private def mkResolver[F[_]: Async](resolverConfig: Resolver.ResolverConfig): Resource[F, Resolver[F]] =
    Resource.eval {
      Resolver
        .fromConfig[F](resolverConfig)
        .leftMap(e => new RuntimeException(s"Error while parsing Iglu resolver config", e))
        .value
        .rethrow
    }

  def mkEnrichmentRegistry[F[_]: Async](
    enrichmentsConfs: List[EnrichmentConf],
    blobClients: List[BlobClient[F]],
    httpClientConfig: HttpClientConfig,
    exitOnJsCompileError: Boolean
  ): Resource[F, EnrichmentRegistry[F]] =
    for {
      _ <- Resource.eval(Logger[F].info(show"Enabled enrichments: ${enrichmentsConfs.map(_.schemaKey.name).mkString(", ")}"))
      _ <- Resource.eval(Assets.downloadAll(enrichmentsConfs, blobClients))
      apiEnrichmentClient <- enrichmentsConfs.collectFirst { case api: ApiRequestConf => api.api.timeout } match {
                               case Some(timeout) =>
                                 HttpClient.resource[F](httpClientConfig, timeout.millis).map(CommonHttpClient.fromHttp4sClient[F])
                               case None =>
                                 Resource.pure[F, CommonHttpClient[F]](CommonHttpClient.noop[F])
                             }
      ipLookupEC <- IpLookupExecutionContext.mk
      sqlEC <- SqlExecutionContext.mk
      maybeRegistry <- Resource.eval {
                         EnrichmentRegistry
                           .build(
                             enrichmentsConfs,
                             apiEnrichmentClient,
                             ipLookupEC,
                             sqlEC,
                             exitOnJsCompileError
                           )
                           .value
                       }
      registry <- maybeRegistry match {
                    case Right(r) => Resource.pure[F, EnrichmentRegistry[F]](r)
                    case Left(error) =>
                      Resource.raiseError[F, EnrichmentRegistry[F], Throwable](
                        new IllegalArgumentException(s"Can't build enrichments registry: $error")
                      )
                  }
    } yield registry

  /**
   * See the description of `cpuParallelism` on the [[Environment]] class
   *
   * For bigger instances (more cores) we want more parallelism, so that cpu-intensive steps can
   * take advantage of all the cores.
   */
  private def chooseCpuParallelism(config: AnyConfig): Int =
    (Runtime.getRuntime.availableProcessors * config.cpuParallelismFraction)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt
}
