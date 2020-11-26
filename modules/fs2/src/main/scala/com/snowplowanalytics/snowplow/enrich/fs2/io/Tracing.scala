package com.snowplowanalytics.snowplow.enrich.fs2.io

import cats.effect.{Sync, Resource}

import io.opencensus.trace.samplers.Samplers
import io.opencensus.trace.Tracing.getExportComponent
import io.opencensus.exporter.trace.zipkin.ZipkinExporterConfiguration
import io.opencensus.exporter.trace.zipkin.ZipkinTraceExporter

import natchez.EntryPoint
import natchez.opencensus.OpenCensus

import com.snowplowanalytics.snowplow.enrich.fs2.config.{ Tracing => TracingConfig }

object Tracing {
  def mkExporter[F[_]: Sync](config: TracingConfig): Resource[F, Unit] = {
    val cfg = ZipkinExporterConfiguration.builder().setV2Url(config.endpoint).setServiceName("enrich").build()
    val register = Sync[F].delay(ZipkinTraceExporter.createAndRegister(cfg))
    val shutdown = Sync[F].delay {
      getExportComponent.shutdown()
      ZipkinTraceExporter.unregister()
    }
    Resource.make(register)(_ => shutdown)
  }

  def build[F[_]: Sync](config: TracingConfig): Resource[F, EntryPoint[F]] =
    for {
      _ <- mkExporter[F](config)
      entryPoint <- Resource.liftF[F, EntryPoint[F]](OpenCensus.entryPoint[F](Samplers.alwaysSample()))
    } yield entryPoint
}
