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
package com.snowplowanalytics.snowplow.enrich.common

import cats.syntax.validated._
import cats.data.Ior

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect

import org.apache.thrift.TSerializer

import org.joda.time.DateTime

import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.Registry

import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.{CollectorPayload => tCollectorPayload}

import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.RemoteAdapter
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{AtomicFields, EnrichmentRegistry}
import com.snowplowanalytics.snowplow.enrich.common.loaders.{CollectorPayload, ThriftLoader}

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class EtlPipelineSpec extends Specification with ValidatedMatchers with CatsEffect {
  def is = s2"""
  EtlPipeline should always produce either bad or good row for each event of the payload   $e1
  Processing of events with malformed query string should be supported                     $e2
  Processing of invalid CollectorPayload (CPFormatViolation bad row) should be supported   $e3
  Absence of CollectorPayload (None) should be supported                                   $e4
  """

  val adapterRegistry = new AdapterRegistry[IO](
    Map.empty[(String, String), RemoteAdapter[IO]],
    adaptersSchemas = adaptersSchemas
  )
  val enrichmentReg = EnrichmentRegistry[IO]()
  val igluCentral = Registry.IgluCentral
  def igluClient = IgluCirceClient.fromResolver[IO](Resolver[IO](List(igluCentral), None), cacheSize = 0, maxJsonDepth = 40)
  val processor = Processor("sce-test-suite", "1.0.0")
  val dateTime = DateTime.now()

  def e1 =
    for {
      client <- igluClient
      collectorPayloadBatched = EtlPipelineSpec.buildBatchedPayload()
      output <- EtlPipeline
                  .processEvents[IO](
                    adapterRegistry,
                    enrichmentReg,
                    client,
                    processor,
                    dateTime,
                    Some(collectorPayloadBatched).validNel,
                    AcceptInvalid.featureFlags,
                    IO.unit,
                    SpecHelpers.registryLookup,
                    AtomicFields.from(Map.empty),
                    emitIncomplete,
                    SpecHelpers.DefaultMaxJsonDepth
                  )
    } yield output must be like {
      case Ior.Right(_) :: Ior.Left(_) :: Ior.Left(_) :: Ior.Left(_) :: Nil => ok
      case other => ko(s"[$other] is not a list with 1 enriched event and 3 bad rows")
    }

  def e2 =
    for {
      client <- igluClient
      thriftBytesMalformedQS = EtlPipelineSpec.buildThriftBytesMalformedQS()
      collectorPayload = ThriftLoader
                           .toCollectorPayload(thriftBytesMalformedQS, processor)
                           .map(_.get)
                           .toOption
                           .get
      output <- EtlPipeline
                  .processEvents[IO](
                    adapterRegistry,
                    enrichmentReg,
                    client,
                    processor,
                    dateTime,
                    Some(collectorPayload).validNel,
                    AcceptInvalid.featureFlags,
                    IO.unit,
                    SpecHelpers.registryLookup,
                    AtomicFields.from(Map.empty),
                    emitIncomplete,
                    SpecHelpers.DefaultMaxJsonDepth
                  )
    } yield output must be like {
      case Ior.Right(_) :: Nil => ok
      case other => ko(s"[$other] is not a list with 1 enriched event")
    }

  def e3 =
    for {
      client <- igluClient
      invalidCollectorPayload = ThriftLoader.toCollectorPayload(Array(1.toByte), processor)
      output <- EtlPipeline
                  .processEvents[IO](
                    adapterRegistry,
                    enrichmentReg,
                    client,
                    processor,
                    dateTime,
                    invalidCollectorPayload,
                    AcceptInvalid.featureFlags,
                    IO.unit,
                    SpecHelpers.registryLookup,
                    AtomicFields.from(Map.empty),
                    emitIncomplete,
                    SpecHelpers.DefaultMaxJsonDepth
                  )
    } yield output must be like {
      case Ior.Left(_: BadRow.CPFormatViolation) :: Nil => ok
      case other => ko(s"[$other] is not a CPFormatViolation bad row")
    }

  def e4 =
    for {
      client <- igluClient
      collectorPayload = None
      output <- EtlPipeline
                  .processEvents[IO](
                    adapterRegistry,
                    enrichmentReg,
                    client,
                    processor,
                    dateTime,
                    collectorPayload.validNel[BadRow],
                    AcceptInvalid.featureFlags,
                    IO.unit,
                    SpecHelpers.registryLookup,
                    AtomicFields.from(Map.empty),
                    emitIncomplete,
                    SpecHelpers.DefaultMaxJsonDepth
                  )
    } yield output must beEqualTo(Nil)
}

object EtlPipelineSpec {
  def buildBatchedPayload(): CollectorPayload = {
    val context =
      CollectorPayload.Context(
        Some(DateTime.parse("2017-07-14T03:39:39.000+00:00")),
        Some("127.0.0.1"),
        None,
        None,
        Nil,
        None
      )
    val source = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
    CollectorPayload(
      CollectorPayload.Api("com.snowplowanalytics.snowplow", "tp2"),
      Nil,
      Some("application/json"),
      Some(
        """{
          |"schema":"iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4",
          |"data":[
          |  {"e":"pv","url":"https://console.snowplowanalytics.com/?code=3EaD7VJ2f_aZn33R&state=Y1F6eHNnelUwRVR0c1Q4N3Y3RVM0NlFjdEFLRll6NmsxSnEuZzl%2BTktTQg%3D%3D","page":"Snowplow Insights","refr":"https://id.snowplowanalytics.com/u/login?state=g6Fo2SBDbWRWUmJHTXlHM05pTXhkRTliUUo0YlVIWnVtd0lwUqN0aWTZIHdaQmRyV3NuTTlvNFVZX2tyNUt2MEljcDFGRm9lakFPo2NpZNkgMllzeVFqRHJDTVhoRmRqeXI1MmZ6NDZqUXNlQVpuUUY","tv":"js-2.10.2","tna":"snplow5","aid":"console","p":"web","tz":"America/Chicago","lang":"en-US","cs":"UTF-8","f_pdf":"1","f_qt":"0","f_realp":"0","f_wma":"0","f_dir":"0","f_fla":"0","f_java":"1","f_gears":"0","f_ag":"0","res":"1792x1120","cd":"24","cookie":"1","eid":"1a950884-61d4-4179-a89a-43b67fb58a8f","dtm":"1581382581877","cx":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W3sic2NoZW1hIjoiaWdsdTpjb20uZ29vZ2xlLmFuYWx5dGljcy9jb29raWVzL2pzb25zY2hlbWEvMS0wLTAiLCJkYXRhIjp7Il9nYSI6IkdBMS4yLjkwOTgzMDA0OS4xNTgxMDgxMzMzIn19LHsic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvd2ViX3BhZ2UvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsiaWQiOiIxNTNmOTA1ZC1iZGFjLTRhYzUtODI2Yy04YWExNmNmMTNmMzkifX0seyJzY2hlbWEiOiJpZ2x1Om9yZy53My9QZXJmb3JtYW5jZVRpbWluZy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJuYXZpZ2F0aW9uU3RhcnQiOjE1ODEzODI1ODA3MDYsInVubG9hZEV2ZW50U3RhcnQiOjAsInVubG9hZEV2ZW50RW5kIjowLCJyZWRpcmVjdFN0YXJ0IjowLCJyZWRpcmVjdEVuZCI6MCwiZmV0Y2hTdGFydCI6MTU4MTM4MjU4MTM3OCwiZG9tYWluTG9va3VwU3RhcnQiOjE1ODEzODI1ODEzNzgsImRvbWFpbkxvb2t1cEVuZCI6MTU4MTM4MjU4MTM3OCwiY29ubmVjdFN0YXJ0IjoxNTgxMzgyNTgxMzc4LCJjb25uZWN0RW5kIjoxNTgxMzgyNTgxMzc4LCJzZWN1cmVDb25uZWN0aW9uU3RhcnQiOjAsInJlcXVlc3RTdGFydCI6MTU4MTM4MjU4MTM3OCwicmVzcG9uc2VTdGFydCI6MTU4MTM4MjU4MTUwMCwicmVzcG9uc2VFbmQiOjE1ODEzODI1ODE1MTAsImRvbUxvYWRpbmciOjE1ODEzODI1ODE1MTEsImRvbUludGVyYWN0aXZlIjoxNTgxMzgyNTgxNjg5LCJkb21Db250ZW50TG9hZGVkRXZlbnRTdGFydCI6MTU4MTM4MjU4MTY4OSwiZG9tQ29udGVudExvYWRlZEV2ZW50RW5kIjoxNTgxMzgyNTgxNjg5LCJkb21Db21wbGV0ZSI6MCwibG9hZEV2ZW50U3RhcnQiOjAsImxvYWRFdmVudEVuZCI6MH19XX0","vp":"1792x1034","ds":"1792x1034","vid":"4","sid":"82138304-11e6-4999-92a9-2b99118ec50a","duid":"27b9c270-8741-4b77-8082-14c76341d33e","stm":"1581382583373"},
          |  {"e":"ue","ue_px":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9hcHBsaWNhdGlvbl9lcnJvci9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6eyJwcm9ncmFtbWluZ0xhbmd1YWdlIjoiSkFWQVNDUklQVCIsIm1lc3NhZ2UiOiJBUElfRVJST1IgLSA0MDMgLSAiLCJzdGFja1RyYWNlIjpudWxsLCJsaW5lTnVtYmVyIjowLCJsaW5lQ29sdW1uIjowLCJmaWxlTmFtZSI6IiJ9fX0","tv":"js-2.10.2","tna":"snplow5","aid":"console","p":"web","tz":"America/Chicago","lang":"en-US","cs":"UTF-8","f_pdf":"1","f_qt":"0","f_realp":"0","f_wma":"0","f_dir":"0","f_fla":"0","f_java":"1","f_gears":"0","f_ag":"0","res":"1792x1120","cd":"24","cookie":"1","eid":"fed3c6ca-5b8e-47a7-9bfa-102804bd14e0","dtm":"1581382582270","cx":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W3sic2NoZW1hIjoiaWdsdTpjb20uZ29vZ2xlLmFuYWx5dGljcy9jb29raWVzL2pzb25zY2hlbWEvMS0wLTAiLCJkYXRhIjp7Il9nYSI6IkdBMS4yLjkwOTgzMDA0OS4xNTgxMDgxMzMzIn19LHsic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvd2ViX3BhZ2UvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsiaWQiOiI0MDVlM2U4My1mOGQ3LTRmM2MtYWE5Zi1jNjQ4MzU5NjRhYzkifX0seyJzY2hlbWEiOiJpZ2x1Om9yZy53My9QZXJmb3JtYW5jZVRpbWluZy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJuYXZpZ2F0aW9uU3RhcnQiOjE1ODEzODI1ODA3MDYsInVubG9hZEV2ZW50U3RhcnQiOjAsInVubG9hZEV2ZW50RW5kIjowLCJyZWRpcmVjdFN0YXJ0IjowLCJyZWRpcmVjdEVuZCI6MCwiZmV0Y2hTdGFydCI6MTU4MTM4MjU4MTM3OCwiZG9tYWluTG9va3VwU3RhcnQiOjE1ODEzODI1ODEzNzgsImRvbWFpbkxvb2t1cEVuZCI6MTU4MTM4MjU4MTM3OCwiY29ubmVjdFN0YXJ0IjoxNTgxMzgyNTgxMzc4LCJjb25uZWN0RW5kIjoxNTgxMzgyNTgxMzc4LCJzZWN1cmVDb25uZWN0aW9uU3RhcnQiOjAsInJlcXVlc3RTdGFydCI6MTU4MTM4MjU4MTM3OCwicmVzcG9uc2VTdGFydCI6MTU4MTM4MjU4MTUwMCwicmVzcG9uc2VFbmQiOjE1ODEzODI1ODE1MTAsImRvbUxvYWRpbmciOjE1ODEzODI1ODE1MTEsImRvbUludGVyYWN0aXZlIjoxNTgxMzgyNTgxNjg5LCJkb21Db250ZW50TG9hZGVkRXZlbnRTdGFydCI6MTU4MTM4MjU4MTY4OSwiZG9tQ29udGVudExvYWRlZEV2ZW50RW5kIjoxNTgxMzgyNTgxNjg5LCJkb21Db21wbGV0ZSI6MTU4MTM4MjU4MTg3OSwibG9hZEV2ZW50U3RhcnQiOjE1ODEzODI1ODE4NzksImxvYWRFdmVudEVuZCI6MTU4MTM4MjU4MTg3OX19LHsic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuY29uc29sZS91c2VyL2pzb25zY2hlbWEvMS0wLTAiLCJkYXRhIjp7InVzZXJJZCI6IjIyNTIzOTg1LTUxMzMtNDk1YS05ZTY5LWM3YzIzMDVkOWMxMyIsImZpcnN0TmFtZSI6IlRpIiwibGFzdE5hbWUiOiJMaXB0YWsifX0seyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5jb25zb2xlL29yZ2FuaXphdGlvbi9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJuYW1lIjoiQmlnIFRyZWUiLCJvcmdhbml6YXRpb25JZCI6IjZjZTI0Y2YxLTEzMzAtNGRmYi05YTk0LWVmOTEyNzkxYzE2MCJ9fV19","vp":"1792x1034","ds":"1792x1034","vid":"4","sid":"82138304-11e6-4999-92a9-2b99118ec50a","duid":"27b9c270-8741-4b77-8082-14c76341d33e","refr":"https://console.snowplowanalytics.com/?code=3EaD7VJ2f_aZn33R&state=Y1F6eHNnelUwRVR0c1Q4N3Y3RVM0NlFjdEFLRll6NmsxSnEuZzl%2BTktTQg%3D%3D","url":"https://console.snowplowanalytics.com/","stm":"1581382583373"},
          |  {"e":"ue","ue_px":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9hcHBsaWNhdGlvbl9lcnJvci9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6eyJwcm9ncmFtbWluZ0xhbmd1YWdlIjoiSkFWQVNDUklQVCIsIm1lc3NhZ2UiOiJBUElfRVJST1IgLSA0MDMgLSAiLCJzdGFja1RyYWNlIjpudWxsLCJsaW5lTnVtYmVyIjowLCJsaW5lQ29sdW1uIjowLCJmaWxlTmFtZSI6IiJ9fX0","tv":"js-2.10.2","tna":"snplow5","aid":"console","p":"web","tz":"America/Chicago","lang":"en-US","cs":"UTF-8","f_pdf":"1","f_qt":"0","f_realp":"0","f_wma":"0","f_dir":"0","f_fla":"0","f_java":"1","f_gears":"0","f_ag":"0","res":"1792x1120","cd":"24","cookie":"1","eid":"ab8f3859-37cc-4e4a-af9f-3dd91464ee25","dtm":"1581382582276","cx":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W3sic2NoZW1hIjoiaWdsdTpjb20uZ29vZ2xlLmFuYWx5dGljcy9jb29raWVzL2pzb25zY2hlbWEvMS0wLTAiLCJkYXRhIjp7Il9nYSI6IkdBMS4yLjkwOTgzMDA0OS4xNTgxMDgxMzMzIn19LHsic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvd2ViX3BhZ2UvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsiaWQiOiI0MDVlM2U4My1mOGQ3LTRmM2MtYWE5Zi1jNjQ4MzU5NjRhYzkifX0seyJzY2hlbWEiOiJpZ2x1Om9yZy53My9QZXJmb3JtYW5jZVRpbWluZy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJuYXZpZ2F0aW9uU3RhcnQiOjE1ODEzODI1ODA3MDYsInVubG9hZEV2ZW50U3RhcnQiOjAsInVubG9hZEV2ZW50RW5kIjowLCJyZWRpcmVjdFN0YXJ0IjowLCJyZWRpcmVjdEVuZCI6MCwiZmV0Y2hTdGFydCI6MTU4MTM4MjU4MTM3OCwiZG9tYWluTG9va3VwU3RhcnQiOjE1ODEzODI1ODEzNzgsImRvbWFpbkxvb2t1cEVuZCI6MTU4MTM4MjU4MTM3OCwiY29ubmVjdFN0YXJ0IjoxNTgxMzgyNTgxMzc4LCJjb25uZWN0RW5kIjoxNTgxMzgyNTgxMzc4LCJzZWN1cmVDb25uZWN0aW9uU3RhcnQiOjAsInJlcXVlc3RTdGFydCI6MTU4MTM4MjU4MTM3OCwicmVzcG9uc2VTdGFydCI6MTU4MTM4MjU4MTUwMCwicmVzcG9uc2VFbmQiOjE1ODEzODI1ODE1MTAsImRvbUxvYWRpbmciOjE1ODEzODI1ODE1MTEsImRvbUludGVyYWN0aXZlIjoxNTgxMzgyNTgxNjg5LCJkb21Db250ZW50TG9hZGVkRXZlbnRTdGFydCI6MTU4MTM4MjU4MTY4OSwiZG9tQ29udGVudExvYWRlZEV2ZW50RW5kIjoxNTgxMzgyNTgxNjg5LCJkb21Db21wbGV0ZSI6MTU4MTM4MjU4MTg3OSwibG9hZEV2ZW50U3RhcnQiOjE1ODEzODI1ODE4NzksImxvYWRFdmVudEVuZCI6MTU4MTM4MjU4MTg3OX19LHsic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuY29uc29sZS91c2VyL2pzb25zY2hlbWEvMS0wLTAiLCJkYXRhIjp7InVzZXJJZCI6IjIyNTIzOTg1LTUxMzMtNDk1YS05ZTY5LWM3YzIzMDVkOWMxMyIsImZpcnN0TmFtZSI6IlRpIiwibGFzdE5hbWUiOiJMaXB0YWsifX0seyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5jb25zb2xlL29yZ2FuaXphdGlvbi9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJuYW1lIjoiQmlnIFRyZWUiLCJvcmdhbml6YXRpb25JZCI6IjZjZTI0Y2YxLTEzMzAtNGRmYi05YTk0LWVmOTEyNzkxYzE2MCJ9fV19","vp":"1792x1034","ds":"1792x1034","vid":"4","sid":"82138304-11e6-4999-92a9-2b99118ec50a","duid":"27b9c270-8741-4b77-8082-14c76341d33e","refr":"https://console.snowplowanalytics.com/?code=3EaD7VJ2f_aZn33R&state=Y1F6eHNnelUwRVR0c1Q4N3Y3RVM0NlFjdEFLRll6NmsxSnEuZzl%2BTktTQg%3D%3D","url":"https://console.snowplowanalytics.com/","stm":"1581382583373"},
          |  {"e":"ue","ue_px":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy91bnN0cnVjdF9ldmVudC9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9hcHBsaWNhdGlvbl9lcnJvci9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6eyJwcm9ncmFtbWluZ0xhbmd1YWdlIjoiSkFWQVNDUklQVCIsIm1lc3NhZ2UiOiJBUElfRVJST1IgLSA0MDMgLSAiLCJzdGFja1RyYWNlIjpudWxsLCJsaW5lTnVtYmVyIjowLCJsaW5lQ29sdW1uIjowLCJmaWxlTmFtZSI6IiJ9fX0","tv":"js-2.10.2","tna":"snplow5","aid":"console","p":"web","tz":"America/Chicago","lang":"en-US","cs":"UTF-8","f_pdf":"1","f_qt":"0","f_realp":"0","f_wma":"0","f_dir":"0","f_fla":"0","f_java":"1","f_gears":"0","f_ag":"0","res":"1792x1120","cd":"24","cookie":"1","eid":"e94b4262-63e5-4811-b5db-21c4bd2ab354","dtm":"1581382582388","cx":"eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5zbm93cGxvdy9jb250ZXh0cy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6W3sic2NoZW1hIjoiaWdsdTpjb20uZ29vZ2xlLmFuYWx5dGljcy9jb29raWVzL2pzb25zY2hlbWEvMS0wLTAiLCJkYXRhIjp7Il9nYSI6IkdBMS4yLjkwOTgzMDA0OS4xNTgxMDgxMzMzIn19LHsic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3Muc25vd3Bsb3cvd2ViX3BhZ2UvanNvbnNjaGVtYS8xLTAtMCIsImRhdGEiOnsiaWQiOiI0MDVlM2U4My1mOGQ3LTRmM2MtYWE5Zi1jNjQ4MzU5NjRhYzkifX0seyJzY2hlbWEiOiJpZ2x1Om9yZy53My9QZXJmb3JtYW5jZVRpbWluZy9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJuYXZpZ2F0aW9uU3RhcnQiOjE1ODEzODI1ODA3MDYsInVubG9hZEV2ZW50U3RhcnQiOjAsInVubG9hZEV2ZW50RW5kIjowLCJyZWRpcmVjdFN0YXJ0IjowLCJyZWRpcmVjdEVuZCI6MCwiZmV0Y2hTdGFydCI6MTU4MTM4MjU4MTM3OCwiZG9tYWluTG9va3VwU3RhcnQiOjE1ODEzODI1ODEzNzgsImRvbWFpbkxvb2t1cEVuZCI6MTU4MTM4MjU4MTM3OCwiY29ubmVjdFN0YXJ0IjoxNTgxMzgyNTgxMzc4LCJjb25uZWN0RW5kIjoxNTgxMzgyNTgxMzc4LCJzZWN1cmVDb25uZWN0aW9uU3RhcnQiOjAsInJlcXVlc3RTdGFydCI6MTU4MTM4MjU4MTM3OCwicmVzcG9uc2VTdGFydCI6MTU4MTM4MjU4MTUwMCwicmVzcG9uc2VFbmQiOjE1ODEzODI1ODE1MTAsImRvbUxvYWRpbmciOjE1ODEzODI1ODE1MTEsImRvbUludGVyYWN0aXZlIjoxNTgxMzgyNTgxNjg5LCJkb21Db250ZW50TG9hZGVkRXZlbnRTdGFydCI6MTU4MTM4MjU4MTY4OSwiZG9tQ29udGVudExvYWRlZEV2ZW50RW5kIjoxNTgxMzgyNTgxNjg5LCJkb21Db21wbGV0ZSI6MTU4MTM4MjU4MTg3OSwibG9hZEV2ZW50U3RhcnQiOjE1ODEzODI1ODE4NzksImxvYWRFdmVudEVuZCI6MTU4MTM4MjU4MTg3OX19LHsic2NoZW1hIjoiaWdsdTpjb20uc25vd3Bsb3dhbmFseXRpY3MuY29uc29sZS91c2VyL2pzb25zY2hlbWEvMS0wLTAiLCJkYXRhIjp7InVzZXJJZCI6IjIyNTIzOTg1LTUxMzMtNDk1YS05ZTY5LWM3YzIzMDVkOWMxMyIsImZpcnN0TmFtZSI6IlRpIiwibGFzdE5hbWUiOiJMaXB0YWsifX0seyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5jb25zb2xlL29yZ2FuaXphdGlvbi9qc29uc2NoZW1hLzEtMC0wIiwiZGF0YSI6eyJuYW1lIjoiQmlnIFRyZWUiLCJvcmdhbml6YXRpb25JZCI6IjZjZTI0Y2YxLTEzMzAtNGRmYi05YTk0LWVmOTEyNzkxYzE2MCJ9fV19","vp":"1792x1034","ds":"1792x1034","vid":"4","sid":"82138304-11e6-4999-92a9-2b99118ec50a","duid":"27b9c270-8741-4b77-8082-14c76341d33e","refr":"https://console.snowplowanalytics.com/?code=3EaD7VJ2f_aZn33R&state=Y1F6eHNnelUwRVR0c1Q4N3Y3RVM0NlFjdEFLRll6NmsxSnEuZzl%2BTktTQg%3D%3D","url":"https://console.snowplowanalytics.com/","stm":"1581382583373"}
          |]}""".stripMargin
      ),
      source,
      context
    )
  }

  def buildThriftBytesMalformedQS(): Array[Byte] = {
    val tCP = new tCollectorPayload(
      "iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0",
      "1.2.3.4",
      System.currentTimeMillis,
      "UTF-8",
      "EtlPipelineSpec collector"
    )
    tCP.setPath("com.snowplowanalytics.iglu/v1")
    val schema = "iglu:com.mailgun/message_clicked/jsonschema/1-0-0"
    val queryString = s"schema=$schema&city=Berlin&token=42&foo" // param foo has no value
    tCP.setQuerystring(queryString)

    val ThriftSerializer = new ThreadLocal[TSerializer] {
      override def initialValue = new TSerializer()
    }
    val serializer = ThriftSerializer.get()
    serializer.serialize(tCP)
  }
}
