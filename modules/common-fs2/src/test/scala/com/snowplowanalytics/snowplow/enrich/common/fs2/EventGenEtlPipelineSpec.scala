/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2

import cats.data.{Ior, ValidatedNel}
import cats.effect.testing.specs2.CatsEffect
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.Registry
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}
import com.snowplowanalytics.snowplow.enrich.common.EtlPipeline
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.loaders._
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent.toPartiallyEnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.RemoteAdapter
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.eventgen.runGen
import com.snowplowanalytics.snowplow.eventgen.enrich.{SdkEvent => GenSdkEvent}
import org.specs2.matcher.MustMatchers.{left => _, right => _}
import com.softwaremill.diffx.specs2.DiffMatcher._
import com.softwaremill.diffx.generic.auto._
import fs2.{Pipe, Stream}
import _root_.io.circe.{Decoder, Json, JsonNumber, JsonObject}
import _root_.io.circe.parser.decode
import _root_.io.circe.syntax._
import _root_.io.circe.generic.auto._
import _root_.io.circe.literal._
import org.joda.time.DateTime
import org.specs2.mutable.Specification
import org.specs2.specification.core.{Fragment, Fragments}

import java.time.Instant
import scala.util.{Random, Try}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.AtomicFields

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers

class EventGenEtlPipelineSpec extends Specification with CatsEffect {
  case class ContextMatcher(v: String)

  implicit val cmDecoder: Decoder[ContextMatcher] = Decoder.decodeString.emapTry { str =>
    Try(ContextMatcher(str))
  }

  case class IntermediateEvent(
    app_id: Option[String],
    platform: Option[String],
    event: Option[String],
    txn_id: Option[String],
    name_tracker: Option[String],
    v_tracker: Option[String],
    v_collector: Option[String],
    user_id: Option[String],
    user_ipaddress: Option[String],
    user_fingerprint: Option[String],
    domain_userid: Option[String],
    domain_sessionidx: Option[String],
    network_userid: Option[String],
    geo_country: Option[String],
    geo_region: Option[String],
    geo_city: Option[String],
    geo_zipcode: Option[String],
    geo_latitude: Option[String],
    geo_longitude: Option[String],
    geo_region_name: Option[String],
    ip_isp: Option[String],
    ip_organization: Option[String],
    ip_domain: Option[String],
    ip_netspeed: Option[String],
    refr_urlscheme: Option[String],
    refr_urlhost: Option[String],
    refr_urlport: Option[String],
    refr_urlpath: Option[String],
    refr_urlquery: Option[String],
    refr_urlfragment: Option[String],
    refr_medium: Option[String],
    refr_source: Option[String],
    refr_term: Option[String],
    mkt_medium: Option[String],
    mkt_source: Option[String],
    mkt_term: Option[String],
    mkt_content: Option[String],
    mkt_campaign: Option[String],
    contexts: Option[ContextMatcher],
    unstruct_event: Option[String],
    tr_orderid: Option[String],
    tr_affiliation: Option[String],
    tr_total: Option[String],
    tr_tax: Option[String],
    tr_shipping: Option[String],
    tr_city: Option[String],
    tr_state: Option[String],
    tr_country: Option[String],
    ti_orderid: Option[String],
    ti_sku: Option[String],
    ti_name: Option[String],
    ti_category: Option[String],
    ti_price: Option[String],
    ti_quantity: Option[String],
    br_name: Option[String],
    br_family: Option[String],
    br_version: Option[String],
    br_type: Option[String],
    br_renderengine: Option[String],
    br_lang: Option[String],
    br_features_pdf: Option[String],
    br_features_flash: Option[String],
    br_features_java: Option[String],
    br_features_director: Option[String],
    br_features_quicktime: Option[String],
    br_features_realplayer: Option[String],
    br_features_windowsmedia: Option[String],
    br_features_gears: Option[String],
    br_features_silverlight: Option[String],
    br_cookies: Option[String],
    br_colordepth: Option[String],
    br_viewwidth: Option[String],
    br_viewheight: Option[String],
    os_name: Option[String],
    os_family: Option[String],
    os_manufacturer: Option[String],
    os_timezone: Option[String],
    dvce_type: Option[String],
    dvce_ismobile: Option[String],
    dvce_screenwidth: Option[String],
    dvce_screenheight: Option[String],
    doc_charset: Option[String],
    doc_width: Option[String],
    doc_height: Option[String],
    tr_currency: Option[String],
    tr_total_base: Option[String],
    tr_tax_base: Option[String],
    tr_shipping_base: Option[String],
    ti_currency: Option[String],
    ti_price_base: Option[String],
    base_currency: Option[String],
    geo_timezone: Option[String],
    mkt_clickid: Option[String],
    mkt_network: Option[String],
    etl_tags: Option[String],
    refr_domain_userid: Option[String],
    refr_dvce_tstamp: Option[String],
    domain_sessionid: Option[String],
    event_fingerprint: Option[String]
  )

  object IntermediateEvent {
    def pad(e: IntermediateEvent): IntermediateEvent = e.copy(contexts = Some(e.contexts.getOrElse(ContextMatcher("{}"))))
  }

  val rng: Random = new scala.util.Random(1L)

  val adapterRegistry = new AdapterRegistry(Map.empty[(String, String), RemoteAdapter[IO]], SpecHelpers.adaptersSchemas)
  val enrichmentReg = EnrichmentRegistry[IO]()
  val igluCentral = Registry.IgluCentral
  val client = IgluCirceClient.parseDefault[IO](json"""
      {
        "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-1",
        "data": {
          "cacheSize": 500,
          "repositories": [
            {
              "name": "Iglu Central",
              "priority": 0,
              "vendorPrefixes": [ "com.snowplowanalytics" ],
              "connection": {
                "http": {
                  "uri": "http://iglucentral.com"
                }
              }
            },
            {
              "name": "Iglu Central - GCP Mirror",
              "priority": 1,
              "vendorPrefixes": [ "com.snowplowanalytics" ],
              "connection": {
                "http": {
                  "uri": "http://mirror01.iglucentral.com"
                }
              }
            }
          ]
        }
      }
      """)
  val processor = Processor("sce-test-suite", "1.0.0")
  val dateTime = DateTime.now()
  val process = Processor("EventGenEtlPipelineSpec", "v1")

  def processEvents(e: CollectorPayload): IO[List[Ior[BadRow, EnrichedEvent]]] =
    EtlPipeline.processEvents[IO](
      adapterRegistry,
      enrichmentReg,
      client.rethrowT.unsafeRunSync(),
      processor,
      dateTime,
      Some(e).validNel,
      EtlPipeline.FeatureFlags(acceptInvalid = false, legacyEnrichmentOrder = false),
      IO.unit,
      SpecHelpers.registryLookup,
      AtomicFields.from(Map.empty),
      SpecHelpers.emitIncomplete
    )

  def rethrowBadRows[A]: Pipe[IO, ValidatedNel[BadRow, A], A] =
    (in: Stream[IO, ValidatedNel[BadRow, A]]) =>
      in.map(e =>
        e.leftMap(er =>
          new Exception(
            er
              .foldLeft("\n")((acc, br) => acc + br.compact + "\n")
          )
        ).toEither
      ).rethrow[IO, A]

  def rethrowBadRow[A]: Pipe[IO, Ior[BadRow, A], A] =
    (in: Stream[IO, Ior[BadRow, A]]) =>
      in
        .map(_.leftMap(br => new Exception(br.compact)).toEither)
        .rethrow[IO, A]

  val innerFolder: Json.Folder[Json] = new Json.Folder[Json] {
    def onNull: Json = Json.Null
    def onBoolean(value: Boolean): Json = Json.fromString(if (value) "1" else "0")
    def onNumber(value: JsonNumber): Json = Json.fromString(value.toString)
    def onString(value: String): Json = Json.fromString(value)
    def onArray(value: Vector[Json]): Json = Json.fromValues(value.toIterable)
    def onObject(value: JsonObject): Json = Json.fromString(Json.fromJsonObject(value).noSpaces)
  }

  val folder = new Json.Folder[Json] {
    def onNull: Json = Json.Null
    def onBoolean(value: Boolean): Json = Json.fromString(if (value) "1" else "0")
    def onNumber(value: JsonNumber): Json = Json.fromString(value.toString)
    def onString(value: String): Json = Json.fromString(value)
    def onArray(value: Vector[Json]): Json = Json.fromValues(value.toIterable)
    def onObject(value: JsonObject): Json =
      Json.fromJsonObject(
        value.mapValues(_.foldWith(innerFolder))
      )
  }

  val GeneratorTest: Fragments = Stream
    .repeatEval(IO.delay(runGen(GenSdkEvent.genPair(1, 10, Instant.now), rng)))
    .take(50)
    .flatMap {
      case (payload, events) =>
        Stream
          .emit(ThriftLoader.toCollectorPayload(payload.toRaw, process))
          .covary[IO]
          .through(rethrowBadRows)
          .unNone
          .evalMap(processEvents)
          .flatMap(Stream.emits)
          .through(rethrowBadRow)
          .map(toPartiallyEnrichedEvent)
          .map(_.asJson.foldWith(folder).noSpaces)
          .map(decode[IntermediateEvent])
          .rethrow
          .map(IntermediateEvent.pad)
          .map(Option[IntermediateEvent])
          .zipAll(
            Stream
              .emits(events)
              .covary[IO]
              .map(_.toJson(false))
              .map(_.foldWith(folder))
              .map(_.noSpaces)
              .map(
                _.replaceAll(
                  "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
                  "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1"
                )
              )
              .map(decode[IntermediateEvent])
              .rethrow
              .map(IntermediateEvent.pad)
              .map(Option[IntermediateEvent])
          )(None, None)
    }
    .zipWithIndex
    .fold(
      List.empty[((Option[IntermediateEvent], Option[IntermediateEvent]), Long)]
    ) { (agg, els) =>
      els :: agg
    }
    .map(matches =>
      "All elements of origin stream should match" >>
        Fragment.foreach(matches.reverse) {
          case (els, i) =>
            s"Element $i should match the reference" >> {
              els._1 must matchTo(els._2)
            }
        }
    )
    .handleError { x =>
      "No exception was thrown" >> {
        x === None
      }
    }
    .compile
    .lastOrError
    .unsafeRunSync()
}

object EventGenEtlPipelineSpec {}
