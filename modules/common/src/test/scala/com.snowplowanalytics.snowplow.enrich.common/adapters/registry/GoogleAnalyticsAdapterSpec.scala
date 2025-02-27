/*
 * Copyright (c) 2018-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.adapters.registry

import cats.data.NonEmptyList
import cats.syntax.option._

import cats.effect.testing.specs2.CatsEffect

import org.joda.time.DateTime

import org.specs2.Specification
import org.specs2.matcher.{DataTables, ValidatedMatchers}

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers._

class GoogleAnalyticsAdapterSpec extends Specification with DataTables with ValidatedMatchers with CatsEffect {

  def is = s2"""
  toRawEvents returns a failNel if the query string is empty               $e1
  toRawEvents returns a failNel if there is no t param in the query string $e2
  toRawEvents returns a failNel if there are no corresponding hit types    $e3
  toRawEvents returns a succNel if the payload is correct                  $e4
  toRawEvents returns a succNel containing the added contexts              $e5
  toRawEvents returns a succNel containing the direct mappings             $e6
  toRawEvents returns a succNel containing properly typed contexts         $e7
  toRawEvents returns a succNel containing pageview as a context           $e8
  toRawEvents returns a succNel with product composite contexts            $e9
  toRawEvents returns a succNel with impression composite contexts         $e10
  toRawEvents returns a succNel with conflicting composite contexts        $e11
  toRawEvents returns a succNel with repeated composite contexts           $e12
  toRawEvents returns a succNel with promo composite contexts              $e13
  toRawEvents returns a succnel with multiple raw events                   $e14
  toRawEvents returns a succNel with multiple composite contexts with cu   $e15
  breakDownCompositeField should work properly                             $e20
  """

  val adapterWithDefaultSchemas = GoogleAnalyticsAdapter(schemas = googleAnalyticsSchemas)
  val api = CollectorPayload.Api("com.google.analytics.measurement-protocol", "v1")
  val source = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
  val context =
    CollectorPayload.Context(
      DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
      "37.157.33.123".some,
      None,
      None,
      Nil,
      None
    )

  val static = Map(
    "tv" -> "com.google.analytics.measurement-protocol-v1",
    "e" -> "ue",
    "p" -> "srv"
  ).toOpt

  val hitContext = (hitType: String) => s"""
    |{
      |"schema":"iglu:com.google.analytics.measurement-protocol/hit/jsonschema/1-0-0",
      |"data":{"type":"$hitType"}
    |}""".stripMargin.replaceAll("[\n\r]", "")

  def e1 = {
    val payload = CollectorPayload(api, Nil, None, None, source, context)
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.AdapterFailure.InputData("body", None, "empty body")
          )
        )
      )
  }

  def e2 = {
    val body = "dl=docloc"
    val payload = CollectorPayload(api, Nil, None, body.some, source, context)
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.one(
            FailureDetails.AdapterFailure.InputData(
              "body",
              "dl=docloc".some,
              "no t parameter provided: cannot determine hit type"
            )
          )
        )
      )
  }

  def e3 = {
    val body = "t=unknown&dl=docloc"
    val payload = CollectorPayload(api, Nil, None, body.some, source, context)
    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(
        _ must beInvalid(
          NonEmptyList.of(
            FailureDetails.AdapterFailure
              .InputData("t", "unknown".some, "no matching hit type"),
            FailureDetails.AdapterFailure.SchemaMapping(
              "unknown".some,
              adapterWithDefaultSchemas.unstructEventData.mapValues(_.schemaKey),
              "no schema associated with the provided type parameter"
            )
          )
        )
      )
  }

  def e4 = {
    val body = "t=pageview&dh=host&dp=path"
    val payload = CollectorPayload(api, Nil, None, body.some, source, context)

    val expectedJson =
      """|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
           |"data":{
             |"schema":"iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0",
             |"data":{
               |"documentHostName":"host",
               |"documentPath":"path"
             |}
           |}
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedCO =
      s"""|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
           |"data":[${hitContext("pageview")}]
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedParams = static ++ Map("ue_pr" -> expectedJson, "co" -> expectedCO).toOpt

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beValid(NonEmptyList.one(RawEvent(api, expectedParams, None, source, context))))
  }

  def e5 = {
    val body = "t=pageview&dh=host&cid=id&v=version"
    val payload = CollectorPayload(api, Nil, None, body.some, source, context)

    val expectedUE =
      """|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
           |"data":{
             |"schema":"iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0",
             |"data":{
               |"documentHostName":"host"
             |}
           |}
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedCO =
      s"""|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
           |"data":[${hitContext("pageview")},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/user/jsonschema/1-0-0",
             |"data":{"clientId":"id"}
           |},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/general/jsonschema/1-0-0",
             |"data":{"protocolVersion":"version"}
           |}]
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedParams = static ++ Map("ue_pr" -> expectedUE, "co" -> expectedCO).toOpt

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beValid(NonEmptyList.one(RawEvent(api, expectedParams, None, source, context))))
  }

  def e6 = {
    val body = "t=pageview&dp=path&uip=ip"
    val payload = CollectorPayload(api, Nil, None, body.some, source, context)

    val expectedUE =
      """|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
           |"data":{
             |"schema":"iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0",
             |"data":{"documentPath":"path"}
           |}
         |}""".stripMargin.replaceAll("[\n\r]", "")
    // uip is part of the session context
    val expectedCO =
      s"""|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
           |"data":[${hitContext("pageview")},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/session/jsonschema/1-0-0",
             |"data":{"ipOverride":"ip"}
           |}]
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedParams = static ++ Map("ue_pr" -> expectedUE, "co" -> expectedCO, "ip" -> "ip").toOpt

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beValid(NonEmptyList.one(RawEvent(api, expectedParams, None, source, context))))
  }

  def e7 = {
    val body = "t=item&in=name&ip=12.228&iq=12&aip=0"
    val payload = CollectorPayload(api, Nil, None, body.some, source, context)

    val expectedUE =
      """|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
           |"data":{
             |"schema":"iglu:com.google.analytics.measurement-protocol/item/jsonschema/1-0-0",
             |"data":{
               |"price":12.23,
               |"name":"name",
               |"quantity":12
             |}
           |}
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedCO =
      s"""|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
           |"data":[{
             |"schema":"iglu:com.google.analytics.measurement-protocol/general/jsonschema/1-0-0",
             |"data":{"anonymizeIp":false}
           |},${hitContext("item")}]
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedParams = static ++ Map(
      "ue_pr" -> expectedUE,
      "co" -> expectedCO,
      // ip, iq and in are direct mappings too
      "ti_pr" -> "12.228",
      "ti_qu" -> "12",
      "ti_nm" -> "name"
    ).toOpt

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beValid(NonEmptyList.one(RawEvent(api, expectedParams, None, source, context))))
  }

  def e8 = {
    val body = "t=exception&exd=desc&exf=1&dh=host"
    val payload = CollectorPayload(api, Nil, None, body.some, source, context)

    val expectedUE =
      """|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
           |"data":{
             |"schema":"iglu:com.google.analytics.measurement-protocol/exception/jsonschema/1-0-0",
             |"data":{"description":"desc","isFatal":true}
           |}
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedCO =
      s"""|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
           |"data":[${hitContext("exception")},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0",
             |"data":{"documentHostName":"host"}
           |}]
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedParams = static ++ Map("ue_pr" -> expectedUE, "co" -> expectedCO).toOpt

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beValid(NonEmptyList.one(RawEvent(api, expectedParams, None, source, context))))
  }

  def e9 = {
    val body = "t=transaction&ti=tr&cu=EUR&pr12id=ident&pr12cd42=val"
    val payload = CollectorPayload(api, Nil, None, body.some, source, context)

    val expectedUE =
      """|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
           |"data":{
             |"schema":"iglu:com.google.analytics.measurement-protocol/transaction/jsonschema/1-0-0",
             |"data":{"currencyCode":"EUR","id":"tr"}
           |}
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedCO =
      s"""|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
           |"data":[${hitContext("transaction")},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/product_custom_dimension/jsonschema/1-0-0",
             |"data":{"dimensionIndex":42,"productIndex":12,"value":"val"}
           |},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/product/jsonschema/1-0-0",
             |"data":{"currencyCode":"EUR","sku":"ident","index":12}
           |}]
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedParams = static ++ Map(
      "ue_pr" -> expectedUE,
      "co" -> expectedCO,
      "tr_cu" -> "EUR",
      "tr_id" -> "tr"
    ).toOpt

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beValid(NonEmptyList.one(RawEvent(api, expectedParams, None, source, context))))
  }

  def e10 = {
    val body = "t=pageview&dp=path&il12pi42id=s&il12pi42cd36=dim"
    val payload = CollectorPayload(api, Nil, None, body.some, source, context)

    val expectedUE =
      """|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
           |"data":{
             |"schema":"iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0",
             |"data":{"documentPath":"path"}
           |}
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedCO =
      s"""|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
           |"data":[${hitContext("pageview")},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/product_impression_custom_dimension/jsonschema/1-0-0",
             |"data":{"customDimensionIndex":36,"productIndex":42,"value":"dim","listIndex":12}
           |},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/product_impression/jsonschema/1-0-0",
             |"data":{"productIndex":42,"sku":"s","listIndex":12}
           |}]
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedParams = static ++ Map("ue_pr" -> expectedUE, "co" -> expectedCO).toOpt

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beValid(NonEmptyList.one(RawEvent(api, expectedParams, None, source, context))))
  }

  def e11 = {
    val body = "t=screenview&cd=name&cd12=dim"
    val payload = CollectorPayload(api, Nil, None, body.some, source, context)

    val expectedUE =
      """|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
           |"data":{
             |"schema":"iglu:com.google.analytics.measurement-protocol/screen_view/jsonschema/1-0-0",
             |"data":{"screenName":"name"}
           |}
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedCO =
      s"""|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
           |"data":[${hitContext("screenview")},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/custom_dimension/jsonschema/1-0-0",
             |"data":{"value":"dim","index":12}
           |}]
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedParams = static ++ Map("ue_pr" -> expectedUE, "co" -> expectedCO).toOpt

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beValid(NonEmptyList.one(RawEvent(api, expectedParams, None, source, context))))
  }

  def e12 = {
    val body = "t=pageview&dp=path&pr1id=s1&pr2id=s2&pr1cd1=v1&pr1cd2=v2"
    val payload = CollectorPayload(api, Nil, None, body.some, source, context)

    val expectedUE =
      """|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
           |"data":{
             |"schema":"iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0",
             |"data":{"documentPath":"path"}
           |}
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedCO =
      s"""|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
           |"data":[${hitContext("pageview")},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/product_custom_dimension/jsonschema/1-0-0",
             |"data":{"dimensionIndex":1,"productIndex":1,"value":"v1"}
           |},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/product_custom_dimension/jsonschema/1-0-0",
             |"data":{"dimensionIndex":2,"productIndex":1,"value":"v2"}
           |},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/product/jsonschema/1-0-0",
             |"data":{"sku":"s1","index":1}
           |},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/product/jsonschema/1-0-0",
             |"data":{"sku":"s2","index":2}
           |}]
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedParams = static ++ Map("ue_pr" -> expectedUE, "co" -> expectedCO).toOpt

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beValid(NonEmptyList.one(RawEvent(api, expectedParams, None, source, context))))
  }

  def e13 = {
    val body = "t=pageview&dp=path&promoa=action&promo12id=id"
    val payload = CollectorPayload(api, Nil, None, body.some, source, context)

    val expectedUE =
      """|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
           |"data":{
             |"schema":"iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0",
             |"data":{"documentPath":"path"}
           |}
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedCO =
      s"""|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
           |"data":[${hitContext("pageview")},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/promotion_action/jsonschema/1-0-0",
             |"data":{"promotionAction":"action"}
           |},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/promotion/jsonschema/1-0-0",
             |"data":{"id":"id","index":12}
           |}]
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedParams = static ++ Map("ue_pr" -> expectedUE, "co" -> expectedCO).toOpt

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beValid(NonEmptyList.one(RawEvent(api, expectedParams, None, source, context))))
  }

  def e14 = {
    val body = "t=pageview&dh=host&dp=path\nt=pageview&dh=host&dp=path"
    val payload = CollectorPayload(api, Nil, None, body.some, source, context)

    val expectedJson =
      """|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
           |"data":{
             |"schema":"iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0",
             |"data":{
               |"documentHostName":"host",
               |"documentPath":"path"
             |}
           |}
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedCO =
      s"""|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
           |"data":[${hitContext("pageview")}]
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedParams = static ++ Map("ue_pr" -> expectedJson, "co" -> expectedCO).toOpt
    val event = RawEvent(api, expectedParams, None, source, context)

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beValid(NonEmptyList.of(event, event)))
  }

  def e15 = {
    val body =
      "t=pageview&dh=host&dp=path&cu=EUR&il1pi1pr=1&il1pi1nm=name1&il1pi1ps=1&il1pi1ca=cat1&il1pi1id=id1&il1pi1br=brand1&il1pi2pr=2&il1pi2nm=name2&il1pi2ps=2&il1pi2ca=cat2&il1pi2id=id2&il1pi2br=brand2&il2pi1pr=21&il2pi1nm=name21&il2pi1ps=21&il2pi1ca=cat21&il2pi1id=id21&il2pi1br=brand21"
    val payload = CollectorPayload(api, Nil, None, body.some, source, context)

    val expectedJson =
      """|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
           |"data":{
             |"schema":"iglu:com.google.analytics.measurement-protocol/page_view/jsonschema/1-0-0",
             |"data":{
               |"documentHostName":"host",
               |"documentPath":"path"
             |}
           |}
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedCO =
      s"""|{
           |"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
           |"data":[${hitContext("pageview")},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/product_impression/jsonschema/1-0-0",
             |"data":{"productIndex":1,"name":"name1","sku":"id1","price":1.0,"brand":"brand1","currencyCode":"EUR","category":"cat1","position":1,"listIndex":1}
           |},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/product_impression/jsonschema/1-0-0",
             |"data":{"productIndex":2,"name":"name2","sku":"id2","price":2.0,"brand":"brand2","currencyCode":"EUR","category":"cat2","position":2,"listIndex":1}
           |},{
             |"schema":"iglu:com.google.analytics.measurement-protocol/product_impression/jsonschema/1-0-0",
             |"data":{"productIndex":1,"name":"name21","sku":"id21","price":21.0,"brand":"brand21","currencyCode":"EUR","category":"cat21","position":21,"listIndex":2}
           |}]
         |}""".stripMargin.replaceAll("[\n\r]", "")
    val expectedParams = static ++ Map(
      "ue_pr" -> expectedJson,
      "co" -> expectedCO,
      "ti_cu" -> "EUR"
    ).toOpt

    adapterWithDefaultSchemas
      .toRawEvents(payload, SpecHelpers.client, SpecHelpers.registryLookup, SpecHelpers.DefaultMaxJsonDepth)
      .map(_ must beValid(NonEmptyList.one(RawEvent(api, expectedParams, None, source, context))))
  }

  def e20 = {
    val errorMessage = (s: String) =>
      FailureDetails.AdapterFailure.InputData(
        s,
        None,
        "composite field name has to conform to regex " +
          """(pr|promo|il|cd|cm|cg)(\d+)([a-zA-Z]*)(\d*)([a-zA-Z]*)(\d*)$"""
      )
    val s = Seq(
      adapterWithDefaultSchemas.breakDownCompField("pr") must beLeft(errorMessage("pr")),
      adapterWithDefaultSchemas.breakDownCompField("pr12id") must beRight((List("pr", "id"), List("12"))),
      adapterWithDefaultSchemas.breakDownCompField("12") must beLeft(errorMessage("12")),
      adapterWithDefaultSchemas.breakDownCompField("") must beLeft(
        FailureDetails.AdapterFailure
          .InputData("", None, "cannot parse empty field name")
      ),
      adapterWithDefaultSchemas.breakDownCompField("pr12id", "identifier", "IF") must beRight(
        Map("IFpr" -> "12", "prid" -> "identifier")
      ),
      adapterWithDefaultSchemas.breakDownCompField("pr12cm42", "value", "IF") must beRight(
        Map("IFprcm" -> "12", "IFcm" -> "42", "prcm" -> "value")
      ),
      adapterWithDefaultSchemas.breakDownCompField("pr", "value", "IF") must beLeft(errorMessage("pr")),
      adapterWithDefaultSchemas.breakDownCompField("pr", "", "IF") must beLeft(errorMessage("pr")),
      adapterWithDefaultSchemas.breakDownCompField("pr12", "val", "IF") must beRight(Map("IFpr" -> "12", "pr" -> "val"))
    )
    s.reduce(_ and _)
  }
}
