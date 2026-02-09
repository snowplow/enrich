/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.core

import cats.Id
import cats.effect.{IO, Ref, Resource}
import cats.effect.testkit.TestControl
import cats.effect.testing.specs2.CatsEffect
import io.circe.Json
import io.circe.parser.{parse => parseAsCirce}
import io.circe.literal._
import org.http4s.Uri
import org.http4s.client.{Client => Http4sClient}
import org.http4s.dsl.io._
import org.http4s.syntax.all._
import org.specs2.Specification
import org.specs2.matcher.Matcher

import com.snowplowanalytics.snowplow.runtime.AppInfo
import com.snowplowanalytics.iglu.core.SchemaKey

import scala.concurrent.duration.DurationLong
import java.util.{Base64, UUID}
import java.time.Instant

class MetadataReporterSpec extends Specification with CatsEffect {
  import MetadataReporterSpec._

  def is = s2"""
  The MetadataReporter should:
    Not send any http requests when there are no aggregates $e0
    Send valid metadata events at the correct intervals $e1
    Batch up identical aggregates into a single tracker event $e2
    Batch up non-identical aggregates into a single http request $e3
    Split aggregates into multiple http requests according to the config's maxBodySize $e4
    Attach multiple contexts to an event when an aggregate has multiple contexts $e5
    Correctly json-escape special characters $e6
    Recover from http errors and never crash $e7
  """

  def e0 = {

    val resources = for {
      httpClient <- Resource.eval(mockHttpClient)
      _ <- MetadataReporter.build(testConfig, testAppInfo, httpClient.mock)
    } yield httpClient.results

    val run = resources.use { ref =>
      IO.sleep(1.day) >> ref.get
    }

    val assertions = run.map { results =>
      results must beEmpty
    }

    TestControl.executeEmbed(assertions)
  }

  // A test that sees two metadata events separated by 42 minutes (longer than the reporting period)
  def e1 = {

    val input = {
      val schemaKey = SchemaKey.fromUri("iglu:myvendor/myschema/jsonschema/1-0-0").toOption.get
      val entityKey = SchemaKey.fromUri("iglu:myvendor/mycontext/jsonschema/1-0-8").toOption.get
      val metadataEvent =
        Metadata.MetadataEvent(Some(schemaKey), Some("mysource"), Some("mytracker"), Some("myplatform"), Some("myscenario"))
      val entitiesAndCount = Metadata.EntitiesAndCount(Set(entityKey), 42)
      Map(metadataEvent -> entitiesAndCount)
    }

    def expectedEvent(periodStart: String, periodEnd: String) = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
      "data": {
        "schema": "iglu:com.snowplowanalytics.console/observed_event/jsonschema/6-0-1",
        "data": {
          "organizationId": $testOrganizationId,
          "pipelineId": $testPipelineId,
          "eventVendor": "myvendor",
          "eventName": "myschema",
          "eventVersion": "1-0-0",
          "source": "mysource",
          "tracker": "mytracker",
          "platform": "myplatform",
          "scenario_id": "myscenario",
          "eventVolume": 42,
          "periodStart": $periodStart,
          "periodEnd": $periodEnd
        }
      }
    }
    """

    val expectedContext = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
      "data": [{
        "schema": "iglu:com.snowplowanalytics.console/observed_entity/jsonschema/4-0-0",
        "data": {
          "entityVendor": "myvendor",
          "entityName": "mycontext",
          "entityVersion": "1-0-8"
        }
      }]
    }
    """

    val resources = for {
      httpClient <- Resource.eval(mockHttpClient)
      reporter <- MetadataReporter.build(testConfig, testAppInfo, httpClient.mock)
    } yield (reporter, httpClient.results)

    val run = resources.use {
      case (reporter, ref) =>
        for {
          _ <- IO.sleep(42.minutes)
          _ <- reporter.add(input)
          _ <- IO.sleep(42.minutes)
          _ <- reporter.add(input)
          _ <- IO.sleep(1.day)
          results <- ref.get
        } yield results
    }

    val assertions = run.map {
      case Vector(httpRequest1, httpRequest2) =>
        val t1 = httpRequest1 must (
          List(
            haveCorrectUri,
            haveTrackedEventCount(1),
            beSentAt(Instant.parse("1970-01-01T00:45:00.00Z")),
            containTrackedEvent(expectedEvent(periodStart = "1970-01-01T00:40:00Z", periodEnd = "1970-01-01T00:45:00Z"), expectedContext)
          ).reduce(_ and _)
        )
        val t2 = httpRequest2 must (
          List(
            haveCorrectUri,
            haveTrackedEventCount(1),
            beSentAt(Instant.parse("1970-01-01T01:25:00.00Z")),
            containTrackedEvent(expectedEvent(periodStart = "1970-01-01T01:20:00Z", periodEnd = "1970-01-01T01:25:00Z"), expectedContext)
          ).reduce(_ and _)
        )

        t1 and t2
      case other => ko(s"Expected 2 http request, got ${other.size} http requests")
    }

    TestControl.executeEmbed(assertions)
  }

  // A test that sees two identical metadata events separated by 42 **seconds** (less than the reporting period)
  def e2 = {

    val inputEventVolume = 42
    val expectedEventVolume = 42 * 2 // Because this test add the same aggregate twice

    val input = {
      val schemaKey = SchemaKey.fromUri("iglu:myvendor/myschema/jsonschema/1-0-0").toOption.get
      val entityKey = SchemaKey.fromUri("iglu:myvendor/mycontext/jsonschema/1-0-8").toOption.get
      val metadataEvent =
        Metadata.MetadataEvent(Some(schemaKey), Some("mysource"), Some("mytracker"), Some("myplatform"), Some("myscenario"))
      val entitiesAndCount = Metadata.EntitiesAndCount(Set(entityKey), inputEventVolume)
      Map(metadataEvent -> entitiesAndCount)
    }

    val expectedEvent = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
      "data": {
        "schema": "iglu:com.snowplowanalytics.console/observed_event/jsonschema/6-0-1",
        "data": {
          "organizationId": $testOrganizationId,
          "pipelineId": $testPipelineId,
          "eventVendor": "myvendor",
          "eventName": "myschema",
          "eventVersion": "1-0-0",
          "source": "mysource",
          "tracker": "mytracker",
          "platform": "myplatform",
          "scenario_id": "myscenario",
          "eventVolume": $expectedEventVolume,
          "periodStart": "1970-01-01T00:40:00Z",
          "periodEnd": "1970-01-01T00:45:00Z"
        }
      }
    }
    """

    val expectedContext = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
      "data": [{
        "schema": "iglu:com.snowplowanalytics.console/observed_entity/jsonschema/4-0-0",
        "data": {
          "entityVendor": "myvendor",
          "entityName": "mycontext",
          "entityVersion": "1-0-8"
        }
      }]
    }
    """

    val resources = for {
      httpClient <- Resource.eval(mockHttpClient)
      reporter <- MetadataReporter.build(testConfig, testAppInfo, httpClient.mock)
    } yield (reporter, httpClient.results)

    val run = resources.use {
      case (reporter, ref) =>
        for {
          _ <- IO.sleep(42.minutes)
          _ <- reporter.add(input)
          _ <- IO.sleep(42.seconds)
          _ <- reporter.add(input)
          _ <- IO.sleep(1.day)
          results <- ref.get
        } yield results
    }

    val assertions = run.map {
      case Vector(httpRequest1) =>
        httpRequest1 must List(
          haveCorrectUri,
          haveTrackedEventCount(1),
          beSentAt(Instant.parse("1970-01-01T00:45:00.00Z")),
          containTrackedEvent(expectedEvent, expectedContext)
        ).reduce(_ and _)
      case other => ko(s"Expected 1 http request, got ${other.size} http requests")
    }

    TestControl.executeEmbed(assertions)
  }

  // A test that sees two non-identical metadata events separated by 42 **seconds** (less than the reporting period)
  def e3 = {

    def input(appId: String) = {
      val schemaKey = SchemaKey.fromUri("iglu:myvendor/myschema/jsonschema/1-0-0").toOption.get
      val entityKey = SchemaKey.fromUri("iglu:myvendor/mycontext/jsonschema/1-0-8").toOption.get
      val metadataEvent =
        Metadata.MetadataEvent(Some(schemaKey), Some(appId), Some("mytracker"), Some("myplatform"), Some("myscenario"))
      val entitiesAndCount = Metadata.EntitiesAndCount(Set(entityKey), 42)
      Map(metadataEvent -> entitiesAndCount)
    }

    def expectedEvent(appId: String) = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
      "data": {
        "schema": "iglu:com.snowplowanalytics.console/observed_event/jsonschema/6-0-1",
        "data": {
          "organizationId": $testOrganizationId,
          "pipelineId": $testPipelineId,
          "eventVendor": "myvendor",
          "eventName": "myschema",
          "eventVersion": "1-0-0",
          "source": $appId,
          "tracker": "mytracker",
          "platform": "myplatform",
          "scenario_id": "myscenario",
          "eventVolume": 42,
          "periodStart": "1970-01-01T00:40:00Z",
          "periodEnd": "1970-01-01T00:45:00Z"
        }
      }
    }
    """

    val expectedContext = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
      "data": [{
        "schema": "iglu:com.snowplowanalytics.console/observed_entity/jsonschema/4-0-0",
        "data": {
          "entityVendor": "myvendor",
          "entityName": "mycontext",
          "entityVersion": "1-0-8"
        }
      }]
    }
    """

    val resources = for {
      httpClient <- Resource.eval(mockHttpClient)
      reporter <- MetadataReporter.build(testConfig, testAppInfo, httpClient.mock)
    } yield (reporter, httpClient.results)

    val run = resources.use {
      case (reporter, ref) =>
        for {
          _ <- IO.sleep(42.minutes)
          _ <- reporter.add(input("myapp1"))
          _ <- IO.sleep(42.seconds)
          _ <- reporter.add(input("myapp2"))
          _ <- IO.sleep(1.day)
          results <- ref.get
        } yield results
    }

    val assertions = run.map {
      case Vector(httpRequest1) =>
        httpRequest1 must List(
          haveCorrectUri,
          haveTrackedEventCount(2),
          beSentAt(Instant.parse("1970-01-01T00:45:00.00Z")),
          containTrackedEvent(expectedEvent(appId = "myapp1"), expectedContext),
          containTrackedEvent(expectedEvent(appId = "myapp2"), expectedContext)
        ).reduce(_ and _)
      case other => ko(s"Expected 1 http request, got ${other.size} http requests")
    }

    TestControl.executeEmbed(assertions)
  }

  def e4 = {

    val input = {
      val schemaKey = SchemaKey.fromUri("iglu:myvendor/myschema/jsonschema/1-0-0").toOption.get
      val entityKey = SchemaKey.fromUri("iglu:myvendor/mycontext/jsonschema/1-0-8").toOption.get
      def metadataEvent(appId: String) =
        Metadata.MetadataEvent(Some(schemaKey), Some(appId), Some("mytracker"), Some("myplatform"), Some("myscenario"))
      val entitiesAndCount = Metadata.EntitiesAndCount(Set(entityKey), 42)
      Map(
        metadataEvent("mysource1") -> entitiesAndCount,
        metadataEvent("mysource2") -> entitiesAndCount,
        metadataEvent("mysource3") -> entitiesAndCount
      )
    }

    val thisTestConfig = testConfig.copy(maxBodySize = 50)

    def expectedEvent(appId: String) = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
      "data": {
        "schema": "iglu:com.snowplowanalytics.console/observed_event/jsonschema/6-0-1",
        "data": {
          "organizationId": $testOrganizationId,
          "pipelineId": $testPipelineId,
          "eventVendor": "myvendor",
          "eventName": "myschema",
          "eventVersion": "1-0-0",
          "source": $appId,
          "tracker": "mytracker",
          "platform": "myplatform",
          "scenario_id": "myscenario",
          "eventVolume": 42,
          "periodStart": "1970-01-01T00:40:00Z",
          "periodEnd": "1970-01-01T00:45:00Z"
        }
      }
    }
    """

    val expectedContext = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
      "data": [{
        "schema": "iglu:com.snowplowanalytics.console/observed_entity/jsonschema/4-0-0",
        "data": {
          "entityVendor": "myvendor",
          "entityName": "mycontext",
          "entityVersion": "1-0-8"
        }
      }]
    }
    """

    val resources = for {
      httpClient <- Resource.eval(mockHttpClient)
      reporter <- MetadataReporter.build(thisTestConfig, testAppInfo, httpClient.mock)
    } yield (reporter, httpClient.results)

    val run = resources.use {
      case (reporter, ref) =>
        for {
          _ <- IO.sleep(42.minutes)
          _ <- reporter.add(input)
          _ <- IO.sleep(1.day)
          results <- ref.get
        } yield results
    }

    val assertions = run.map {
      case Vector(httpRequest1, httpRequest2, httpRequest3) =>
        val t1 = httpRequest1 must (
          List(
            haveCorrectUri,
            haveTrackedEventCount(1),
            beSentAt(Instant.parse("1970-01-01T00:45:00.00Z")),
            containTrackedEvent(expectedEvent("mysource1"), expectedContext)
          ).reduce(_ and _)
        )
        val t2 = httpRequest2 must (
          List(
            haveCorrectUri,
            haveTrackedEventCount(1),
            beSentAt(Instant.parse("1970-01-01T00:45:00.00Z")),
            containTrackedEvent(expectedEvent("mysource2"), expectedContext)
          ).reduce(_ and _)
        )
        val t3 = httpRequest3 must (
          List(
            haveCorrectUri,
            haveTrackedEventCount(1),
            beSentAt(Instant.parse("1970-01-01T00:45:00.00Z")),
            containTrackedEvent(expectedEvent("mysource3"), expectedContext)
          ).reduce(_ and _)
        )

        t1 and t2 and t3
      case other => ko(s"Expected 3 http request, got ${other.size} http requests")
    }

    TestControl.executeEmbed(assertions)
  }

  def e5 = {

    def input(appId: String) = {
      val schemaKey = SchemaKey.fromUri("iglu:myvendor/myschema/jsonschema/1-0-0").toOption.get
      val entityKey1 = SchemaKey.fromUri("iglu:myvendor/mycontext1/jsonschema/2-0-0").toOption.get
      val entityKey2 = SchemaKey.fromUri("iglu:myvendor/mycontext2/jsonschema/1-0-8").toOption.get
      val metadataEvent =
        Metadata.MetadataEvent(Some(schemaKey), Some(appId), Some("mytracker"), Some("myplatform"), Some("myscenario"))
      val entitiesAndCount = Metadata.EntitiesAndCount(Set(entityKey1, entityKey2), 42)
      Map(metadataEvent -> entitiesAndCount)
    }

    def expectedEvent(appId: String) = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
      "data": {
        "schema": "iglu:com.snowplowanalytics.console/observed_event/jsonschema/6-0-1",
        "data": {
          "organizationId": $testOrganizationId,
          "pipelineId": $testPipelineId,
          "eventVendor": "myvendor",
          "eventName": "myschema",
          "eventVersion": "1-0-0",
          "source": $appId,
          "tracker": "mytracker",
          "platform": "myplatform",
          "scenario_id": "myscenario",
          "eventVolume": 42,
          "periodStart": "1970-01-01T00:40:00Z",
          "periodEnd": "1970-01-01T00:45:00Z"
        }
      }
    }
    """

    val expectedContext = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
      "data": [
        {
          "schema": "iglu:com.snowplowanalytics.console/observed_entity/jsonschema/4-0-0",
          "data": {
            "entityVendor": "myvendor",
            "entityName": "mycontext1",
            "entityVersion": "2-0-0"
          }
        },
        {
          "schema": "iglu:com.snowplowanalytics.console/observed_entity/jsonschema/4-0-0",
          "data": {
            "entityVendor": "myvendor",
            "entityName": "mycontext2",
            "entityVersion": "1-0-8"
          }
        }
      ]
    }
    """

    val resources = for {
      httpClient <- Resource.eval(mockHttpClient)
      reporter <- MetadataReporter.build(testConfig, testAppInfo, httpClient.mock)
    } yield (reporter, httpClient.results)

    val run = resources.use {
      case (reporter, ref) =>
        for {
          _ <- IO.sleep(42.minutes)
          _ <- reporter.add(input("myapp1"))
          _ <- IO.sleep(42.seconds)
          _ <- reporter.add(input("myapp2"))
          _ <- IO.sleep(1.day)
          results <- ref.get
        } yield results
    }

    val assertions = run.map {
      case Vector(httpRequest1) =>
        httpRequest1 must List(
          haveCorrectUri,
          haveTrackedEventCount(2),
          beSentAt(Instant.parse("1970-01-01T00:45:00.00Z")),
          containTrackedEvent(expectedEvent(appId = "myapp1"), expectedContext),
          containTrackedEvent(expectedEvent(appId = "myapp2"), expectedContext)
        ).reduce(_ and _)
      case other => ko(s"Expected 1 http request, got ${other.size} http requests")
    }

    TestControl.executeEmbed(assertions)
  }

  def e6 = {

    val input = {
      val schemaKey = SchemaKey.fromUri("iglu:myvendor/myschema/jsonschema/1-0-0").toOption.get
      val entityKey = SchemaKey.fromUri("iglu:myvendor/mycontext/jsonschema/1-0-8").toOption.get
      val metadataEvent =
        Metadata.MetadataEvent(Some(schemaKey),
                               Some("app{,\"}:;[']"),
                               Some("tracker{,\"}:;[']"),
                               Some("myplatform"),
                               Some("scenario{,\"}:;[']")
        )
      val entitiesAndCount = Metadata.EntitiesAndCount(Set(entityKey), 42)
      Map(metadataEvent -> entitiesAndCount)
    }

    val expectedEvent = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
      "data": {
        "schema": "iglu:com.snowplowanalytics.console/observed_event/jsonschema/6-0-1",
        "data": {
          "organizationId": $testOrganizationId,
          "pipelineId": $testPipelineId,
          "eventVendor": "myvendor",
          "eventName": "myschema",
          "eventVersion": "1-0-0",
          "source": "app{,\"}:;[']",
          "tracker": "tracker{,\"}:;[']",
          "platform": "myplatform",
          "scenario_id": "scenario{,\"}:;[']",
          "eventVolume": 42,
          "periodStart": "1970-01-01T00:40:00Z",
          "periodEnd": "1970-01-01T00:45:00Z"
        }
      }
    }
    """

    val expectedContext = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1",
      "data": [{
        "schema": "iglu:com.snowplowanalytics.console/observed_entity/jsonschema/4-0-0",
        "data": {
          "entityVendor": "myvendor",
          "entityName": "mycontext",
          "entityVersion": "1-0-8"
        }
      }]
    }
    """

    val resources = for {
      httpClient <- Resource.eval(mockHttpClient)
      reporter <- MetadataReporter.build(testConfig, testAppInfo, httpClient.mock)
    } yield (reporter, httpClient.results)

    val run = resources.use {
      case (reporter, ref) =>
        for {
          _ <- IO.sleep(42.minutes)
          _ <- reporter.add(input)
          _ <- IO.sleep(1.day)
          results <- ref.get
        } yield results
    }

    val assertions = run.map {
      case Vector(httpRequest1) =>
        httpRequest1 must (
          List(
            haveCorrectUri,
            haveTrackedEventCount(1),
            beSentAt(Instant.parse("1970-01-01T00:45:00.00Z")),
            containTrackedEvent(expectedEvent, expectedContext)
          ).reduce(_ and _)
        )
      case other => ko(s"Expected 1 http request, got ${other.size} http requests")
    }

    TestControl.executeEmbed(assertions)
  }

  def e7 = {

    // An http client that returns 500s on the first day and 200s thereafter
    def badHttpClient(delegate: Http4sClient[IO]): Http4sClient[IO] =
      Http4sClient { request =>
        Resource.suspend {
          IO.realTimeInstant.map { now =>
            if (now.isBefore(Instant.parse("1970-01-02T00:00:00.00Z")))
              Resource.eval(InternalServerError("boom!"))
            else
              delegate.run(request)
          }
        }
      }

    val resources = for {
      httpClient <- Resource.eval(mockHttpClient)
      reporter <- MetadataReporter.build(testConfig, testAppInfo, badHttpClient(httpClient.mock))
    } yield (reporter, httpClient.results)

    val input = {
      val schemaKey = SchemaKey.fromUri("iglu:myvendor/myschema/jsonschema/1-0-0").toOption.get
      val entityKey = SchemaKey.fromUri("iglu:myvendor/mycontext/jsonschema/1-0-8").toOption.get
      val metadataEvent =
        Metadata.MetadataEvent(Some(schemaKey), Some("mysource"), Some("mytracker"), Some("myplatform"), Some("myscenario"))
      val entitiesAndCount = Metadata.EntitiesAndCount(Set(entityKey), 42)
      Map(metadataEvent -> entitiesAndCount)
    }

    val run = resources.use {
      case (reporter, ref) =>
        for {
          _ <- IO.sleep(42.minutes)
          _ <- reporter.add(input)
          _ <- IO.sleep(42.minutes)
          _ <- reporter.add(input)
          _ <- IO.sleep(1.day)
          _ <- reporter.add(input)
          _ <- IO.sleep(1.day)
          results <- ref.get
        } yield results
    }

    val test = run.map {
      case Vector(httpRequest1) =>
        httpRequest1 must (
          List(
            haveCorrectUri,
            haveTrackedEventCount(1),
            beSentAt(Instant.parse("1970-01-02T01:25:00.00Z"))
          ).reduce(_ and _)
        )
      case other => ko(s"Expected 1 http request, got ${other.size} http requests")
    }

    TestControl.executeEmbed(test)
  }

  /** Custom matchers */

  def haveCorrectUri: Matcher[MockHttpClientResult] = { (result: MockHttpClientResult) =>
    result.uri.toString must beEqualTo("http://mycollector/com.snowplowanalytics.snowplow/tp2")
  }

  def haveTrackedEventCount(expected: Int): Matcher[MockHttpClientResult] = { (result: MockHttpClientResult) =>
    parseAsCirce(result.body) must beRight.like {
      case json: Json =>
        json.hcursor.downField("data").values must beSome.like {
          case items: Iterable[Json] =>
            items must haveSize(expected)
        }
    }
  }

  def beSentAt(expectedTimestamp: Instant): Matcher[MockHttpClientResult] = { (result: MockHttpClientResult) =>
    result.sentTstamp must beEqualTo(expectedTimestamp)
  }

  def containTrackedEvent(expectedEvent: Json, expectedContext: Json): Matcher[MockHttpClientResult] = { (result: MockHttpClientResult) =>
    def matchUePx: Matcher[Json] = { (json: Json) =>
      json.hcursor.downField("ue_px").as[String] must beRight.like {
        case b64: String =>
          val bytes = Base64.getDecoder().decode(b64)
          parseAsCirce(new String(bytes)) must beRight(expectedEvent)
      }
    }

    def matchCx: Matcher[Json] = { (json: Json) =>
      json.hcursor.downField("cx").as[String] must beRight.like {
        case b64: String =>
          val bytes = Base64.getDecoder().decode(b64)
          parseAsCirce(new String(bytes)) must beRight(expectedContext)
      }
    }

    parseAsCirce(result.body) must beRight.like {
      case json: Json =>
        json.hcursor.downField("data").values must beSome.like {
          case trackedEvents: Iterable[Json] =>
            trackedEvents must contain(matchUePx and matchCx)
        }
    }
  }

}

object MetadataReporterSpec {

  val testOrganizationId = UUID.randomUUID
  val testPipelineId = UUID.randomUUID

  val testAppInfo = new AppInfo {
    def name = "enrich-test"
    def version = "0.0.0"
    def dockerAlias = "snowplow/enrich-test:0.0.0"
    def cloud = "OnPrem"
  }

  val testConfig: Config.Metadata = Config.MetadataM[Id](
    endpoint = uri"http://mycollector",
    organizationId = testOrganizationId,
    pipelineId = testPipelineId,
    interval = 5.minutes,
    maxBodySize = 10000
  )

  /** Holds details of the http requests received by our mock http client */
  case class MockHttpClientResult(
    sentTstamp: Instant,
    uri: Uri,
    body: String
  )

  def mockHttpClient: IO[MockHttpClient] =
    Ref[IO].of(Vector.empty[MockHttpClientResult]).map(new MockHttpClient(_))

  class MockHttpClient(val results: Ref[IO, Vector[MockHttpClientResult]]) {

    def mock: Http4sClient[IO] =
      Http4sClient { req =>
        Resource.eval {
          for {
            now <- IO.realTimeInstant
            body <- req.bodyText.compile.string
            _ <- results.update(_ :+ MockHttpClientResult(now, req.uri, body))
            response <- Ok("ok")
          } yield response
        }
      }
  }

}
