/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.io.experimental

import cats.Id
import cats.data.NonEmptyList
import cats.effect.{IO, Ref, Resource}
import cats.effect.testing.specs2.CatsEffect
import io.circe.literal._
import fs2.Stream
import org.http4s.{Request, Uri}
import org.http4s.circe.jsonEncoder
import org.http4s.client.{Client => HttpClient}
import org.http4s.dsl.io._
import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.badrows.{
  BadRow,
  Failure => BadRowFailure,
  FailureDetails,
  Payload => BadRowPayload,
  Processor => BadRowProcessor
}
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.fs2.Enrich
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{BackoffPolicy, IdentityM => IdentityConfig}
import com.snowplowanalytics.snowplow.enrich.common.utils.OptionIor

import java.util.UUID
import java.time.Instant
import scala.concurrent.duration.DurationInt

class IdentitySpec extends Specification with CatsEffect {
  import IdentitySpec._

  "Identity" should {

    "Add identity contexts to events" in {

      val e0 = new EnrichedEvent()
      e0.event_id = testIdPairs(0).eventId

      val e1 = new EnrichedEvent()
      e1.event_id = testIdPairs(1).eventId

      val e2 = new EnrichedEvent()
      e2.event_id = testIdPairs(2).eventId

      val e3 = new EnrichedEvent()
      e3.event_id = testIdPairs(3).eventId

      // An input that contains the two events
      val input1 = Enrich.Result[Unit](
        original = (),
        enriched = List(OptionIor.Right(e0), OptionIor.Right(e1)),
        collectorTstamp = Some(42L)
      )

      // An input that contains the other two events
      val input2 = Enrich.Result[Unit](
        original = (),
        enriched = List(OptionIor.Right(e2), OptionIor.Right(e3)),
        collectorTstamp = Some(42L)
      )

      val stream = Stream
        .emit(List(input1, input2))
        .covary[IO]
        .through(Identity.pipe[IO, Unit](testConfig, testHttpClient))

      val expectedContext0 = json"""
      {
        "snowplowId": ${testIdPairs(0).snowplowId},
        "createdAt": "2023-03-03T03:03:03.333Z",
        "merged": [$testMergedIdentity]
      }
      """

      val expectedContext1 = json"""
      {
        "snowplowId": ${testIdPairs(1).snowplowId},
        "createdAt": "2023-03-03T03:03:03.333Z",
        "merged": [$testMergedIdentity]
      }
      """

      val expectedContext2 = json"""
      {
        "snowplowId": ${testIdPairs(2).snowplowId},
        "createdAt": "2023-03-03T03:03:03.333Z",
        "merged": [$testMergedIdentity]
      }
      """

      val expectedContext3 = json"""
      {
        "snowplowId": ${testIdPairs(3).snowplowId},
        "createdAt": "2023-03-03T03:03:03.333Z",
        "merged": [$testMergedIdentity]
      }
      """

      val expectedKey = SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/identity/jsonschema/1-0-0").toOption.get

      for {
        outputs <- stream.compile.toList
      } yield outputs must beLike {
        case List(List(outputs1, outputs2)) =>
          val o1 = outputs1 must beLike {
            case Enrich.Result((), List(OptionIor.Right(output0), OptionIor.Right(output1)), Some(42L)) =>
              val t0 = output0.derived_contexts must beLike {
                case List(SelfDescribingData(key, json)) =>
                  (json.noSpaces must beEqualTo(expectedContext0.noSpaces)) and (key must beEqualTo(expectedKey))
              }
              val t1 = output1.derived_contexts must beLike {
                case List(SelfDescribingData(key, json)) =>
                  (json.noSpaces must beEqualTo(expectedContext1.noSpaces)) and (key must beEqualTo(expectedKey))
              }
              t0 and t1
          }
          val o2 = outputs2 must beLike {
            case Enrich.Result((), List(OptionIor.Right(output2), OptionIor.Right(output3)), Some(42L)) =>
              val t2 = output2.derived_contexts must beLike {
                case List(SelfDescribingData(key, json)) =>
                  (json.noSpaces must beEqualTo(expectedContext2.noSpaces)) and (key must beEqualTo(expectedKey))
              }
              val t3 = output3.derived_contexts must beLike {
                case List(SelfDescribingData(key, json)) =>
                  (json.noSpaces must beEqualTo(expectedContext3.noSpaces)) and (key must beEqualTo(expectedKey))
              }
              t2 and t3
          }
          o1 and o2
      }
    }

    "add identity contexts to incomplete events" in {
      val good = new EnrichedEvent()
      good.event_id = testIdPairs(0).eventId

      val incomplete = new EnrichedEvent()
      incomplete.event_id = testIdPairs(1).eventId

      val bad = {
        val processor = BadRowProcessor("foo", "bar")
        val rawEvent = BadRowPayload.RawEvent("a", "b", Nil, None, "c", "d", None, None, None, None, None, Nil, None)
        val payload = BadRowPayload.EnrichmentPayload(EnrichedEvent.toPartiallyEnrichedEvent(incomplete), rawEvent)
        val detail = FailureDetails.SchemaViolation.NotJson("some-field", None, "boom!")
        val failure = BadRowFailure.SchemaViolations(Instant.now(), NonEmptyList.one(detail))
        BadRow.SchemaViolations(processor, failure, payload)
      }

      val input = Enrich.Result[Unit](
        original = (),
        enriched = List(
          // Note, 1 good, 1 incomplete
          OptionIor.Right(good),
          OptionIor.Both(bad, incomplete)
        ),
        collectorTstamp = Some(42L)
      )

      val expectedGoodContext = json"""
      {
        "snowplowId": ${testIdPairs(0).snowplowId},
        "createdAt": "2023-03-03T03:03:03.333Z",
        "merged": [$testMergedIdentity]
      }
      """

      val expectedIncompleteContext = json"""
      {
        "snowplowId": ${testIdPairs(1).snowplowId},
        "createdAt": "2023-03-03T03:03:03.333Z",
        "merged": [$testMergedIdentity]
      }
      """

      val expectedKey = SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/identity/jsonschema/1-0-0").toOption.get

      val stream = Stream
        .emit(List(input))
        .covary[IO]
        .through(Identity.pipe[IO, Unit](testConfig, testHttpClient))

      for {
        outputs <- stream.compile.toList
      } yield outputs must beLike {
        case List(List(Enrich.Result((), List(result1, result2), Some(42L)))) =>
          val test1 = result1 must beLike {
            case OptionIor.Right(e) =>
              val t1 = e.event_id must beEqualTo(good.event_id)
              val t2 = e.derived_contexts must beLike {
                case List(SelfDescribingData(key, json)) =>
                  (json.noSpaces must beEqualTo(expectedGoodContext.noSpaces)) and (key must beEqualTo(expectedKey))
              }
              t1 and t2
          }
          val test2 = result2 must beLike {
            case OptionIor.Both(b, e) =>
              val t1 = b must beEqualTo(bad)
              val t2 = e.event_id must beEqualTo(incomplete.event_id)
              val t3 = e.derived_contexts must beLike {
                case List(SelfDescribingData(key, json)) =>
                  (json.noSpaces must beEqualTo(expectedIncompleteContext.noSpaces)) and (key must beEqualTo(expectedKey))
              }
              t1 and t2 and t3
          }

          test1 and test2
      }
    }

    "preserve bad rows without any change" in {
      val good = new EnrichedEvent()
      good.event_id = testIdPairs(0).eventId

      val bad = {
        val processor = BadRowProcessor("foo", "bar")
        val rawEvent = BadRowPayload.RawEvent("a", "b", Nil, None, "c", "d", None, None, None, None, None, Nil, None)
        val payload = BadRowPayload.EnrichmentPayload(EnrichedEvent.toPartiallyEnrichedEvent(good), rawEvent)
        val detail = FailureDetails.SchemaViolation.NotJson("some-field", None, "boom!")
        val failure = BadRowFailure.SchemaViolations(Instant.now(), NonEmptyList.one(detail))
        BadRow.SchemaViolations(processor, failure, payload)
      }

      val input = Enrich.Result[Unit](
        original = (),
        enriched = List(
          // Note, 1 good, 1 bad
          OptionIor.Right(good),
          OptionIor.Left(bad)
        ),
        collectorTstamp = Some(42L)
      )

      val stream = Stream
        .emit(List(input))
        .covary[IO]
        .through(Identity.pipe[IO, Unit](testConfig, testHttpClient))

      for {
        outputs <- stream.compile.toList
      } yield outputs must beLike {
        case List(List(Enrich.Result((), List(result1, result2), Some(42L)))) =>
          val test1 = result1 must beLike {
            case OptionIor.Right(e) =>
              (e.event_id must beEqualTo(good.event_id)) and (e.derived_contexts must haveLength(1))
          }
          val test2 = result2 must beLike {
            case OptionIor.Left(b) =>
              b must beEqualTo(bad)
          }

          test1 and test2
      }
    }

    "send valid requests to the identity api" in {

      val e = new EnrichedEvent()
      e.event_id = UUID.randomUUID.toString
      e.domain_userid = "test-domain-user-id"
      e.network_userid = "test-network-user-id"
      e.user_id = "test-user-id"

      val inputs = Enrich.Result[Unit](
        original = (),
        enriched = List(OptionIor.Right(e)),
        collectorTstamp = Some(42L)
      )

      val expectedRequestBody = json"""
        [
          {
            "eventId": ${e.event_id},
            "identifiers": {
              "domain_userid": ${e.domain_userid},
              "network_userid": ${e.network_userid},
              "user_id": ${e.user_id}
            }
          }
        ]
      """

      for {
        ref <- Ref[IO].of(List.empty[(Request[IO], String)])
        stream = Stream
                   .emit(List(inputs))
                   .covary[IO]
                   .through(Identity.pipe[IO, Unit](testConfig, new RecordingHttpClient(ref).mock))
        _ <- stream.compile.drain
        result <- ref.get
      } yield result must beLike {
        case List((request, body)) =>
          val expectedHeader1 = "Content-Type: application/x.snowplow.v1+json"
          val expectedHeader2 = "Accept: application/x.snowplow.v1+json"
          List(
            request.uri.toString must beEqualTo("http://test-server:8787/identities/batch"),
            request.method.toString must beEqualTo("POST"),
            request.headers.headers.map(_.toString) must contain(expectedHeader1, expectedHeader2),
            request.headers.headers.map(_.toString) must contain(beMatching("^Authorization: Basic .+")),
            body must beEqualTo(expectedRequestBody.noSpaces)
          ).reduce(_ and _)
      }
    }

    "omit id fields from the request body when they are null" in {

      // unset network_userid and user_id
      val e = new EnrichedEvent()
      e.event_id = UUID.randomUUID.toString
      e.domain_userid = "test-domain-user-id"

      val inputs = Enrich.Result[Unit](
        original = (),
        enriched = List(OptionIor.Right(e)),
        collectorTstamp = Some(42L)
      )

      val expectedRequestBody = json"""
        [
          {
            "eventId": ${e.event_id},
            "identifiers": {
              "domain_userid": ${e.domain_userid}
            }
          }
        ]
      """

      for {
        ref <- Ref[IO].of(List.empty[(Request[IO], String)])
        stream = Stream
                   .emit(List(inputs))
                   .covary[IO]
                   .through(Identity.pipe[IO, Unit](testConfig, new RecordingHttpClient(ref).mock))
        _ <- stream.compile.drain
        result <- ref.get
      } yield result must beLike {
        case List((_, body)) =>
          body must beEqualTo(expectedRequestBody.noSpaces)
      }
    }
  }
}

object IdentitySpec {
  case class IdPair(eventId: String, snowplowId: String)

  val testIdPairs = List(
    IdPair(UUID.randomUUID.toString, UUID.randomUUID.toString),
    IdPair(UUID.randomUUID.toString, UUID.randomUUID.toString),
    IdPair(UUID.randomUUID.toString, UUID.randomUUID.toString),
    IdPair(UUID.randomUUID.toString, UUID.randomUUID.toString)
  )

  val testMergedIdentity = json"""
    {
      "mergedAt": "2021-01-01T01:01:01.111Z",
      "createdAt": "2022-02-02T02:02:02.222Z",
      "snowplowId": "21a443d6-ca28-4ac1-9440-46f90263e5bd"
    }
  """

  val mockResponseBody = json"""
  [
    {
      "createdAt": "2023-03-03T03:03:03.333Z",
      "eventId": ${testIdPairs(0).eventId},
      "snowplowId": ${testIdPairs(0).snowplowId},
      "merged": [$testMergedIdentity]
    },
    {
      "createdAt": "2023-03-03T03:03:03.333Z",
      "eventId": ${testIdPairs(1).eventId},
      "snowplowId": ${testIdPairs(1).snowplowId},
      "merged": [$testMergedIdentity]
    },
    {
      "createdAt": "2023-03-03T03:03:03.333Z",
      "eventId": ${testIdPairs(2).eventId},
      "snowplowId": ${testIdPairs(2).snowplowId},
      "merged": [$testMergedIdentity]
    },
    {
      "createdAt": "2023-03-03T03:03:03.333Z",
      "eventId": ${testIdPairs(3).eventId},
      "snowplowId": ${testIdPairs(3).snowplowId},
      "merged": [$testMergedIdentity]
    }
  ]
  """

  val testHttpClient: HttpClient[IO] = HttpClient[IO] { _ =>
    Resource.eval(Created(mockResponseBody))
  }

  // A mock Client[F] that records the requests it receives
  class RecordingHttpClient(ref: Ref[IO, List[(Request[IO], String)]]) {
    def mock: HttpClient[IO] =
      HttpClient[IO] { req =>
        Resource.eval {
          for {
            body <- req.bodyText.compile.string
            _ <- ref.update(_ :+ ((req, body)))
            response <- Created(mockResponseBody)
          } yield response
        }
      }
  }

  val testConfig = IdentityConfig[Id](Uri.unsafeFromString("http://test-server:8787"),
                                      "test-user",
                                      "test-password",
                                      1,
                                      BackoffPolicy(100.milli, 10.second, Some(1))
  )

}
