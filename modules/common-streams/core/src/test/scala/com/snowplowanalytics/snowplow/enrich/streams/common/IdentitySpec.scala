/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.streams.common

import cats.Id
import cats.effect.{IO, Ref, Resource}
import cats.effect.testing.specs2.CatsEffect
import io.circe.literal._
import org.http4s.{Request, Uri}
import org.specs2.Specification
import org.http4s.circe.jsonEncoder
import org.http4s.client.{Client => Http4sClient}
import org.http4s.dsl.io._

import com.snowplowanalytics.snowplow.runtime.Retrying
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

import java.util.UUID
import scala.concurrent.duration.DurationInt

class IdentitySpec extends Specification with CatsEffect {
  import IdentitySpec._

  def is = s2"""
  Identity should 
    add contexts to enriched events $e1
    send valid requests to the identity api $e2
    omit id fields from the request body when they are null $e3
  """

  def e1 = {
    val e1 = new EnrichedEvent()
    e1.event_id = testIdPairs(1).eventId

    val e2 = new EnrichedEvent()
    e2.event_id = testIdPairs(2).eventId

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
    val expectedKey = SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/identity/jsonschema/1-0-0").toOption.get

    val api = Identity.build(testConfig, testHttpClient)

    for {
      _ <- Identity.addContexts(api, List(e1, e2))
    } yield {
      val t1 = e1.derived_contexts must beLike {
        case List(SelfDescribingData(key, json)) =>
          (json.noSpaces must beEqualTo(expectedContext1.noSpaces)) and (key must beEqualTo(expectedKey))
      }
      val t2 = e2.derived_contexts must beLike {
        case List(SelfDescribingData(key, json)) =>
          (json.noSpaces must beEqualTo(expectedContext2.noSpaces)) and (key must beEqualTo(expectedKey))
      }
      t1 and t2
    }
  }

  def e2 = {
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString
    e.domain_userid = "test-domain-user-id"
    e.network_userid = "test-network-user-id"
    e.user_id = "test-user-id"

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
      api = Identity.build(testConfig, new RecordingHttpClient(ref).mock)
      _ <- Identity.addContexts(api, List(e))
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

  def e3 = {

    // unset network_userid and user_id
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString
    e.domain_userid = "test-domain-user-id"

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
      api = Identity.build(testConfig, new RecordingHttpClient(ref).mock)
      _ <- Identity.addContexts(api, List(e))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        body must beEqualTo(expectedRequestBody.noSpaces)
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

  val testHttpClient: Http4sClient[IO] = Http4sClient[IO] { _ =>
    Resource.eval(Created(mockResponseBody))
  }

  // A mock Client[F] that records the requests it receives
  class RecordingHttpClient(ref: Ref[IO, List[(Request[IO], String)]]) {
    def mock: Http4sClient[IO] =
      Http4sClient[IO] { req =>
        Resource.eval {
          for {
            body <- req.bodyText.compile.string
            _ <- ref.update(_ :+ ((req, body)))
            response <- Created(mockResponseBody)
          } yield response
        }
      }
  }

  val testConfig = Config.IdentityM[Id](Uri.unsafeFromString("http://test-server:8787"),
                                        "test-user",
                                        "test-password",
                                        1,
                                        Retrying.Config.ForTransient(100.millis, 1)
  )

}
