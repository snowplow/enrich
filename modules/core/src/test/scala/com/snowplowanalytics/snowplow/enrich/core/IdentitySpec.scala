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
import cats.effect.testing.specs2.CatsEffect
import io.circe.Json
import io.circe.literal._
import org.http4s.{Request, Uri}
import org.specs2.Specification
import org.http4s.circe.jsonEncoder
import org.http4s.client.{Client => Http4sClient}
import org.http4s.dsl.io._

import com.snowplowanalytics.snowplow.runtime.Retrying
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonPath

import java.util.UUID
import scala.concurrent.duration.DurationInt

class IdentitySpec extends Specification with CatsEffect {
  import IdentitySpec._

  def is = s2"""
  Identity should
    add identity contexts to enriched events $e1
    send valid requests to the identity api $e2
    omit identifiers from the request body when they are null $e3
    extract and send identifiers from entities $e4
    extract from the correct entity when multiple entities of same type exist $e5
    extract identifiers from derived contexts $e16
    extract from contexts before derived contexts when both contain the same entity type $e17
    extract identifiers from unstructured events $e6
    filter with All logic and In operator $e7
    filter with All logic and NotIn operator $e8
    filter with Any logic and In operator $e9
    filter with Any logic and NotIn operator $e10
    filter out event when field is missing and operator is In $e11
    filter out event when field is missing and operator is NotIn $e12
    not query the api if no identifier can be found $e13
    not add identity context when circuit breaker is open $e14
    recover and add identity context after circuit closes $e15
    write decision_reasons onto the identity entity $e18
    omit decision_reasons from the identity entity when the api omits the key $e19
    omit decision_reasons from the identity entity when the api returns an empty array $e20
    omit decision_reasons from the identity entity when the api returns null $e21
    omit decision_reasons from the identity entity when the api returns a malformed value $e22
    keep identities for the rest of the batch when one entry has malformed decision_reasons $e23
    omit decision_reasons entirely when one element is not a string $e24
    omit decision_reasons when the api returns an object instead of an array $e25
    pass an unknown reason through unchanged $e26
    report exactly the events whose decision_reasons were dropped $e27
    say how many and which events lost decision_reasons, and mark a cut id list $e28
    omit decision_reasons when the api returns a bare string instead of an array $e29
  """

  def e1 = {
    val e1 = new EnrichedEvent()
    e1.event_id = testIdPairs(1).eventId
    e1.user_id = "test-user-1"

    val e2 = new EnrichedEvent()
    e2.event_id = testIdPairs(2).eventId
    e2.user_id = "test-user-2"

    val expectedContext1 = json"""
    {
      "snowplow_id": ${testIdPairs(1).snowplowId},
      "created_at": "2023-03-03T03:03:03.333Z"
    }
    """

    val expectedContext2 = json"""
    {
      "snowplow_id": ${testIdPairs(2).snowplowId},
      "created_at": "2023-03-03T03:03:03.333Z"
    }
    """
    val expectedKey = expectedIdentitySchemaKey

    // e1 already carries an entity from an earlier enrichment. The identity entity goes in front of
    // it; it must never replace it.
    val preExisting = SelfDescribingData(
      SchemaKey("com.test", "earlier_context", "jsonschema", SchemaVer.Full(1, 0, 0)),
      json"""{"from": "an earlier enrichment"}"""
    )
    e1.derived_contexts = List(preExisting)

    for {
      api <- Identity.build(testConfig(), testHttpClient)
      _ <- api.addIdentityContexts(List(e1, e2))
    } yield {
      val t1 = e1.derived_contexts must beLike {
        case List(SelfDescribingData(key, json), kept) =>
          (json.noSpaces must beEqualTo(expectedContext1.noSpaces)) and
            (key must beEqualTo(expectedKey)) and
            (kept must beEqualTo(preExisting))
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
    e.derived_tstamp = "2024-01-16 10:30:45.123"

    val expectedRequestBody = json"""
      [
        {
          "event_id": ${e.event_id},
          "derived_timestamp": ${e.derived_tstamp},
          "identifiers": {
            "user_id": ${e.user_id},
            "domain_userid": ${e.domain_userid},
            "network_userid": ${e.network_userid}
          }
        }
      ]
    """

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(testConfig(), new RecordingHttpClient(ref).mock)
      _ <- api.addIdentityContexts(List(e))
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
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString
    e.domain_userid = "test-domain-user-id"

    val expectedRequestBody = json"""
      [
        {
          "event_id": ${e.event_id},
          "derived_timestamp": null,
          "identifiers": {
            "domain_userid": ${e.domain_userid}
          }
        }
      ]
    """

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(testConfig(), new RecordingHttpClient(ref).mock)
      _ <- api.addIdentityContexts(List(e))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        body must beEqualTo(expectedRequestBody.noSpaces)
    }
  }

  def e4 = {
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString
    e.user_id = "test-user-id"
    e.derived_tstamp = "2024-01-16 14:25:30.456"

    val testEntityData = json"""{"customId": "custom-123"}"""
    val testEntitySchema = SchemaKey("com.test", "user_context", "jsonschema", SchemaVer.Full(1, 0, 0))
    e.contexts = List(SelfDescribingData(testEntitySchema, testEntityData))

    val config = testConfig(
      identifiers = List(
        Config.Identity.Identifier(
          name = "user_id",
          field = atomicField("user_id")
        ),
        Config.Identity.Identifier(
          name = "custom_id",
          field = entityField("com.test", "user_context", 1, "$.customId", Some(0))
        )
      ),
      filters = None
    )

    val expectedRequestBody = json"""
      [
        {
          "event_id": ${e.event_id},
          "derived_timestamp": ${e.derived_tstamp},
          "identifiers": {
            "user_id": ${e.user_id},
            "custom_id": "custom-123"
          }
        }
      ]
    """

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(config, new RecordingHttpClient(ref).mock)
      _ <- api.addIdentityContexts(List(e))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        body must beEqualTo(expectedRequestBody.noSpaces)
    }
  }

  def e5 = {
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString
    e.user_id = "test-user-id"

    val testEntitySchema = SchemaKey("com.test", "user_context", "jsonschema", SchemaVer.Full(1, 0, 0))
    val entity0 = json"""{"customId": "id-from-first"}"""
    val entity1 = json"""{"customId": "id-from-second"}"""
    val entity2 = json"""{"customId": "id-from-third"}"""

    e.contexts = List(
      SelfDescribingData(testEntitySchema, entity0),
      SelfDescribingData(testEntitySchema, entity1),
      SelfDescribingData(testEntitySchema, entity2)
    )

    val config = testConfig(
      identifiers = List(
        Config.Identity.Identifier(
          name = "user_id",
          field = atomicField("user_id")
        ),
        Config.Identity.Identifier(
          name = "first_custom_id",
          field = entityField("com.test", "user_context", 1, "$.customId", Some(0))
        ),
        Config.Identity.Identifier(
          name = "second_custom_id",
          field = entityField("com.test", "user_context", 1, "$.customId", Some(1))
        ),
        Config.Identity.Identifier(
          name = "third_custom_id",
          field = entityField("com.test", "user_context", 1, "$.customId", Some(2))
        ),
        Config.Identity.Identifier(
          name = "default_custom_id",
          field = entityField("com.test", "user_context", 1, "$.customId", None)
        )
      ),
      filters = None
    )

    val expectedRequestBody = json"""
      [
        {
          "event_id": ${e.event_id},
          "derived_timestamp": null,
          "identifiers": {
            "default_custom_id": "id-from-first",
            "first_custom_id": "id-from-first",
            "second_custom_id": "id-from-second",
            "user_id": ${e.user_id},
            "third_custom_id": "id-from-third"
          }
        }
      ]
    """

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(config, new RecordingHttpClient(ref).mock)
      _ <- api.addIdentityContexts(List(e))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        body must beEqualTo(expectedRequestBody.noSpaces)
    }
  }

  def e6 = {
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString

    val testEventData = json"""{"eventUserId": "event-user-123"}"""
    val testEventSchema = SchemaKey("com.test", "login_event", "jsonschema", SchemaVer.Full(1, 0, 0))
    e.unstruct_event = Some(SelfDescribingData(testEventSchema, testEventData))

    val config = testConfig(
      identifiers = List(
        Config.Identity.Identifier(
          name = "event_user_id",
          field = eventField("com.test", "login_event", 1, "$.eventUserId")
        )
      ),
      filters = None
    )

    val expectedRequestBody = json"""
      [
        {
          "event_id": ${e.event_id},
          "derived_timestamp": null,
          "identifiers": {
            "event_user_id": "event-user-123"
          }
        }
      ]
    """

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(config, new RecordingHttpClient(ref).mock)
      _ <- api.addIdentityContexts(List(e))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        body must beEqualTo(expectedRequestBody.noSpaces)
    }
  }

  def e7 = {
    val e1 = new EnrichedEvent()
    e1.event_id = UUID.randomUUID.toString
    e1.app_id = "allowed_app"
    e1.user_id = "user1"

    val e2 = new EnrichedEvent()
    e2.event_id = UUID.randomUUID.toString
    e2.app_id = "blocked_app"
    e2.user_id = "user2"

    val config = testConfig(
      identifiers = List(
        Config.Identity.Identifier(
          name = "user_id",
          field = atomicField("user_id")
        )
      ),
      filters = Some(
        Config.Identity.Filtering.Filters(
          logic = Config.Identity.Filtering.Logic.All,
          rules = List(
            Config.Identity.Filtering.Rule(
              field = atomicField("app_id"),
              operator = Config.Identity.Filtering.Operator.In,
              values = List("allowed_app")
            )
          )
        )
      )
    )

    // Only e1 should pass the filter
    val expectedRequestBody = json"""
      [
        {
          "event_id": ${e1.event_id},
          "derived_timestamp": null,
          "identifiers": {
            "user_id": ${e1.user_id}
          }
        }
      ]
    """

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(config, new RecordingHttpClient(ref).mock)
      _ <- api.addIdentityContexts(List(e1, e2))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        body must beEqualTo(expectedRequestBody.noSpaces)
    }
  }

  def e8 = {
    val e1 = new EnrichedEvent()
    e1.event_id = UUID.randomUUID.toString
    e1.app_id = "production_app"
    e1.user_id = "test-user"

    val e2 = new EnrichedEvent()
    e2.event_id = UUID.randomUUID.toString
    e2.app_id = "test_app"
    e2.user_id = "test-user"

    val config = testConfig(
      identifiers = List(
        Config.Identity.Identifier(
          name = "user_id",
          field = atomicField("user_id")
        )
      ),
      filters = Some(
        Config.Identity.Filtering.Filters(
          logic = Config.Identity.Filtering.Logic.All,
          rules = List(
            Config.Identity.Filtering.Rule(
              field = atomicField("app_id"),
              operator = Config.Identity.Filtering.Operator.NotIn,
              values = List("test_app")
            )
          )
        )
      )
    )

    // Only e1 should pass
    val expectedRequestBody = json"""
      [
        {
          "event_id": ${e1.event_id},
          "derived_timestamp": null,
          "identifiers": {
            "user_id": ${e1.user_id}
          }
        }
      ]
    """

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(config, new RecordingHttpClient(ref).mock)
      _ <- api.addIdentityContexts(List(e1, e2))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        body must beEqualTo(expectedRequestBody.noSpaces)
    }
  }

  def e9 = {
    val e1 = new EnrichedEvent()
    e1.event_id = UUID.randomUUID.toString
    e1.app_id = "allowed_app"
    e1.user_id = ""
    e1.derived_tstamp = "2024-01-16 09:15:22.789"

    val e2 = new EnrichedEvent()
    e2.event_id = UUID.randomUUID.toString
    e2.app_id = "blocked_app"
    e2.user_id = "test-user"
    e2.derived_tstamp = "2024-01-16 09:15:23.001"

    val config = testConfig(
      identifiers = List(
        Config.Identity.Identifier(
          name = "user_id",
          field = atomicField("user_id")
        )
      ),
      filters = Some(
        Config.Identity.Filtering.Filters(
          logic = Config.Identity.Filtering.Logic.Any,
          rules = List(
            Config.Identity.Filtering.Rule(
              field = atomicField("app_id"),
              operator = Config.Identity.Filtering.Operator.In,
              values = List("allowed_app")
            ),
            Config.Identity.Filtering.Rule(
              field = atomicField("user_id"),
              operator = Config.Identity.Filtering.Operator.In,
              values = List("test-user")
            )
          )
        )
      )
    )

    // Both events should pass
    val expectedRequestBody = json"""
      [
        {
          "event_id": ${e1.event_id},
          "derived_timestamp": ${e1.derived_tstamp},
          "identifiers": {
            "user_id": ${e1.user_id}
          }
        },
        {
          "event_id": ${e2.event_id},
          "derived_timestamp": ${e2.derived_tstamp},
          "identifiers": {
            "user_id": ${e2.user_id}
          }
        }
      ]
    """

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(config, new RecordingHttpClient(ref).mock)
      _ <- api.addIdentityContexts(List(e1, e2))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        body must beEqualTo(expectedRequestBody.noSpaces)
    }
  }

  def e10 = {
    val e1 = new EnrichedEvent()
    e1.event_id = UUID.randomUUID.toString
    e1.app_id = "dev_app"
    e1.user_id = "test-user"

    val e2 = new EnrichedEvent()
    e2.event_id = UUID.randomUUID.toString
    e2.app_id = "production_app"
    e2.user_id = "test-user"

    val e3 = new EnrichedEvent()
    e3.event_id = UUID.randomUUID.toString
    e3.app_id = "staging_app"
    e3.user_id = "test-user"

    val config = testConfig(
      identifiers = List(
        Config.Identity.Identifier(
          name = "user_id",
          field = atomicField("user_id")
        )
      ),
      filters = Some(
        Config.Identity.Filtering.Filters(
          logic = Config.Identity.Filtering.Logic.Any,
          rules = List(
            Config.Identity.Filtering.Rule(
              field = atomicField("app_id"),
              operator = Config.Identity.Filtering.Operator.NotIn,
              values = List("test_app", "dev_app")
            )
          )
        )
      )
    )

    // e2 and e3 should pass
    val expectedRequestBody = json"""
      [
        {
          "event_id": ${e2.event_id},
          "derived_timestamp": null,
          "identifiers": {
            "user_id": ${e2.user_id}
          }
        },
        {
          "event_id": ${e3.event_id},
          "derived_timestamp": null,
          "identifiers": {
            "user_id": ${e3.user_id}
          }
        }
      ]
    """

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(config, new RecordingHttpClient(ref).mock)
      _ <- api.addIdentityContexts(List(e1, e2, e3))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        body must beEqualTo(expectedRequestBody.noSpaces)
    }
  }

  def e11 = {
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString

    val config = testConfig(
      identifiers = List(
        Config.Identity.Identifier(
          name = "user_id",
          field = atomicField("user_id")
        )
      ),
      filters = Some(
        Config.Identity.Filtering.Filters(
          logic = Config.Identity.Filtering.Logic.All,
          rules = List(
            Config.Identity.Filtering.Rule(
              field = atomicField("app_id"),
              operator = Config.Identity.Filtering.Operator.In,
              values = List("allowed_app")
            )
          )
        )
      )
    )

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(config, new RecordingHttpClient(ref).mock)
      _ <- api.addIdentityContexts(List(e))
      result <- ref.get
    } yield result must beEqualTo(Nil)
  }

  def e12 = {
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString

    val config = testConfig(
      identifiers = List(
        Config.Identity.Identifier(
          name = "user_id",
          field = atomicField("user_id")
        )
      ),
      filters = Some(
        Config.Identity.Filtering.Filters(
          logic = Config.Identity.Filtering.Logic.All,
          rules = List(
            Config.Identity.Filtering.Rule(
              field = atomicField("app_id"),
              operator = Config.Identity.Filtering.Operator.NotIn,
              values = List("blocked_app")
            )
          )
        )
      )
    )

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(config, new RecordingHttpClient(ref).mock)
      _ <- api.addIdentityContexts(List(e))
      result <- ref.get
    } yield result must beEqualTo(Nil)
  }

  def e13 = {
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString
    e.user_id = "test-user-id"

    val testEntitySchema = SchemaKey("com.test", "user_context", "jsonschema", SchemaVer.Full(1, 0, 0))
    val entity0 = json"""{"customId": "id-from-first"}"""
    val entity1 = json"""{"customId": "id-from-second"}"""

    e.contexts = List(
      SelfDescribingData(testEntitySchema, entity0),
      SelfDescribingData(testEntitySchema, entity1)
    )

    val config = testConfig(
      identifiers = List(
        Config.Identity.Identifier(
          name = "out_of_bounds_id",
          field = entityField("com.test", "user_context", 1, "$.customId", Some(5))
        )
      ),
      filters = None
    )

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(config, new RecordingHttpClient(ref).mock)
      _ <- api.addIdentityContexts(List(e))
      result <- ref.get
    } yield result must beTypedEqualTo(Nil)
  }

  def e14 = {
    val mockResponseBody = """[]"""

    val test = for {
      callCount <- Ref.of[IO, Int](0)
      failingClient = Http4sClient.fromHttpApp[IO](org.http4s.HttpApp[IO] { _ =>
                        callCount.updateAndGet(_ + 1).flatMap { count =>
                          if (count <= 5)
                            IO.raiseError(new Exception("API unavailable"))
                          else
                            Created(mockResponseBody)
                        }
                      })

      identifiers = List(
                      Config.Identity.Identifier("user_id", Config.Identity.Identifier.Atomic(EnrichedEvent.atomicFieldsByName("user_id")))
                    )

      cbConfig = Config.Identity.CircuitBreakerConfig(
                   maxConsecutiveFailures = 5,
                   failureRateThreshold = 0.5,
                   failureRateWindow = 1.minute,
                   minRequestsForRateCheck = 10,
                   initialBackoff = 100.millis,
                   maxBackoff = 5.minutes,
                   backoffMultiplier = 2.0
                 )

      config = testConfig(
                 identifiers = identifiers,
                 filters = None
               ).copy(circuitBreaker = cbConfig)

      event = new EnrichedEvent()
      _ = event.event_id = "event-123"
      _ = event.user_id = "user-456"

      api <- Identity.build(config, failingClient)
      _ <- api.addIdentityContexts(List(event))
      _ <- api.addIdentityContexts(List(event))
      _ <- api.addIdentityContexts(List(event))
      _ <- api.addIdentityContexts(List(event))
      _ <- api.addIdentityContexts(List(event))
      _ <- IO.sleep(50.millis)
      _ <- api.addIdentityContexts(List(event))
      finalCallCount <- callCount.get
    } yield (event.derived_contexts.size, finalCallCount)

    test.map {
      case (contextCount, finalCallCount) =>
        (contextCount must beEqualTo(0)) and (finalCallCount must beEqualTo(5))
    }
  }

  def e15 = {
    val mockResponseBody = """[{"created_at":"2024-01-01T00:00:00Z","event_id":"event-123","snowplow_id":"sp-123"}]"""

    val test = for {
      callCount <- Ref.of[IO, Int](0)
      recoveringClient = Http4sClient.fromHttpApp[IO](org.http4s.HttpApp[IO] { _ =>
                           callCount.updateAndGet(_ + 1).flatMap { count =>
                             if (count <= 3)
                               IO.raiseError(new Exception("API unavailable"))
                             else
                               Created(mockResponseBody)
                           }
                         })

      identifiers = List(
                      Config.Identity.Identifier("user_id", Config.Identity.Identifier.Atomic(EnrichedEvent.atomicFieldsByName("user_id")))
                    )

      cbConfig = Config.Identity.CircuitBreakerConfig(
                   maxConsecutiveFailures = 3,
                   failureRateThreshold = 0.5,
                   failureRateWindow = 1.minute,
                   minRequestsForRateCheck = 10,
                   initialBackoff = 50.millis,
                   maxBackoff = 5.minutes,
                   backoffMultiplier = 2.0
                 )

      config = testConfig(
                 identifiers = identifiers,
                 filters = None
               ).copy(circuitBreaker = cbConfig)

      event = new EnrichedEvent()
      _ = event.event_id = "event-123"
      _ = event.user_id = "user-456"

      api <- Identity.build(config, recoveringClient)
      _ <- api.addIdentityContexts(List(event))
      _ <- api.addIdentityContexts(List(event))
      _ <- api.addIdentityContexts(List(event))
      _ <- IO.sleep(100.millis)
      _ <- api.addIdentityContexts(List(event))
      finalCallCount <- callCount.get
    } yield (event.derived_contexts.size, finalCallCount)

    test.map {
      case (contextCount, finalCallCount) =>
        (contextCount must beEqualTo(1)) and (finalCallCount must beEqualTo(4))
    }
  }

  def e16 = {
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString
    e.user_id = "test-user-id"
    e.derived_tstamp = "2024-01-16 14:25:30.456"

    val testEntityData = json"""{"customId": "derived-123"}"""
    val testEntitySchema = SchemaKey("com.test", "user_context", "jsonschema", SchemaVer.Full(1, 0, 0))
    e.derived_contexts = List(SelfDescribingData(testEntitySchema, testEntityData))

    val config = testConfig(
      identifiers = List(
        Config.Identity.Identifier(
          name = "user_id",
          field = atomicField("user_id")
        ),
        Config.Identity.Identifier(
          name = "custom_id",
          field = entityField("com.test", "user_context", 1, "$.customId", Some(0))
        )
      ),
      filters = None
    )

    val expectedRequestBody = json"""
      [
        {
          "event_id": ${e.event_id},
          "derived_timestamp": ${e.derived_tstamp},
          "identifiers": {
            "user_id": ${e.user_id},
            "custom_id": "derived-123"
          }
        }
      ]
    """

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(config, new RecordingHttpClient(ref).mock)
      _ <- api.addIdentityContexts(List(e))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        body must beEqualTo(expectedRequestBody.noSpaces)
    }
  }

  def e17 = {
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString
    e.user_id = "test-user-id"

    val testEntitySchema = SchemaKey("com.test", "user_context", "jsonschema", SchemaVer.Full(1, 0, 0))
    val fromContexts = json"""{"customId": "from-contexts"}"""
    val fromDerived = json"""{"customId": "from-derived"}"""

    e.contexts = List(SelfDescribingData(testEntitySchema, fromContexts))
    e.derived_contexts = List(SelfDescribingData(testEntitySchema, fromDerived))

    val config = testConfig(
      identifiers = List(
        Config.Identity.Identifier(
          name = "user_id",
          field = atomicField("user_id")
        ),
        Config.Identity.Identifier(
          name = "context_id",
          field = entityField("com.test", "user_context", 1, "$.customId", Some(0))
        ),
        Config.Identity.Identifier(
          name = "derived_id",
          field = entityField("com.test", "user_context", 1, "$.customId", Some(1))
        )
      ),
      filters = None
    )

    // contexts are ordered before derived_contexts, so index 0 is the context
    // entity and index 1 is the derived context entity
    val expectedRequestBody = json"""
      [
        {
          "event_id": ${e.event_id},
          "derived_timestamp": null,
          "identifiers": {
            "user_id": ${e.user_id},
            "context_id": "from-contexts",
            "derived_id": "from-derived"
          }
        }
      ]
    """

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(config, new RecordingHttpClient(ref).mock)
      _ <- api.addIdentityContexts(List(e))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        body must beEqualTo(expectedRequestBody.noSpaces)
    }
  }

  def e18 = emitsReasons(json"""["merge_limit_exceeded", "degraded_registry_only"]""")

  def e19 = emitsNoDecisionReasons(None)

  def e20 = emitsNoDecisionReasons(Some(json"[]"))

  def e21 = emitsNoDecisionReasons(Some(Json.Null))

  def e22 = emitsNoDecisionReasons(Some(json"""[1, 2]"""))

  // A malformed value is not a transport failure. With retries available the API is still called once,
  // the value costs only that entry's reasons, and every other entry keeps its identity and its reasons.
  def e23 = {
    val wellFormed = new EnrichedEvent()
    wellFormed.event_id = testIdPairs(0).eventId
    wellFormed.user_id = "test-user-0"

    val malformed = new EnrichedEvent()
    malformed.event_id = testIdPairs(1).eventId
    malformed.user_id = "test-user-1"

    val responseBody = json"""
    [
      {
        "created_at": "2023-03-03T03:03:03.333Z",
        "event_id": ${testIdPairs(0).eventId},
        "snowplow_id": ${testIdPairs(0).snowplowId},
        "decision_reasons": ["merge_limit_exceeded"]
      },
      {
        "created_at": "2023-03-03T03:03:03.333Z",
        "event_id": ${testIdPairs(1).eventId},
        "snowplow_id": ${testIdPairs(1).snowplowId},
        "decision_reasons": [1, 2]
      }
    ]
    """

    val expectedWellFormed = json"""
    {
      "snowplow_id": ${testIdPairs(0).snowplowId},
      "created_at": "2023-03-03T03:03:03.333Z",
      "decision_reasons": ["merge_limit_exceeded"]
    }
    """

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api <- Identity.build(testConfig(attempts = 3), new RecordingHttpClient(ref, responseBody).mock)
      _ <- api.addIdentityContexts(List(wellFormed, malformed))
      requests <- ref.get
    } yield {
      val keptItsReasons = wellFormed.derived_contexts must beLike {
        case List(SelfDescribingData(key, json)) =>
          (key must beEqualTo(expectedIdentitySchemaKey)) and
            (json.noSpaces must beEqualTo(expectedWellFormed.noSpaces))
      }
      val keptIdentityLostReasons = malformed.derived_contexts must beLike {
        case List(SelfDescribingData(key, json)) =>
          (key must beEqualTo(expectedIdentitySchemaKey)) and hasOnlyBaseFields(json)
      }
      // One request despite three attempts being allowed: nothing was retried
      keptItsReasons and keptIdentityLostReasons and (requests must haveSize(1))
    }
  }

  // The guard is all or nothing: one bad element drops the whole array, it does not filter the array
  def e24 = emitsNoDecisionReasons(Some(json"""["merge_limit_exceeded", 1]"""))

  def e25 = emitsNoDecisionReasons(Some(json"""{"code": "merge_limit_exceeded"}"""))

  def e29 = emitsNoDecisionReasons(Some(json""""merge_limit_exceeded""""))

  // Enrich holds no list of allowed values, and neither does the schema. This is what lets the identity
  // service add a reason without an enrich release: a value enrich has never seen is written as sent,
  // and the identity models decide what it means.
  def e26 = emitsReasons(json"""["not_a_known_reason"]""")

  // merge reports exactly the events whose reasons it dropped; the warning is built from that list
  def e27 = {
    val kept = new EnrichedEvent()
    kept.event_id = testIdPairs(0).eventId
    val dropped = new EnrichedEvent()
    dropped.event_id = testIdPairs(1).eventId
    val nothingToReport = new EnrichedEvent()
    nothingToReport.event_id = testIdPairs(2).eventId

    val identities = List(
      // A duplicate for the same event, shadowed by the later entry below. Neither attached nor reported.
      Identity.Identity("2023-03-03T03:03:03.333Z", testIdPairs(0).eventId, "sp-shadowed", Some(json"""[1]""")),
      Identity.Identity("2023-03-03T03:03:03.333Z",
                        testIdPairs(0).eventId,
                        testIdPairs(0).snowplowId,
                        Some(json"""["merge_limit_exceeded"]""")
      ),
      Identity.Identity("2023-03-03T03:03:03.333Z", testIdPairs(1).eventId, testIdPairs(1).snowplowId, Some(json"""[1, 2]""")),
      Identity.Identity("2023-03-03T03:03:03.333Z", testIdPairs(2).eventId, testIdPairs(2).snowplowId, Some(json"[]")),
      // Not in the batch: the API answered for an event it was not asked about. Ignored, and not reported.
      Identity.Identity("2023-03-03T03:03:03.333Z", UUID.randomUUID.toString, "sp-stray", Some(json"""[1]"""))
    )

    Identity.merge(List(kept, dropped, nothingToReport), identities) must beEqualTo(
      Identity.Merged(attached = 3, dropped = List(testIdPairs(1).eventId))
    )
  }

  // The warning is the only operator-facing signal, so its shape is pinned: count, denominator, ids,
  // and a marker when the id list is cut
  def e28 = {
    val ids = (1 to 12).map(i => s"event-$i").toList
    val cut = Identity.droppedWarning(Identity.Merged(attached = 20, dropped = ids)).getOrElse("")
    val whole = Identity.droppedWarning(Identity.Merged(attached = 5, dropped = ids.take(2))).getOrElse("")
    val clean = Identity.droppedWarning(Identity.Merged(attached = 5, dropped = Nil))

    List(
      cut must contain("12 of 20 events given an identity entity"),
      cut must contain("(first 10 of 12)"),
      cut must contain("event-10"),
      cut must not(contain("event-11")),
      whole must contain("2 of 5 events given an identity entity"),
      whole must not(contain("(first")),
      whole must endWith("event-1, event-2"),
      clean must beNone
    ).reduce(_ and _)
  }

  // One identity in the API response, for testIdPairs(0), with or without a decision_reasons key
  private def identityResponse(reasons: Option[Json]): Json =
    Json.arr(
      Json.fromFields(
        List(
          "created_at" -> Json.fromString("2023-03-03T03:03:03.333Z"),
          "event_id" -> Json.fromString(testIdPairs(0).eventId),
          "snowplow_id" -> Json.fromString(testIdPairs(0).snowplowId)
        ) ++ reasons.map("decision_reasons" -> _)
      )
    )

  private def enrichOne(reasons: Option[Json]): IO[EnrichedEvent] = {
    val e = new EnrichedEvent()
    e.event_id = testIdPairs(0).eventId
    e.user_id = "test-user-0"

    for {
      api <- Identity.build(testConfig(), staticHttpClient(identityResponse(reasons)))
      _ <- api.addIdentityContexts(List(e))
    } yield e
  }

  // A non-empty array of strings is written onto the entity exactly as the API sent it
  private def emitsReasons(reasons: Json) = {
    val expectedContext = json"""
    {
      "snowplow_id": ${testIdPairs(0).snowplowId},
      "created_at": "2023-03-03T03:03:03.333Z",
      "decision_reasons": $reasons
    }
    """

    enrichOne(Some(reasons)).map(_.derived_contexts must beLike {
      case List(SelfDescribingData(key, json)) =>
        (key must beEqualTo(expectedIdentitySchemaKey)) and
          (json.noSpaces must beEqualTo(expectedContext.noSpaces))
    })
  }

  // The API says "no reasons" by omitting the key, sending an empty array, or sending null, and it can
  // send a shape enrich does not write. All of these must produce an entity with exactly the two base
  // fields. The exact key-set assertion is what fails if the shape guard in merge() is ever dropped,
  // because a dropped guard writes the key rather than leaving it out.
  private def emitsNoDecisionReasons(reasons: Option[Json]) =
    enrichOne(reasons).map(_.derived_contexts must beLike {
      case List(SelfDescribingData(key, json)) =>
        (key must beEqualTo(expectedIdentitySchemaKey)) and hasOnlyBaseFields(json)
    })

  private def hasOnlyBaseFields(json: Json) =
    json.asObject.map(_.keys.toList) must beSome(List("snowplow_id", "created_at"))
}

object IdentitySpec {
  case class IdPair(eventId: String, snowplowId: String)

  // Helper to create Atomic with resolved Field
  def atomicField(name: String): Config.Identity.Identifier.Atomic =
    Config.Identity.Identifier.Atomic(EnrichedEvent.atomicFieldsByName(name))

  // Helper to create Event with compiled path
  def eventField(
    vendor: String,
    name: String,
    majorVersion: Int,
    path: String
  ): Config.Identity.Identifier.Event = {
    val compiled = JsonPath.compileQuery(path).toOption.map(new JsonPath(_, path)).get
    Config.Identity.Identifier.Event(vendor, name, majorVersion, compiled)
  }

  // Helper to create Entity with compiled path
  def entityField(
    vendor: String,
    name: String,
    majorVersion: Int,
    path: String,
    index: Option[Int]
  ): Config.Identity.Identifier.Entity = {
    val compiled = JsonPath.compileQuery(path).toOption.map(new JsonPath(_, path)).get
    Config.Identity.Identifier.Entity(vendor, name, majorVersion, index, compiled)
  }

  val testIdPairs = List(
    IdPair(UUID.randomUUID.toString, UUID.randomUUID.toString),
    IdPair(UUID.randomUUID.toString, UUID.randomUUID.toString),
    IdPair(UUID.randomUUID.toString, UUID.randomUUID.toString),
    IdPair(UUID.randomUUID.toString, UUID.randomUUID.toString)
  )

  val mockResponseBody = json"""
  [
    {
      "created_at": "2023-03-03T03:03:03.333Z",
      "event_id": ${testIdPairs(0).eventId},
      "snowplow_id": ${testIdPairs(0).snowplowId}
    },
    {
      "created_at": "2023-03-03T03:03:03.333Z",
      "event_id": ${testIdPairs(1).eventId},
      "snowplow_id": ${testIdPairs(1).snowplowId}
    },
    {
      "created_at": "2023-03-03T03:03:03.333Z",
      "event_id": ${testIdPairs(2).eventId},
      "snowplow_id": ${testIdPairs(2).snowplowId}
    },
    {
      "created_at": "2023-03-03T03:03:03.333Z",
      "event_id": ${testIdPairs(3).eventId},
      "snowplow_id": ${testIdPairs(3).snowplowId}
    }
  ]
  """

  val expectedIdentitySchemaKey: SchemaKey =
    SchemaKey.fromUri("iglu:com.snowplowanalytics.snowplow/identity/jsonschema/2-0-1").toOption.get

  // A mock Client[F] that answers every request with the given body
  def staticHttpClient(responseBody: Json): Http4sClient[IO] =
    Http4sClient[IO] { _ =>
      Resource.eval(Created(responseBody))
    }

  val testHttpClient: Http4sClient[IO] = staticHttpClient(mockResponseBody)

  // A mock Client[F] that records the requests it receives
  class RecordingHttpClient(ref: Ref[IO, List[(Request[IO], String)]], responseBody: Json = mockResponseBody) {
    def mock: Http4sClient[IO] =
      Http4sClient[IO] { req =>
        Resource.eval {
          for {
            body <- req.bodyText.compile.string
            _ <- ref.update(_ :+ ((req, body)))
            response <- Created(responseBody)
          } yield response
        }
      }
  }

  val defaultIdentifiers = List(
    Config.Identity.Identifier(
      name = "user_id",
      field = atomicField("user_id")
    ),
    Config.Identity.Identifier(
      name = "domain_userid",
      field = atomicField("domain_userid")
    ),
    Config.Identity.Identifier(
      name = "network_userid",
      field = atomicField("network_userid")
    )
  )

  def testConfig(
    identifiers: List[Config.Identity.Identifier] = defaultIdentifiers,
    filters: Option[Config.Identity.Filtering.Filters] = None,
    attempts: Int = 1
  ) =
    Config.IdentityM[Id](
      Uri.unsafeFromString("http://test-server:8787"),
      "test-user",
      "test-password",
      1,
      Retrying.Config.ForTransient(100.millis, attempts),
      Config.Identity.CircuitBreakerConfig(
        maxConsecutiveFailures = 5,
        failureRateThreshold = 0.5,
        failureRateWindow = 1.minute,
        minRequestsForRateCheck = 10,
        initialBackoff = 30.seconds,
        maxBackoff = 5.minutes,
        backoffMultiplier = 2.0
      ),
      identifiers,
      filters
    )

}
