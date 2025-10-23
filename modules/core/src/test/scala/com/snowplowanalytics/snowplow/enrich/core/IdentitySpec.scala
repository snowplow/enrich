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
import io.circe.literal._
import org.http4s.{Request, Uri}
import org.specs2.Specification
import org.http4s.circe.jsonEncoder
import org.http4s.client.{Client => Http4sClient}
import org.http4s.dsl.io._

import com.snowplowanalytics.snowplow.runtime.Retrying
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonPath.compileQuery

import java.util.UUID
import scala.concurrent.duration.DurationInt

class IdentitySpec extends Specification with CatsEffect {
  import IdentitySpec._

  def is = s2"""
  Identity should
    add contexts to enriched events $e1
    send valid requests to the identity api $e2
    omit id fields from the request body when they are null $e3
    extract and send custom identifiers $e4
    extract from the correct entity when multiple entities of same type exist $e5
    extract identifiers from unstructured events $e6
    filter with All logic and In operator $e7
    filter with All logic and NotIn operator $e8
    filter with Any logic and In operator $e9
    filter with Any logic and NotIn operator $e10
    return None when entity index is out of bounds $e11
    filter out event when field is missing and operator is In $e12
    filter out event when field is missing and operator is NotIn $e13
  """

  def e1 = {
    val e1 = new EnrichedEvent()
    e1.event_id = testIdPairs(1).eventId
    e1.user_id = "test-user-1" // Add identifier so batch is not empty

    val e2 = new EnrichedEvent()
    e2.event_id = testIdPairs(2).eventId
    e2.user_id = "test-user-2" // Add identifier so batch is not empty

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
      _ <- api.addContexts(List(e1, e2))
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
      _ <- api.addContexts(List(e))
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
      _ <- api.addContexts(List(e))
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

    // Create a test entity context
    val testEntityData = json"""{"customId": "custom-123"}"""
    val testEntitySchema = SchemaKey("com.test", "user_context", "jsonschema", SchemaVer.Full(1, 0, 0))
    e.contexts = List(SelfDescribingData(testEntitySchema, testEntityData))

    val customConfig = Config.CustomIdentifiers(
      identifiers = List(
        Config.Identifier(
          name = "user_id",
          field = atomicField("user_id"),
          priority = 1,
          unique = true
        ),
        Config.Identifier(
          name = "custom_id",
          field = entityField("com.test", "user_context", 1, "$.customId", Some(0)),
          priority = 2,
          unique = false
        )
      ),
      filters = None
    )

    val expectedRequestBody = json"""
      [
        {
          "eventId": ${e.event_id},
          "identifiers": {
            "user_id": ${e.user_id},
            "custom": {
              "user_id": {
                "value": "test-user-id",
                "priority": 1,
                "unique": true
              },
              "custom_id": {
                "value": "custom-123",
                "priority": 2,
                "unique": false
              }
            }
          }
        }
      ]
    """

    val testConfigWithCustom = testConfig.copy(customIdentifiers = Some(customConfig))

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api = Identity.build(testConfigWithCustom, new RecordingHttpClient(ref).mock)
      _ <- api.addContexts(List(e))
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

    // Create multiple entities of the same schema type
    val testEntitySchema = SchemaKey("com.test", "user_context", "jsonschema", SchemaVer.Full(1, 0, 0))
    val entity0 = json"""{"customId": "id-from-first"}"""
    val entity1 = json"""{"customId": "id-from-second"}"""
    val entity2 = json"""{"customId": "id-from-third"}"""

    e.contexts = List(
      SelfDescribingData(testEntitySchema, entity0),
      SelfDescribingData(testEntitySchema, entity1),
      SelfDescribingData(testEntitySchema, entity2)
    )

    val customConfig = Config.CustomIdentifiers(
      identifiers = List(
        Config.Identifier(
          name = "first_custom_id",
          field = entityField("com.test", "user_context", 1, "$.customId", Some(0)),
          priority = 1,
          unique = false
        ),
        Config.Identifier(
          name = "second_custom_id",
          field = entityField("com.test", "user_context", 1, "$.customId", Some(1)),
          priority = 2,
          unique = false
        ),
        Config.Identifier(
          name = "third_custom_id",
          field = entityField("com.test", "user_context", 1, "$.customId", Some(2)),
          priority = 3,
          unique = false
        ),
        Config.Identifier(
          name = "default_custom_id",
          field = entityField("com.test", "user_context", 1, "$.customId", None),
          priority = 4,
          unique = false
        )
      ),
      filters = None
    )

    val expectedRequestBody = json"""
      [
        {
          "eventId": ${e.event_id},
          "identifiers": {
            "user_id": ${e.user_id},
            "custom": {
              "first_custom_id": {
                "value": "id-from-first",
                "priority": 1,
                "unique": false
              },
              "second_custom_id": {
                "value": "id-from-second",
                "priority": 2,
                "unique": false
              },
              "third_custom_id": {
                "value": "id-from-third",
                "priority": 3,
                "unique": false
              },
              "default_custom_id": {
                "value": "id-from-first",
                "priority": 4,
                "unique": false
              }
            }
          }
        }
      ]
    """

    val testConfigWithCustom = testConfig.copy(customIdentifiers = Some(customConfig))

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api = Identity.build(testConfigWithCustom, new RecordingHttpClient(ref).mock)
      _ <- api.addContexts(List(e))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        body must beEqualTo(expectedRequestBody.noSpaces)
    }
  }

  def e6 = {
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString
    e.user_id = "test-user-id"

    // Create a test unstructured event
    val testEventData = json"""{"eventUserId": "event-user-123"}"""
    val testEventSchema = SchemaKey("com.test", "login_event", "jsonschema", SchemaVer.Full(1, 0, 0))
    e.unstruct_event = Some(SelfDescribingData(testEventSchema, testEventData))

    val customConfig = Config.CustomIdentifiers(
      identifiers = List(
        Config.Identifier(
          name = "event_user_id",
          field = eventField("com.test", "login_event", 1, "$.eventUserId"),
          priority = 1,
          unique = false
        )
      ),
      filters = None
    )

    val expectedRequestBody = json"""
      [
        {
          "eventId": ${e.event_id},
          "identifiers": {
            "user_id": ${e.user_id},
            "custom": {
              "event_user_id": {
                "value": "event-user-123",
                "priority": 1,
                "unique": false
              }
            }
          }
        }
      ]
    """

    val testConfigWithCustom = testConfig.copy(customIdentifiers = Some(customConfig))

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api = Identity.build(testConfigWithCustom, new RecordingHttpClient(ref).mock)
      _ <- api.addContexts(List(e))
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
    e1.user_id = "test-user"

    val e2 = new EnrichedEvent()
    e2.event_id = UUID.randomUUID.toString
    e2.app_id = "blocked_app"
    e2.user_id = "test-user"

    val customConfig = Config.CustomIdentifiers(
      identifiers = List(
        Config.Identifier(
          name = "user_id",
          field = atomicField("user_id"),
          priority = 1,
          unique = true
        )
      ),
      filters = Some(
        Config.Filter(
          logic = Config.FilterLogic.All,
          rules = List(
            Config.FilterRule(
              field = atomicField("app_id"),
              operator = Config.FilterOperator.In,
              values = List("allowed_app")
            )
          )
        )
      )
    )

    // Only e1 should pass the filter, so we expect only one request
    val expectedRequestBody = json"""
      [
        {
          "eventId": ${e1.event_id},
          "identifiers": {
            "user_id": "test-user",
            "custom": {
              "user_id": {
                "value": "test-user",
                "priority": 1,
                "unique": true
              }
            }
          }
        }
      ]
    """

    val testConfigWithCustom = testConfig.copy(customIdentifiers = Some(customConfig))

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api = Identity.build(testConfigWithCustom, new RecordingHttpClient(ref).mock)
      _ <- api.addContexts(List(e1, e2))
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
    e1.user_id = "" // Empty user_id

    val e2 = new EnrichedEvent()
    e2.event_id = UUID.randomUUID.toString
    e2.app_id = "blocked_app"
    e2.user_id = "test-user" // Has user_id

    val customConfig = Config.CustomIdentifiers(
      identifiers = List(
        Config.Identifier(
          name = "user_id",
          field = atomicField("user_id"),
          priority = 1,
          unique = true
        )
      ),
      filters = Some(
        Config.Filter(
          logic = Config.FilterLogic.Any, // OR logic
          rules = List(
            Config.FilterRule(
              field = atomicField("app_id"),
              operator = Config.FilterOperator.In,
              values = List("allowed_app")
            ),
            Config.FilterRule(
              field = atomicField("user_id"),
              operator = Config.FilterOperator.In,
              values = List("test-user")
            )
          )
        )
      )
    )

    // Both events should pass because we use Any (OR) logic
    // e1: app_id matches
    // e2: user_id matches
    val testConfigWithCustom = testConfig.copy(customIdentifiers = Some(customConfig))

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api = Identity.build(testConfigWithCustom, new RecordingHttpClient(ref).mock)
      _ <- api.addContexts(List(e1, e2))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        // Both events should be in the request
        body must contain(e1.event_id)
        body must contain(e2.event_id)
    }
  }

  def e8 = {
    val e1 = new EnrichedEvent()
    e1.event_id = UUID.randomUUID.toString
    e1.app_id = "production_app"
    e1.user_id = "test-user"

    val e2 = new EnrichedEvent()
    e2.event_id = UUID.randomUUID.toString
    e2.app_id = "test_app" // Should be filtered out
    e2.user_id = "test-user"

    val customConfig = Config.CustomIdentifiers(
      identifiers = List(
        Config.Identifier(
          name = "user_id",
          field = atomicField("user_id"),
          priority = 1,
          unique = true
        )
      ),
      filters = Some(
        Config.Filter(
          logic = Config.FilterLogic.All,
          rules = List(
            Config.FilterRule(
              field = atomicField("app_id"),
              operator = Config.FilterOperator.NotIn, // NotIn operator
              values = List("test_app", "dev_app")
            )
          )
        )
      )
    )

    // Only e1 should pass (e2 has app_id in the blocked list)
    val expectedRequestBody = json"""
      [
        {
          "eventId": ${e1.event_id},
          "identifiers": {
            "user_id": "test-user",
            "custom": {
              "user_id": {
                "value": "test-user",
                "priority": 1,
                "unique": true
              }
            }
          }
        }
      ]
    """

    val testConfigWithCustom = testConfig.copy(customIdentifiers = Some(customConfig))

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api = Identity.build(testConfigWithCustom, new RecordingHttpClient(ref).mock)
      _ <- api.addContexts(List(e1, e2))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        body must beEqualTo(expectedRequestBody.noSpaces)
    }
  }

  def e10 = {
    val e1 = new EnrichedEvent()
    e1.event_id = UUID.randomUUID.toString
    e1.app_id = "dev_app" // In blocked list
    e1.user_id = "test-user"

    val e2 = new EnrichedEvent()
    e2.event_id = UUID.randomUUID.toString
    e2.app_id = "production_app" // Not in blocked list, should pass
    e2.user_id = "test-user"

    val e3 = new EnrichedEvent()
    e3.event_id = UUID.randomUUID.toString
    e3.app_id = "staging_app" // Not in blocked list, should pass
    e3.user_id = "test-user"

    val customConfig = Config.CustomIdentifiers(
      identifiers = List(
        Config.Identifier(
          name = "user_id",
          field = atomicField("user_id"),
          priority = 1,
          unique = true
        )
      ),
      filters = Some(
        Config.Filter(
          logic = Config.FilterLogic.Any, // OR logic
          rules = List(
            Config.FilterRule(
              field = atomicField("app_id"),
              operator = Config.FilterOperator.NotIn, // NotIn operator
              values = List("test_app", "dev_app")
            )
          )
        )
      )
    )

    // e2 and e3 should pass (not in blocked list)
    // e1 should be filtered out (in blocked list)
    val testConfigWithCustom = testConfig.copy(customIdentifiers = Some(customConfig))

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api = Identity.build(testConfigWithCustom, new RecordingHttpClient(ref).mock)
      _ <- api.addContexts(List(e1, e2, e3))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        // e2 and e3 should be in the request, e1 should not
        body must contain(e2.event_id)
        body must contain(e3.event_id)
        body must not(contain(e1.event_id))
    }
  }

  def e11 = {
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString
    e.user_id = "test-user-id"

    // Create only 2 entities, but try to access index 5
    val testEntitySchema = SchemaKey("com.test", "user_context", "jsonschema", SchemaVer.Full(1, 0, 0))
    val entity0 = json"""{"customId": "id-from-first"}"""
    val entity1 = json"""{"customId": "id-from-second"}"""

    e.contexts = List(
      SelfDescribingData(testEntitySchema, entity0),
      SelfDescribingData(testEntitySchema, entity1)
    )

    val customConfig = Config.CustomIdentifiers(
      identifiers = List(
        Config.Identifier(
          name = "out_of_bounds_id",
          field = entityField("com.test", "user_context", 1, "$.customId", Some(5)), // Index 5 doesn't exist
          priority = 1,
          unique = false
        )
      ),
      filters = None
    )

    // Should only send standard identifiers, no custom (because extraction failed)
    val expectedRequestBody = json"""
      [
        {
          "eventId": ${e.event_id},
          "identifiers": {
            "user_id": ${e.user_id}
          }
        }
      ]
    """

    val testConfigWithCustom = testConfig.copy(customIdentifiers = Some(customConfig))

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api = Identity.build(testConfigWithCustom, new RecordingHttpClient(ref).mock)
      _ <- api.addContexts(List(e))
      result <- ref.get
    } yield result must beLike {
      case List((_, body)) =>
        body must beEqualTo(expectedRequestBody.noSpaces)
    }
  }

  def e12 = {
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString
    // app_id is null/missing
    e.user_id = "test-user"

    val customConfig = Config.CustomIdentifiers(
      identifiers = List(
        Config.Identifier(
          name = "user_id",
          field = atomicField("user_id"),
          priority = 1,
          unique = true
        )
      ),
      filters = Some(
        Config.Filter(
          logic = Config.FilterLogic.All,
          rules = List(
            Config.FilterRule(
              field = atomicField("app_id"),
              operator = Config.FilterOperator.In, // In operator with missing field
              values = List("allowed_app")
            )
          )
        )
      )
    )

    val testConfigWithCustom = testConfig.copy(customIdentifiers = Some(customConfig))

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api = Identity.build(testConfigWithCustom, new RecordingHttpClient(ref).mock)
      _ <- api.addContexts(List(e))
      result <- ref.get
    } yield
    // Per the identity documentation (line 610-612), when filter evaluation fails
    // (missing field), the event should be filtered out and NOT sent to the API
    // No HTTP request should be made when all events are filtered out
    result must beEqualTo(List.empty[(Request[IO], String)])
  }

  def e13 = {
    val e = new EnrichedEvent()
    e.event_id = UUID.randomUUID.toString
    // app_id is null/missing
    e.user_id = "test-user"

    val customConfig = Config.CustomIdentifiers(
      identifiers = List(
        Config.Identifier(
          name = "user_id",
          field = atomicField("user_id"),
          priority = 1,
          unique = true
        )
      ),
      filters = Some(
        Config.Filter(
          logic = Config.FilterLogic.All,
          rules = List(
            Config.FilterRule(
              field = atomicField("app_id"),
              operator = Config.FilterOperator.NotIn, // NotIn operator with missing field
              values = List("blocked_app")
            )
          )
        )
      )
    )

    val testConfigWithCustom = testConfig.copy(customIdentifiers = Some(customConfig))

    for {
      ref <- Ref[IO].of(List.empty[(Request[IO], String)])
      api = Identity.build(testConfigWithCustom, new RecordingHttpClient(ref).mock)
      _ <- api.addContexts(List(e))
      result <- ref.get
    } yield
    // Per the identity documentation (line 610-612), when filter evaluation fails
    // (missing field), the event should be filtered out and NOT sent to the API
    // This test documents that the NotIn operator filters out events
    // when the field value cannot be extracted (same behavior as In operator in e12)
    // No HTTP request should be made when all events are filtered out
    result must beEqualTo(List.empty[(Request[IO], String)])
  }
}

object IdentitySpec {
  case class IdPair(eventId: String, snowplowId: String)

  // Helper to create Atomic with resolved Field
  def atomicField(name: String): Config.IdentifierField.Atomic =
    Config.IdentifierField.Atomic(EnrichedEvent.atomicFieldsByName(name))

  // Helper to create Event with compiled path
  def eventField(
    vendor: String,
    name: String,
    majorVersion: Int,
    path: String
  ): Config.IdentifierField.Event = {
    val compiled = compileQuery(path).toOption.get
    Config.IdentifierField.Event(vendor, name, majorVersion, compiled)
  }

  // Helper to create Entity with compiled path
  def entityField(
    vendor: String,
    name: String,
    majorVersion: Int,
    path: String,
    index: Option[Int]
  ): Config.IdentifierField.Entity = {
    val compiled = compileQuery(path).toOption.get
    Config.IdentifierField.Entity(vendor, name, majorVersion, index, compiled)
  }

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
                                        Retrying.Config.ForTransient(100.millis, 1),
                                        None
  )

}
