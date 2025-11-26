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
package com.snowplowanalytics.snowplow.enrich.kinesis

import java.util.UUID

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.kernel.Resource

import cats.effect.testing.specs2.CatsResource

import org.specs2.mutable.SpecificationLike

import com.snowplowanalytics.snowplow.enrich.core.DockerPull
import com.snowplowanalytics.snowplow.enrich.core.Utils

import com.snowplowanalytics.snowplow.enrich.kinesis.enrichments._
import com.snowplowanalytics.snowplow.enrich.kinesis.utils._

case class TestResources(localstack: Localstack, statsdAdmin: StatsdAdmin)

class DropEventsSpec extends CatsResource[IO, TestResources] with SpecificationLike {
  override protected val Timeout = Utils.TestTimeout
  override protected val ResourceTimeout = 3.minutes

  override def beforeAll(): Unit = {
    DockerPull.pull(Containers.Images.Localstack.image, Containers.Images.Localstack.tag)
    DockerPull.pull(Containers.Images.Statsd.image, Containers.Images.Statsd.tag)
    super.beforeAll()
  }

  override val resource: Resource[IO, TestResources] =
    for {
      localstack <- Containers.localstack
      statsd <- Containers.statsdServer(localstack.container.network)
      statsdHost = statsd.container.getHost()
      statsdAdminPort = statsd.container.getMappedPort(8126)
      statsdAdmin <- mkStatsdAdmin(statsdHost, statsdAdminPort)
    } yield TestResources(localstack, statsdAdmin)

  "enrich-kinesis" should {
    "drop events with failed events enabled" in withResource { testResources =>
      commonDropTest(testResources, emitFailed = true)
    }

    "drop events with failed events disabled" in withResource { testResources =>
      commonDropTest(testResources, emitFailed = false)
    }
  }

  private def commonDropTest(testResources: TestResources, emitFailed: Boolean) = {
    val testName = "drop"
    val nbGood = 100L
    val nbBad = 100L
    val nbGoodDrop = 100L
    val nbBadDrop = 100L
    val uuid = UUID.randomUUID().toString

    val enrichments = List(
      JavascriptDropEvent
    )

    val configPath =
      if (emitFailed)
        "modules/it/kinesis/src/test/resources/enrich/enrich-localstack-statsd.hocon"
      else
        "modules/it/kinesis/src/test/resources/enrich/enrich-localstack-failed-disabled.hocon"

    Containers
      .enrich(
        testResources.localstack,
        configPath = configPath,
        testName = testName,
        enrichments = enrichments,
        uuid = uuid
      )
      .use { enrichKinesis =>
        for {
          output <- run(enrichKinesis, nbGood, nbBad, nbGoodDrop, nbBadDrop)
          counters <- testResources.statsdAdmin.getCounters
        } yield {
          output.enriched.size.toLong must beEqualTo(nbGood)
          output.bad.size.toLong must beEqualTo(nbBad)
          counters must contain(s"'snowplow.enrich.raw;env=$uuid': ${nbGood + nbBad + nbGoodDrop + nbBadDrop}")
          counters must contain(s"'snowplow.enrich.good;env=$uuid': $nbGood")
          counters must contain(s"'snowplow.enrich.bad;env=$uuid': $nbBad")
          counters must contain(s"'snowplow.enrich.dropped;env=$uuid': ${nbGoodDrop + nbBadDrop}")
          if (emitFailed) {
            output.failed.size.toLong must beEqualTo(nbBad)
            counters must contain(s"'snowplow.enrich.failed;env=$uuid': $nbBad")
          } else {
            output.failed.size.toLong must beEqualTo(0)
            counters must contain(s"'snowplow.enrich.failed;env=$uuid': 0")
          }
        }
      }
  }
}
