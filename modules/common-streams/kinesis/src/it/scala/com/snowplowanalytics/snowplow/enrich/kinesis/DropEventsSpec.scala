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
package com.snowplowanalytics.snowplow.enrich.streams.kinesis

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.specs2.CatsResource
import org.specs2.mutable.SpecificationLike

import java.util.UUID
import scala.concurrent.duration._

import com.snowplowanalytics.snowplow.enrich.streams.common.CollectorPayloadGen
import com.snowplowanalytics.snowplow.enrich.streams.kinesis.enrichments._
import com.snowplowanalytics.snowplow.enrich.streams.kinesis.utils._

class DropEventsSpec extends CatsResource[IO, KinesisTestResources] with SpecificationLike {

  override protected val Timeout = 10.minutes

  override def beforeAll(): Unit = {
    DockerPull.pull(Containers.Images.Localstack.image, Containers.Images.Localstack.tag)
    DockerPull.pull(Containers.Images.Statsd.image, Containers.Images.Statsd.tag)
    super.beforeAll()
  }

  override val resource: Resource[IO, KinesisTestResources] =
    for {
      localstack <- Containers.localstack
      statsd <- Containers.statsdServer
      statsdHost = statsd.container.getHost()
      statsdAdminPort = statsd.container.getMappedPort(8126)
      statsdAdmin <- mkStatsdAdmin(statsdHost, statsdAdminPort)
    } yield KinesisTestResources(localstack, statsdAdmin)

  "enrich-kinesis" should {
    "drop events with failed events enabled" in withResource { testResources =>
      commonDropTest(testResources, emitFailed = true)
    }

    "drop events with failed events disabled" in withResource { testResources =>
      commonDropTest(testResources, emitFailed = false)
    }
  }

  private def commonDropTest(testResources: KinesisTestResources, emitFailed: Boolean) = {
    val testName = "drop"
    val nbGood = 100L
    val nbBad = 100L
    val nbGoodDrop = 100L
    val nbBadDrop = 100L
    val uuid = UUID.randomUUID().toString

    val enrichments = List(
      JavascriptDropEvent
    )

    val input = CollectorPayloadGen.generate[IO](
      nbGoodEvents = nbGood,
      nbBadRows = nbBad,
      nbGoodDroppedEvents = nbGoodDrop,
      nbBadDroppedEvents = nbBadDrop
    )

    val configPath =
      if (emitFailed)
        "modules/common-streams/kinesis/src/it/resources/enrich/enrich-localstack-statsd.hocon"
      else
        "modules/common-streams/kinesis/src/it/resources/enrich/enrich-localstack-failed-disabled.hocon"

    Containers
      .enrich(
        testResources.localstack,
        configPath = configPath,
        testName = testName,
        enrichments = enrichments,
        uuid = uuid
      )
      .use { _ =>
        for {
          output <- runEnrichPipe(input, testResources.localstack.mappedPort, uuid)
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
