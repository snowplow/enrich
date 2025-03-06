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

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.specs2.CatsResource
import com.snowplowanalytics.snowplow.enrich.common.fs2.test.CollectorPayloadGen
import com.snowplowanalytics.snowplow.enrich.kinesis.enrichments._
import com.snowplowanalytics.snowplow.enrich.kinesis.utils._
import org.specs2.mutable.SpecificationLike

import java.util.UUID
import scala.concurrent.duration._

class DropEventsSpec extends CatsResource[IO, KinesisTestResources] with SpecificationLike {

  override protected val Timeout = 60.minutes

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
    "drop events when incomplete events enabled" in withResource { testResources =>
      commonDropTest(testResources, emitIncomplete = true)
    }

    "drop events when incomplete events disabled" in withResource { testResources =>
      commonDropTest(testResources, emitIncomplete = false)
    }
  }

  private def commonDropTest(testResources: KinesisTestResources, emitIncomplete: Boolean) = {
    val testName = "enrichments"
    val nbGood = 100l
    val nbBad = 100l
    val nbGoodDrop = 100l
    val nbBadDrop = 100l
    val uuid = UUID.randomUUID().toString

    val enrichments = List(
      JavascriptDropEvent
    )

    val configPath =
      if (emitIncomplete)
        "modules/kinesis/src/it/resources/enrich/enrich-localstack-statsd.hocon"
      else
        "modules/kinesis/src/it/resources/enrich/enrich-localstack-incomplete-disabled.hocon"

    val resources = for {
      _ <- Containers.enrich(
        testResources.localstack,
        configPath = configPath,
        testName = testName,
        enrichments = enrichments,
        uuid = uuid
      )
      enrichPipe <- mkEnrichPipe(testResources.localstack.mappedPort, uuid)
    } yield enrichPipe

    val input = CollectorPayloadGen.generate[IO](
      nbGoodEvents = nbGood,
      nbBadRows = nbBad,
      nbGoodDroppedEvents = nbGoodDrop,
      nbBadDroppedEvents = nbBadDrop
    )

    resources.use { enrich =>
      for {
        output <- enrich(input).compile.toList
        (good, bad, incomplete) = parseOutput(output, testName)
        counters <- testResources.statsdAdmin.getCounters
      } yield {
        good.size.toLong must beEqualTo(nbGood)
        bad.size.toLong must beEqualTo(nbBad)
        counters must contain(s"'snowplow.enrich.raw;env=$uuid': ${nbGood + nbBad + nbGoodDrop + nbBadDrop}")
        counters must contain(s"'snowplow.enrich.good;env=$uuid': $nbGood")
        counters must contain(s"'snowplow.enrich.bad;env=$uuid': $nbBad")
        counters must contain(s"'snowplow.enrich.dropped;env=$uuid': ${nbGoodDrop + nbBadDrop}")
        counters must contain(s"'snowplow.enrich.invalid_enriched;env=$uuid': 0")
        if (emitIncomplete) {
          incomplete.size.toLong must beEqualTo(nbBad)
          counters must contain(s"'snowplow.enrich.incomplete;env=$uuid': $nbBad")
        } else {
          incomplete.size.toLong must beEqualTo(0)
          counters must not(contain(s"'snowplow.enrich.incomplete;env=$uuid'"))
        }
      }
    }
  }
}
