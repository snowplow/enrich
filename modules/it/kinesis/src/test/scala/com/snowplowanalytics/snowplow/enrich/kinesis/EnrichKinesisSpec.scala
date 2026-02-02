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

class EnrichKinesisSpec extends CatsResource[IO, TestInfrastructure] with SpecificationLike {

  import utils._

  override protected val Timeout = Utils.TestTimeout
  override protected val ResourceTimeout = 5.minutes

  override def beforeAll(): Unit = {
    DockerPull.pull(Containers.Images.Localstack.image, Containers.Images.Localstack.tag)
    DockerPull.pull(Containers.Images.MySQL.image, Containers.Images.MySQL.tag)
    DockerPull.pull(Containers.Images.HTTP.image, Containers.Images.HTTP.tag)
    DockerPull.pull(Containers.Images.Statsd.image, Containers.Images.Statsd.tag)
    super.beforeAll()
  }

  override val resource: Resource[IO, TestInfrastructure] = Containers.allContainers

  "enrich-kinesis" should {
    "emit the correct number of enriched events, failed events and bad rows" in withResource { infrastructure =>
      val testName = "count"
      val nbEnriched = 1000L
      val nbBad = 100L
      val uuid = UUID.randomUUID().toString

      Containers
        .enrich(
          infrastructure.localstack,
          configPath = "modules/it/kinesis/src/test/resources/enrich/enrich-localstack.hocon",
          testName = testName,
          enrichments = Nil,
          uuid = uuid
        )
        .use { enrichKinesis =>
          run(enrichKinesis, nbEnriched, nbBad).map { output =>
            output.enriched.size.toLong must beEqualTo(nbEnriched)
            output.failed.size.toLong must beEqualTo(nbBad)
            output.bad.size.toLong must beEqualTo(nbBad)
          }
        }
    }

    "send the metrics to StatsD" in withResource { infrastructure =>
      val testName = "statsd"
      val nbEnriched = 100L
      val nbBad = 10L
      val uuid = UUID.randomUUID().toString

      val statsdHost = infrastructure.statsd.container.getHost
      val statsdAdminPort = infrastructure.statsd.container.getMappedPort(8126)

      val resources = for {
        statsdAdmin <- mkStatsdAdmin(statsdHost, statsdAdminPort)
        enrichKinesis <- Containers.enrich(
                           infrastructure.localstack,
                           configPath = "modules/it/kinesis/src/test/resources/enrich/enrich-localstack-statsd.hocon",
                           testName = testName,
                           enrichments = Nil,
                           uuid = uuid
                         )
      } yield (enrichKinesis, statsdAdmin)

      resources.use {
        case (enrichKinesis, statsdAdmin) =>
          for {
            output <- run(enrichKinesis, nbEnriched, nbBad)
            counters <- statsdAdmin.getCounters
            gauges <- statsdAdmin.getGauges
          } yield {
            output.enriched.size.toLong must beEqualTo(nbEnriched)
            output.failed.size.toLong must beEqualTo(nbBad)
            output.bad.size.toLong must beEqualTo(nbBad)
            counters must contain(s"'snowplow.enrich.raw;env=$uuid': ${nbEnriched + nbBad}")
            counters must contain(s"'snowplow.enrich.good;env=$uuid': $nbEnriched")
            counters must contain(s"'snowplow.enrich.failed;env=$uuid': $nbBad")
            counters must contain(s"'snowplow.enrich.bad;env=$uuid': $nbBad")
            counters must contain(s"'snowplow.enrich.dropped;env=$uuid': 0")
            gauges must contain(s"'snowplow.enrich.e2e_latency_millis;env=$uuid': ")
          }
      }
    }

    "run the enrichments and attach their context" in withResource { infrastructure =>
      val testName = "enrichments"
      val nbEnriched = 1000L
      val uuid = UUID.randomUUID().toString

      val enrichments = List(
        ApiRequest,
        Javascript,
        SqlQuery,
        Yauaa
      )

      val enrichmentsContexts = enrichments.map(_.outputSchema)

      Containers
        .enrich(
          infrastructure.localstack,
          configPath = "modules/it/kinesis/src/test/resources/enrich/enrich-localstack.hocon",
          testName = testName,
          enrichments = enrichments,
          uuid = uuid
        )
        .use { enrichKinesis =>
          run(enrichKinesis, nbEnriched).map { output =>
            output.bad.foreach(badRow => println(badRow.compact))
            output.bad.size.toLong must beEqualTo(0L)
            output.failed.size.toLong must beEqualTo(0L)
            output.enriched.size.toLong must beEqualTo(nbEnriched)
            output.enriched.map { enriched =>
              enriched.derived_contexts.data.map(_.schema) must containTheSameElementsAs(enrichmentsContexts)
            }
          }
        }
    }

    "shutdown when it receives a SIGTERM" in withResource { infrastructure =>
      Containers
        .enrich(
          infrastructure.localstack,
          configPath = "modules/it/kinesis/src/test/resources/enrich/enrich-localstack.hocon",
          testName = "stop",
          enrichments = Nil
        )
        .use { enrich =>
          for {
            _ <- IO(
                   enrich.container.container.getDockerClient
                     .killContainerCmd(enrich.container.container.getContainerId)
                     .withSignal("TERM")
                     .exec()
                 )
            _ <- Containers.waitUntilStopped(enrich.container)
          } yield enrich.container.container.isRunning must beFalse
        }
    }
  }
}
