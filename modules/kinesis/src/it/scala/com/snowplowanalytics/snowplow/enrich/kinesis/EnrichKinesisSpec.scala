/*
 * Copyright (c) 2022-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
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
import org.specs2.specification.BeforeAll

import com.snowplowanalytics.snowplow.enrich.kinesis.enrichments._

import com.snowplowanalytics.snowplow.enrich.common.fs2.test.CollectorPayloadGen

import com.snowplowanalytics.snowplow.enrich.kinesis.Containers.Localstack

class EnrichKinesisSpec extends CatsResource[IO, Localstack] with SpecificationLike with BeforeAll {

  override protected val Timeout = 10.minutes

  override def beforeAll(): Unit = {
    DockerPull.pull(Containers.Images.Localstack.image, Containers.Images.Localstack.tag)
    DockerPull.pull(Containers.Images.MySQL.image, Containers.Images.MySQL.tag)
    DockerPull.pull(Containers.Images.HTTP.image, Containers.Images.HTTP.tag)
    DockerPull.pull(Containers.Images.Statsd.image, Containers.Images.Statsd.tag)
    super.beforeAll()
  }

  override val resource: Resource[IO, Localstack] = Containers.localstack

  "enrich-kinesis" should {
    "be able to parse the minimal config" in withResource { localstack =>
      Containers.enrich(
        localstack,
        configPath = "config/config.kinesis.minimal.hocon",
        testName = "minimal",
        enrichments = Nil
      ).use { e =>
        IO(e.container.getLogs must contain("Running Enrich"))
      }
    }

    "emit the correct number of enriched events, bad rows and incomplete events" in withResource { localstack =>
      import utils._

      val testName = "count"
      val nbGood = 1000l
      val nbBad = 100l
      val uuid = UUID.randomUUID().toString

      val resources = for {
        _ <- Containers.enrich(
          localstack,
          configPath = "modules/kinesis/src/it/resources/enrich/enrich-localstack.hocon",
          testName = testName,
          enrichments = Nil,
          uuid = uuid
        )
        enrichPipe <- mkEnrichPipe(localstack.mappedPort, uuid)
      } yield enrichPipe

      val input = CollectorPayloadGen.generate[IO](nbGood, nbBad)

      resources.use { enrich =>
        for {
          output <- enrich(input).compile.toList
          (good, bad, incomplete) = parseOutput(output, testName)
        } yield {
          good.size.toLong must beEqualTo(nbGood)
          bad.size.toLong must beEqualTo(nbBad)
          incomplete.size.toLong must beEqualTo(nbBad)
        }
      }
    }

    "send the metrics to StatsD" in withResource { localstack =>
      import utils._

      val testName = "statsd"
      val nbGood = 100l
      val nbBad = 10l
      val uuid = UUID.randomUUID().toString

      val resources = for {
        statsd <- Containers.statsdServer
        statsdHost = statsd.container.getHost()
        statsdAdminPort = statsd.container.getMappedPort(8126)
        statsdAdmin <- mkStatsdAdmin(statsdHost, statsdAdminPort)
        _ <- Containers.enrich(
          localstack,
          configPath = "modules/kinesis/src/it/resources/enrich/enrich-localstack-statsd.hocon",
          testName = testName,
          enrichments = Nil,
          uuid = uuid
        )
        enrichPipe <- mkEnrichPipe(localstack.mappedPort, uuid)
      } yield (enrichPipe, statsdAdmin)

      val input = CollectorPayloadGen.generate[IO](nbGood, nbBad)

      resources.use { case (enrich, statsdAdmin) =>
        for {
          output <- enrich(input).compile.toList
          (good, bad, incomplete) = parseOutput(output, testName)
          counters <- statsdAdmin.getCounters
          gauges <- statsdAdmin.getGauges
        } yield {
          good.size.toLong must beEqualTo(nbGood)
          bad.size.toLong must beEqualTo(nbBad)
          incomplete.size.toLong must beEqualTo(nbBad)
          counters must contain(s"'snowplow.enrich.raw;env=test': ${nbGood + nbBad}")
          counters must contain(s"'snowplow.enrich.good;env=test': $nbGood")
          counters must contain(s"'snowplow.enrich.bad;env=test': $nbBad")
          counters must contain(s"'snowplow.enrich.invalid_enriched;env=test': 0")
          counters must contain(s"'snowplow.enrich.incomplete;env=test': $nbBad")
          gauges must contain(s"'snowplow.enrich.latency;env=test': ")
        }
      }
    }

    "run the enrichments and attach their context" in withResource { localstack =>
      import utils._

      val testName = "enrichments"
      val nbGood = 1000l
      val uuid = UUID.randomUUID().toString

      val enrichments = List(
        ApiRequest,
        Javascript,
        SqlQuery,
        Yauaa
      )

      val enrichmentsContexts = enrichments.map(_.outputSchema)

      val resources = for {
        _ <- Containers.mysqlServer
        _ <- Containers.httpServer
        _ <- Containers.enrich(
          localstack,
          configPath = "modules/kinesis/src/it/resources/enrich/enrich-localstack.hocon",
          testName = testName,
          enrichments = enrichments,
          uuid = uuid
        )
        enrichPipe <- mkEnrichPipe(localstack.mappedPort, uuid)
      } yield enrichPipe

      val input = CollectorPayloadGen.generate[IO](nbGood)

      resources.use { enrich =>
        for {
          output <- enrich(input).compile.toList
          (good, bad, incomplete) = parseOutput(output, testName)
        } yield {
          good.size.toLong must beEqualTo(nbGood)
          good.map { enriched =>
            enriched.derived_contexts.data.map(_.schema) must containTheSameElementsAs(enrichmentsContexts)
          }
          bad.size.toLong must beEqualTo(0l)
          incomplete.size.toLong must beEqualTo(0l)
        }
      }
    }

    "shutdown when it receives a SIGTERM" in withResource { localstack =>
      Containers.enrich(
        localstack,
        configPath = "modules/kinesis/src/it/resources/enrich/enrich-localstack.hocon",
        testName = "stop",
        enrichments = Nil,
        waitLogMessage = "enrich.metrics"
      ).use { enrich =>
        for {
          _ <- IO(println("stop - Sending signal"))
          _ <- IO(enrich.container.getDockerClient().killContainerCmd(enrich.container.getContainerId()).withSignal("TERM").exec())
          _ <- Containers.waitUntilStopped(enrich)
        } yield {
          enrich.container.isRunning() must beFalse
          enrich.container.getLogs() must contain("Enrich stopped")
        }
      }
    }
  }
}
