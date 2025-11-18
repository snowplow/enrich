/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.kafka

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.kernel.Resource

import cats.effect.testing.specs2.CatsResource

import com.snowplowanalytics.snowplow.enrich.core.DockerPull
import com.snowplowanalytics.snowplow.enrich.core.Utils

import org.specs2.mutable.SpecificationLike

class EnrichKafkaSpec extends CatsResource[IO, EnrichKafka] with SpecificationLike {

  override protected val Timeout = Utils.TestTimeout
  override protected val ResourceTimeout = 2.minutes

  override def beforeAll(): Unit = {
    DockerPull.pull(Containers.Images.Kafka.image, Containers.Images.Kafka.tag)
    super.beforeAll()
  }

  override val resource: Resource[IO, EnrichKafka] = Containers.resource

  "enrich-kafka" should {
    "emit the correct number of enriched events, failed events and bad rows" in withResource { enrichKafka =>
      val nbEnriched = 1000L
      val nbBad = 100L

      utils.run(enrichKafka, nbEnriched, nbBad).map { output =>
        output.enriched.size.toLong must beEqualTo(nbEnriched)
        output.failed.size.toLong must beEqualTo(nbBad)
        output.bad.size.toLong must beEqualTo(nbBad)
      }
    }
  }
}
