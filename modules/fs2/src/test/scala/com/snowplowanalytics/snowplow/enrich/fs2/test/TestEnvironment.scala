/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.fs2.test

import java.nio.file.Paths

import scala.concurrent.duration._

import cats.Monad
import cats.syntax.either._

import cats.effect.{Blocker, Concurrent, ContextShift, IO, Resource, Timer}
import cats.effect.concurrent.Ref

import io.circe.Json

import fs2.concurrent.Queue

import com.snowplowanalytics.iglu.client.{CirceValidator, Client, Resolver}
import com.snowplowanalytics.iglu.client.resolver.registries.Registry

import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

import com.snowplowanalytics.snowplow.enrich.fs2.{Assets, Enrich, EnrichSpec, Environment, Payload, RawSource}
import com.snowplowanalytics.snowplow.enrich.fs2.Environment.Enrichments
import com.snowplowanalytics.snowplow.enrich.fs2.SpecHelpers.{filesResource, ioClock}
import com.snowplowanalytics.snowplow.enrich.fs2.config
import com.snowplowanalytics.snowplow.enrich.fs2.io.Tracing

import cats.effect.testing.specs2.CatsIO

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

case class TestEnvironment(
  env: Environment[IO],
  counter: Ref[IO, Counter],
  good: Queue[IO, Payload[IO, EnrichedEvent]],
  bad: Queue[IO, Payload[IO, BadRow]]
) {

  /**
   * Run all streams for 5 seconds and get produced events as a list
   * Assets and metrics streams are empty by default, can be enabled
   * by updating default [[Environment]]
   * If assets stream is enabled, first events get enriched with assets
   * downloaded by [[Assets.State.make]], not by [[Assets.run]]
   * @param updateEnv function to update an environment created by
   *                  [[TestEnvironment.make]]
   */
  def run(
    updateEnv: Environment[IO] => Environment[IO] = identity
  )(
    implicit C: Concurrent[IO],
    CS: ContextShift[IO],
    T: Timer[IO]
  ): IO[List[Either[BadRow, Event]]] = {
    val updatedEnv = updateEnv(env)

    val pauses = updatedEnv.pauseEnrich.discrete.evalMap(p => TestEnvironment.logger.info(s"Pause signal is $p"))
    val stream = Enrich.run[IO](updatedEnv).merge(Assets.run[IO](updatedEnv)).merge(pauses)
    bad.dequeue
      .either(good.dequeue)
      .concurrently(stream)
      .haltAfter(5.seconds)
      .compile
      .toList
      .map { rows =>
        rows.map(_.fold(_.data.asLeft, event => EnrichSpec.normalize(event).toEither))
      }
  }
}

object TestEnvironment extends CatsIO {

  val logger: Logger[IO] =
    Slf4jLogger.getLogger[IO]

  val enrichmentReg: EnrichmentRegistry[IO] =
    EnrichmentRegistry[IO]()
  val enrichments: Environment.Enrichments[IO] =
    Environment.Enrichments(enrichmentReg, Nil)

  val ioBlocker: Resource[IO, Blocker] = Blocker[IO]

  val embeddedRegistry =
    Registry.InMemory(
      Registry.Config("fs2-enrich embedded test registry", 1, List("com.acme")),
      List(
        SchemaRegistry.unstructEvent,
        SchemaRegistry.contexts,
        SchemaRegistry.geolocationContext,
        SchemaRegistry.iabAbdRobots,
        SchemaRegistry.yauaaContext,
        SchemaRegistry.acmeTest,
        SchemaRegistry.acmeOutput
      )
    )
  val igluClient: Client[IO, Json] =
    Client[IO, Json](Resolver(List(embeddedRegistry), None), CirceValidator)

  /**
   * A dummy test environment without enrichmenta and with noop sinks and sources
   * One can replace stream and sinks via `.copy`
   */
  def make(source: RawSource[IO], enrichments: List[EnrichmentConf] = Nil): Resource[IO, TestEnvironment] =
    for {
      blocker <- ioBlocker
      _ <- filesResource(blocker, enrichments.flatMap(_.filesToCache).map(p => Paths.get(p._2)))
      counter <- Resource.liftF(Counter.make[IO])
      goodQueue <- Resource.liftF(Queue.unbounded[IO, Payload[IO, EnrichedEvent]])
      badQueue <- Resource.liftF(Queue.unbounded[IO, Payload[IO, BadRow]])
      metrics = Counter.mkCounterMetrics[IO](counter)(Monad[IO], ioClock)
      pauseEnrich <- Environment.makePause[IO]
      assets <- Assets.State.make(blocker, pauseEnrich, enrichments.flatMap(_.filesToCache))
      _ <- Resource.liftF(logger.info("AssetsState initialized"))
      enrichmentsRef <- Enrichments.make[IO](enrichments)
      tracing <- Tracing.build[IO](config.Tracing("http://localhost:9411/api/v2/spans", None))
      environment = Environment[IO](igluClient,
                                    enrichmentsRef,
                                    pauseEnrich,
                                    assets,
                                    blocker,
                                    source,
                                    goodQueue.enqueue,
                                    badQueue.enqueue,
                                    None,
                                    metrics,
                                    None,
                                    None,
                                    Some(tracing)
                    )
      _ <- Resource.liftF(pauseEnrich.set(false) *> logger.info("TestEnvironment initialized"))
    } yield TestEnvironment(environment, counter, goodQueue, badQueue)
}
