/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.fs2.test

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Paths

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.Monad

import cats.effect.{Blocker, Concurrent, ContextShift, IO, Resource, Timer}
import cats.effect.concurrent.Ref
import cats.effect.testing.specs2.CatsIO

import fs2.Stream

import io.circe.{Json, parser}

import com.snowplowanalytics.iglu.client.{CirceValidator, Client, Resolver}
import com.snowplowanalytics.iglu.client.resolver.registries.{Http4sRegistryLookup, Registry}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.badrows.BadRow

import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.utils.BlockerF

import com.snowplowanalytics.snowplow.enrich.common.fs2.{Assets, AttributedData, Enrich, EnrichSpec, Environment}
import com.snowplowanalytics.snowplow.enrich.common.fs2.Environment.{Enrichments, StreamsSettings}
import com.snowplowanalytics.snowplow.enrich.common.fs2.SpecHelpers.{filesResource, ioClock}
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Concurrency, Telemetry}
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Clients

case class TestEnvironment[A](
  env: Environment[IO, A],
  counter: Ref[IO, Counter],
  good: IO[Vector[AttributedData[Array[Byte]]]],
  pii: IO[Vector[AttributedData[Array[Byte]]]],
  bad: IO[Vector[Array[Byte]]]
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
    updateEnv: Environment[IO, A] => Environment[IO, A] = identity
  )(
    implicit C: Concurrent[IO],
    CS: ContextShift[IO],
    T: Timer[IO]
  ): IO[(Vector[BadRow], Vector[Event], Vector[Event])] = {
    val updatedEnv = updateEnv(env)

    val pauses = updatedEnv.pauseEnrich.discrete.evalMap(p => TestEnvironment.logger.info(s"Pause signal is $p"))
    val stream = Enrich.run[IO, A](updatedEnv, false).merge(Assets.run[IO, A](updatedEnv)).merge(pauses)
    for {
      _ <- stream.haltAfter(5.seconds).compile.drain
      goodVec <- good
      piiVec <- pii
      badVec <- bad
    } yield (badVec.map(TestEnvironment.parseBad(_)),
             piiVec.flatMap(p => EnrichSpec.normalize(new String(p.data, UTF_8)).toOption),
             goodVec.flatMap(g => EnrichSpec.normalize(new String(g.data, UTF_8)).toOption)
    )
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
      Registry.Config("snowplow-enrich-pubsub embedded test registry", 1, List("com.acme")),
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
   * A dummy test environment without enrichment and with noop sinks and sources
   * One can replace stream and sinks via `.copy`
   */
  def make(source: Stream[IO, Array[Byte]], enrichments: List[EnrichmentConf] = Nil): Resource[IO, TestEnvironment[Array[Byte]]] =
    for {
      http <- Clients.mkHttp[IO](ExecutionContext.global)
      blocker <- ioBlocker
      _ <- filesResource(blocker, enrichments.flatMap(_.filesToCache).map(p => Paths.get(p._2)))
      counter <- Resource.eval(Counter.make[IO])
      metrics = Counter.mkCounterMetrics[IO](counter)(Monad[IO], ioClock)
      pauseEnrich <- Environment.makePause[IO]
      clients = Clients.init[IO](http, Nil)
      assets <- Assets.State.make(blocker, pauseEnrich, enrichments.flatMap(_.filesToCache), clients)
      _ <- Resource.eval(logger.info("AssetsState initialized"))
      enrichmentsRef <- Enrichments.make[IO](enrichments, BlockerF.ofBlocker(blocker))
      goodRef <- Resource.eval(Ref.of[IO, Vector[AttributedData[Array[Byte]]]](Vector.empty))
      piiRef <- Resource.eval(Ref.of[IO, Vector[AttributedData[Array[Byte]]]](Vector.empty))
      badRef <- Resource.eval(Ref.of[IO, Vector[Array[Byte]]](Vector.empty))
      environment = Environment[IO, Array[Byte]](
                      igluClient,
                      Http4sRegistryLookup(http),
                      enrichmentsRef,
                      pauseEnrich,
                      assets,
                      http,
                      blocker,
                      source,
                      g => goodRef.update(_ :+ g),
                      Some(p => piiRef.update(_ :+ p)),
                      b => badRef.update(_ :+ b),
                       _.map(_ => ()),
                      identity,
                      None,
                      metrics,
                      None,
                      _ => Map.empty,
                      _ => Map.empty,
                      Telemetry(true, 1.minute, "POST", "foo.bar", 1234, true, None, None, None, None, None),
                      EnrichSpec.processor,
                      StreamsSettings(Concurrency(10000, 64), 1024 * 1024),
                      None,
                      None
                    )
      _ <- Resource.eval(pauseEnrich.set(false) *> logger.info("TestEnvironment initialized"))
    } yield TestEnvironment(environment, counter, goodRef.get, piiRef.get, badRef.get)

  def parseBad(bytes: Array[Byte]): BadRow =
    parser
      .parse(new String(bytes, UTF_8))
      .getOrElse(throw new RuntimeException("Error parsing bad row json"))
      .as[BadRow]
      .getOrElse(throw new RuntimeException("Error decoding bad row"))
}
