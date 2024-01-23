/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.fs2.test

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource}

import io.circe.literal._

import fs2.io.readInputStream

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import org.http4s.HttpRoutes
import org.http4s.Method.GET
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.dsl.io._
import org.http4s.syntax.all._

import cats.effect.testing.specs2.CatsEffect

/**
 * Embedded HTTP Server for testing, mostly for assets refresh,
 * but can serve
 */
object HttpServer extends CatsEffect {

  private val logger: Logger[IO] =
    Slf4jLogger.getLogger[IO]

  /**
   * Set of testing routes:
   * * Plain data
   * * Imitating slow connection
   * * Frequently updating resource
   * * Sometimes non-working resource
   *
   * @param counter mutable variable with counter updated on every request
   */
  def routes(counter: Ref[IO, Int]): HttpRoutes[IO] =
    HttpRoutes
      .of[IO] {
        case r @ GET -> Root / "counter" =>
          logger.debug(r.pathInfo.toString) *> counter.updateAndGet(_ + 1).flatMap { i =>
            Ok(s"counter $i")
          }
        case r @ GET -> Root / "flaky" =>
          logger.debug(r.pathInfo.toString) *> counter.update(_ + 1) *>
            counter.get.flatMap { i =>
              val s = i.toString
              if (i == 1 || i == 2) NotFound(s)
              else if (i == 3) Ok(s)
              else NotFound(s)
            }
        case GET -> Root / "maxmind" / "GeoIP2-City.mmdb" =>
          counter.updateAndGet(_ + 1).flatMap { i =>
            val is = readMaxMindDb(i)
            Ok(readInputStream[IO](is, 256).compile.to(Array))
          }
        case GET -> Root / "iab" / file =>
          counter.updateAndGet(_ + 1).flatMap { i =>
            file match {
              case "include" => Ok("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0|1|1")
              case "exclude" => Ok("")
              case "ip" if i == 1 => Ok("175.16.199.0/32")
              case "ip" => Ok("175.1.1.0/32")
              case other =>
                println(s"Not Found ${other}")
                NotFound(other)
            }
          }
        case GET -> Root / "enrichment" / "api" / output =>
          counter.updateAndGet(_ + 1).flatMap { _ =>
            Ok(json"""{"output": $output}""".noSpaces)
          }
      }

  def resource: Resource[IO, Unit] =
    for {
      counter <- Resource.eval(Ref.of[IO, Int](0))
      _ <- BlazeServerBuilder[IO]
             .bindHttp(8080)
             .withHttpApp(routes(counter).orNotFound)
             .withoutBanner
             .withoutSsl
             .resource
    } yield ()

  private def readMaxMindDb(req: Int) = {
    val path =
      if (req < 4) s"/assets-refresh/geoip2-city-$req.mmdb"
      else s"/assets-refresh/geoip2-city-3.mmdb"
    IO(getClass.getResourceAsStream(path))
  }
}
