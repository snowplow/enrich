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
package com.snowplowanalytics.snowplow.enrich.streams.common

import cats.effect.IO
import cats.effect.kernel.Resource

import io.circe.literal._

import fs2.io.readInputStream

import com.comcast.ip4s.Port

import org.http4s.HttpRoutes
import org.http4s.Method.GET
import org.http4s.server.Server
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.dsl.io._
import org.http4s.syntax.all._

object HttpServer {

  val port = 8181

  private def routes: HttpRoutes[IO] =
    HttpRoutes
      .of[IO] {
        case GET -> Root / "maxmind" / "GeoIP2-City.mmdb" =>
          val is = IO(getClass.getResourceAsStream("/GeoIP2-City.mmdb"))
          Ok(readInputStream[IO](is, 256).compile.to(Array))
        case GET -> Root / "enrichment" / "api" / quantity =>
          Ok(json"""{"record":{"sku":"pedals","quantity":${quantity.toInt}}}""".noSpaces)
      }

  def resource: Resource[IO, Server] =
    EmberServerBuilder
      .default[IO]
      .withPort(Port.fromInt(port).get)
      .withHttpApp(routes.orNotFound)
      .build
}
