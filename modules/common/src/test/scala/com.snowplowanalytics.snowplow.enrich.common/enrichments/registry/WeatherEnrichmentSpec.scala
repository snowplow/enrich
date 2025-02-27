/**
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import java.lang.{Float => JFloat}

import org.joda.time.DateTime

import org.specs2.Specification

import cats.data.EitherT

import cats.effect.IO

import cats.effect.testing.specs2.CatsEffect

import io.circe.generic.auto._
import io.circe.literal._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.WeatherConf

object WeatherEnrichmentSpec {
  val OwmApiKey = "OWM_KEY"
}

class WeatherEnrichmentSpec extends Specification with CatsEffect {
  import WeatherEnrichmentSpec._
  def is =
    skipAllIf(sys.env.get(OwmApiKey).isEmpty) ^ // Actually only e4 and e6 need to be skipped
      s2"""
  This is a specification to test the WeatherEnrichment
  Fail event for null time          $e1
  Fail event for invalid key        $e3
  Extract weather stamp             $e2
  Extract humidity                  $e4
  Extract configuration             $e5
  Check time stamp transformation   $e6
  """

  val schemaKey = SchemaKey("vendor", "name", "format", SchemaVer.Full(1, 0, 0))

  lazy val validAppKey = sys.env
    .getOrElse(OwmApiKey,
               throw new IllegalStateException(
                 s"No $OwmApiKey environment variable found, test should have been skipped"
               )
    )

  object invalidEvent {
    var lat: JFloat = 70.98224f
    var lon: JFloat = 70.98224f
    var time: DateTime = null
  }

  object validEvent {
    var lat: JFloat = 20.713052f
    var lon: JFloat = 70.98224f
    var time: DateTime = new DateTime("2020-04-28T12:00:00.000+00:00")
  }

  def e1 =
    (for {
      enr <- WeatherConf(schemaKey, "history.openweathermap.org", "KEY", 10, 5200, 1)
               .enrichment[IO]
      stamp <- EitherT(
                 enr.getWeatherContext(
                   Option(invalidEvent.lat),
                   Option(invalidEvent.lon),
                   Option(invalidEvent.time)
                 )
               ).leftMap(_.head.toString)
    } yield stamp).value.map(_ must beLeft.like {
      case e =>
        e must contain("InputData(derived_tstamp,None,missing)")
    })

  def e2 =
    (for {
      enr <- WeatherConf(schemaKey, "history.openweathermap.org", validAppKey, 10, 5200, 1)
               .enrichment[IO]
      stamp <- EitherT(
                 enr.getWeatherContext(
                   Option(validEvent.lat),
                   Option(validEvent.lon),
                   Option(validEvent.time)
                 )
               ).leftMap(_.head.toString)
    } yield stamp).value.map(_ must beRight)

  def e3 =
    (for {
      enr <- WeatherConf(schemaKey, "history.openweathermap.org", "KEY", 10, 5200, 1)
               .enrichment[IO]
      stamp <- EitherT(
                 enr.getWeatherContext(
                   Option(validEvent.lat),
                   Option(validEvent.lon),
                   Option(validEvent.time)
                 )
               ).leftMap(_.head.toString)
    } yield stamp).value.map(_ must beLeft.like { case e => e must contain("Check your API key") })

  def e4 =
    (for {
      enr <- WeatherConf(schemaKey, "history.openweathermap.org", validAppKey, 15, 5200, 1)
               .enrichment[IO]
      stamp <- EitherT(
                 enr.getWeatherContext(
                   Option(validEvent.lat),
                   Option(validEvent.lon),
                   Option(validEvent.time)
                 )
               ).leftMap(_.head.toString)
    } yield stamp).value.map(_ must beRight.like {
      case weather =>
        val temp = weather.data.hcursor.downField("main").get[Double]("humidity")
        temp must beRight(69.0d)
    })

  def e5 = {
    val configJson = json"""{
      "enabled": true,
      "vendor": "com.snowplowanalytics.snowplow.enrichments",
      "name": "weather_enrichment_config",
      "parameters": {
        "apiKey": "{{KEY}}",
        "cacheSize": 5100,
        "geoPrecision": 1,
        "apiHost": "history.openweathermap.org",
        "timeout": 5
      }
    }"""
    val schemaKey = SchemaKey(
      "com.snowplowanalytics.snowplow.enrichments",
      "weather_enrichment_config",
      "jsonschema",
      SchemaVer.Full(1, 0, 0)
    )
    val config = WeatherEnrichment.parse(configJson, schemaKey)
    config.toEither must beRight(
      WeatherConf(
        schemaKey,
        apiHost = "history.openweathermap.org",
        apiKey = "{{KEY}}",
        timeout = 5,
        cacheSize = 5100,
        geoPrecision = 1
      )
    )
  }

  def e6 =
    (for {
      enr <- WeatherConf(schemaKey, "history.openweathermap.org", validAppKey, 15, 2, 1)
               .enrichment[IO]
      stamp <- EitherT(
                 enr.getWeatherContext(
                   Option(validEvent.lat),
                   Option(validEvent.lon),
                   Option(validEvent.time)
                 )
               ).leftMap(_.head.toString)
    } yield stamp).value.map(_ must beRight.like { // successful request
      case weather =>
        weather.data.hcursor.as[TransformedWeather] must beRight.like {
          case w =>
            w.dt must equalTo("2020-04-28T12:00:00.000Z")
        }
    })
}
