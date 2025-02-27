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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import java.lang.{Float => JFloat}
import java.time.{Instant, ZoneOffset, ZonedDateTime}

import scala.concurrent.duration.FiniteDuration

import cats.Monad
import cats.data.{EitherT, NonEmptyList, ValidatedNel}
import cats.implicits._

import cats.effect.Async

import org.joda.time.{DateTime, DateTimeZone}

import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.weather.providers.openweather._
import com.snowplowanalytics.weather.providers.openweather.responses._

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.WeatherConf
import com.snowplowanalytics.snowplow.enrich.common.utils.CirceUtils

/** Companion object. Lets us create an WeatherEnrichment instance from a Json */
object WeatherEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "weather_enrichment_config",
      "jsonschema",
      1,
      0
    )

  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, WeatherConf] =
    isParseable(c, schemaKey)
      .leftMap(e => NonEmptyList.one(e))
      .flatMap { _ =>
        (
          CirceUtils.extract[String](c, "parameters", "apiHost").toValidatedNel,
          CirceUtils.extract[String](c, "parameters", "apiKey").toValidatedNel,
          CirceUtils.extract[Int](c, "parameters", "timeout").toValidatedNel,
          CirceUtils.extract[Int](c, "parameters", "cacheSize").toValidatedNel,
          CirceUtils.extract[Int](c, "parameters", "geoPrecision").toValidatedNel
        ).mapN(WeatherConf(schemaKey, _, _, _, _, _)).toEither
      }
      .toValidated

  def create[F[_]: Async](
    schemaKey: SchemaKey,
    apiHost: String,
    apiKey: String,
    timeout: FiniteDuration,
    ssl: Boolean,
    cacheSize: Int,
    geoPrecision: Int
  ): EitherT[F, String, WeatherEnrichment[F]] =
    EitherT(
      CreateOWM[F]
        .create(
          apiHost,
          apiKey,
          timeout,
          ssl,
          cacheSize,
          geoPrecision
        )
    ).leftMap(_.message)
      .map(c => WeatherEnrichment(schemaKey, c))
}

/**
 * Contains weather enrichments based on geo coordinates and time
 * @param client OWM client to get the weather from
 */
final case class WeatherEnrichment[F[_]: Monad](schemaKey: SchemaKey, client: OWMCacheClient[F]) extends Enrichment {
  val Schema = SchemaKey("org.openweathermap", "weather", "jsonschema", SchemaVer.Full(1, 0, 0))
  private val enrichmentInfo =
    FailureDetails.EnrichmentInformation(schemaKey, "weather").some

  /**
   * Get weather context as JSON for specific event. Any non-fatal error will return failure and
   * thus whole event will be filtered out in future
   * @param latitude enriched event optional latitude (probably null)
   * @param longitude enriched event optional longitude (probably null)
   * @param time enriched event optional time (probably null)
   * @return weather stamp as self-describing JSON object
   */
  // It accepts Java Float (JFloat) instead of Scala's because it will throw NullPointerException
  // on conversion step if `EnrichedEvent` has nulls as geo_latitude or geo_longitude
  def getWeatherContext(
    latitude: Option[JFloat],
    longitude: Option[JFloat],
    time: Option[DateTime]
  ): F[Either[NonEmptyList[FailureDetails.EnrichmentFailure], SelfDescribingData[Json]]] =
    getWeather(latitude, longitude, time).map(weather => SelfDescribingData(Schema, weather)).value

  /**
   * Get weather stamp as JSON received from OpenWeatherMap and extracted with Scala Weather
   * @param latitude enriched event optional latitude
   * @param longitude enriched event optional longitude
   * @param time enriched event optional time
   * @return weather stamp as JSON object
   */
  private def getWeather(
    latitude: Option[JFloat],
    longitude: Option[JFloat],
    time: Option[DateTime]
  ): EitherT[F, NonEmptyList[FailureDetails.EnrichmentFailure], Json] =
    (latitude, longitude, time) match {
      case (Some(lat), Some(lon), Some(t)) =>
        val ts = ZonedDateTime.ofInstant(Instant.ofEpochMilli(t.getMillis()), ZoneOffset.UTC)
        for {
          weather <- EitherT(client.cachingHistoryByCoords(lat, lon, ts))
                       .leftMap { e =>
                         val f =
                           FailureDetails.EnrichmentFailure(
                             enrichmentInfo,
                             FailureDetails.EnrichmentFailureMessage
                               .Simple(e.getMessage)
                           )
                         NonEmptyList.one(f)
                       }
                       .leftWiden[NonEmptyList[FailureDetails.EnrichmentFailure]]
          transformed = transformWeather(weather)
        } yield transformed.asJson
      case (a, b, c) =>
        val failures = List((a, "geo_latitude"), (b, "geo_longitude"), (c, "derived_tstamp"))
          .collect {
            case (None, n) =>
              FailureDetails.EnrichmentFailure(
                enrichmentInfo,
                FailureDetails.EnrichmentFailureMessage
                  .InputData(n, none, "missing")
              )
          }
        EitherT.leftT(
          NonEmptyList
            .fromList(failures)
            .getOrElse(
              NonEmptyList.one(
                FailureDetails.EnrichmentFailure(
                  enrichmentInfo,
                  FailureDetails.EnrichmentFailureMessage
                    .Simple("could not construct failures")
                )
              )
            )
        )
    }

  /**
   * Apply all necessary transformations (currently only dt(epoch -> db timestamp)
   * from `weather.providers.openweather.Responses.Weather` to `TransformedWeather`
   * for further JSON decomposition
   *
   * @param origin original OpenWeatherMap Weather stamp
   * @return tranfsormed weather
   */
  private[enrichments] def transformWeather(origin: Weather): TransformedWeather =
    TransformedWeather(
      origin.main,
      origin.wind,
      origin.clouds,
      origin.rain,
      origin.snow,
      origin.weather,
      new DateTime(origin.dt.toLong * 1000, DateTimeZone.UTC).toString()
    )
}

/**
 * Copy of `com.snowplowanalytics.weather.providers.openweather.Responses.Weather` intended to
 * execute typesafe (as opposed to JSON) transformation
 */
private[enrichments] final case class TransformedWeather(
  main: MainInfo,
  wind: Wind,
  clouds: Clouds,
  rain: Option[Rain],
  snow: Option[Snow],
  weather: List[WeatherCondition],
  dt: String
)
