package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.extractor

import io.circe.{JsonObject, Json, Decoder}
import cats.implicits._

import com.snowplowanalytics.iglu.core.{SchemaVer, SelfDescribingData, SchemaKey}

import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

sealed trait Extractable extends Product with Serializable {
  def schemaKey: SchemaKey
  def keys: List[(String, TypedField)]

  private def getters =
    keys.map { case (key, v) => Extractable.EventClass.getMethod(key) -> v }
  private def erasers =
    keys.map { case (key, f) => Extractable.EventClass.getMethod("set" ++ key.capitalize, f.manifest.runtimeClass) }

  def getJson(event: EnrichedEvent): Either[Throwable, JsonObject] =
    getters
      .traverse { case (getter, to) => to.cast(getter.invoke(event)).map { value => to.name -> value } }
      .map { kvs => JsonObject.fromIterable(kvs.collect { case (k, Some(v)) => (k, v) }) }

  def process(event: EnrichedEvent): Either[Throwable, Option[SelfDescribingData[Json]]] =
    getJson(event) match {
      case Right(o) if o.isEmpty => Right(None)
      case Right(o) => Right(Some(SelfDescribingData(schemaKey, Json.fromJsonObject(o))))
      case Left(error) => Left(error)
    }

  def erase(event: EnrichedEvent): Unit = {
    erasers.foreach { eraser => eraser.invoke(event, null) }
  }
}

object Extractable {

  type Extractables = List[Extractable]

  def All: Extractables = List(MaxMind)

  implicit def extractableCirceDecoder: Decoder[Extractable] =
    Decoder[String].map(_.toLowerCase).emap { e =>
      All
        .find(_.toString.toLowerCase == e.toLowerCase)
        .toRight(s"$e is an unknown entity to extract. Try: ${All.map(_.toString.toLowerCase).mkString(", ")} or all")
    }

  implicit def extractablesCirceDecoder: Decoder[Extractables] =
    Decoder[List[Extractable]]
      .handleErrorWith(e => Decoder[String].emap(s => if (s.toLowerCase == "all") All.asRight else e.show.asLeft))

  private val EventClass = classOf[EnrichedEvent]

  case object MaxMind extends Extractable {
    val schemaKey = SchemaKey("com.maxmind", "context", "jsonschema", SchemaVer.Full(1,0,0))

    def keys = List(
      "geo_country"     -> TypedField.Str("country"),
      "geo_region"      -> TypedField.Str("region"),
      "geo_city"        -> TypedField.Str("city"),
      "geo_zipcode"     -> TypedField.Str("zipcode"),
      "geo_latitude"    -> TypedField.Flo("latitude"),
      "geo_longitude"   -> TypedField.Flo("longitude"),
      "geo_region_name" -> TypedField.Str("region_name"),

      "ip_isp"          -> TypedField.Str("isp"),
      "ip_organization" -> TypedField.Str("organization"),
      "ip_domain"       -> TypedField.Str("domain"),
      "ip_netspeed"     -> TypedField.Str("netspeed")
    )
  }
}
