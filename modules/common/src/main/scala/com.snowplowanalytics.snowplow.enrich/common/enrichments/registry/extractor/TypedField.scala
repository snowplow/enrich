package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.extractor

import io.circe.{Encoder, Json}

import cats.implicits._

sealed trait TypedField {
  type Target
  def manifest: Manifest[Target]
  def name: String
  def encoder: Encoder[Target]

  def cast(anyRef: AnyRef): Either[Throwable, Option[Json]] =
    Either.catchNonFatal(anyRef.asInstanceOf[Target]).map(Option.apply).nested.map(encoder.apply).value
}

object TypedField {
  case class Str(name: String) extends TypedField {
    type Target = String
    def manifest: Manifest[String] = implicitly[Manifest[String]]
    val encoder = Encoder[String]
  }
  case class Flo(name: String) extends TypedField {
    type Target = java.lang.Float
    def manifest: Manifest[java.lang.Float] = implicitly[Manifest[java.lang.Float]]
    val encoder = Encoder[java.lang.Float]
  }
}

