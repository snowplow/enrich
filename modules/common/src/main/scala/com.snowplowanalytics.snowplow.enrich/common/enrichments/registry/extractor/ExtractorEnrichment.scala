package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.extractor

import java.time.Instant

import cats.Applicative
import cats.implicits._
import cats.data.{EitherT, ValidatedNel, NonEmptyList}

import io.circe.{Error, Json}
import io.circe.parser.parse
import io.circe.syntax._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SelfDescribingData, SchemaKey}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.badrows.FailureDetails.{EnrichmentFailureMessage, EnrichmentFailure}
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor, Payload, Failure}
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent.toRawEvent
import com.snowplowanalytics.snowplow.enrich.common.enrichments.MiscEnrichments
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.extractor.ExtractorEnrichment.failedReflection
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.{ParseableEnrichment, EnrichmentConf}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent.toPartiallyEnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.CirceUtils

final case class ExtractorEnrichment(entities: Set[Extractable], erase: Boolean) {
  def process[F[_]: Applicative](processor: Processor, rawEvent: RawEvent, event: EnrichedEvent): EitherT[F, BadRow, Unit] = {
    // Create beforehand because later it could be mutated
    val badRowPayload = Payload.EnrichmentPayload(toPartiallyEnrichedEvent(event), toRawEvent(rawEvent))
    entities.toList.flatTraverse { entity =>
      entity.process(event) match {
        case Left(value) =>
          Left((entity.toString.toLowerCase, value))
        case Right(Some(json)) =>
          if (erase) entity.erase(event) else ()                           // Unsafe mutation
          Right(List(json))
        case Right(None) =>
          Right(Nil)
      }
    } match {
      case Left(error) =>
        EitherT.leftT(failedReflection(processor, badRowPayload)(error))
      case Right(entities) if entities.isEmpty =>
        EitherT.rightT[F, BadRow](())
      case Right(entities) =>
        val contexts = if (event.derived_contexts == null)
          SelfDescribingData(MiscEnrichments.ContextsSchema, List.empty[Json]).asRight
        else
          parse(event.derived_contexts)
            .flatMap(_.as[SelfDescribingData[Json]])
            .flatMap { contexts => contexts.data.as[List[Json]].map(data => SelfDescribingData(contexts.schema, data)) }

        contexts
          .map { contexts => SelfDescribingData(contexts.schema, (contexts.data ++ entities.map(_.normalize)).asJson) }
          .leftMap(ExtractorEnrichment.failedDecoding(processor, badRowPayload))
          .toEitherT[F]
          .map(contexts => event.setDerived_contexts(contexts.asString))   // Unsafe mutation
    }
  }
}

object ExtractorEnrichment extends ParseableEnrichment {

  def failedDecoding(processor: Processor, payload: Payload.EnrichmentPayload)(error: Error): BadRow = {
    val message = EnrichmentFailureMessage.Simple(s"Cannot decode derived_contexts. ${error.show}")
    val enrichmentFailure = EnrichmentFailure(None, message)
    val messages = NonEmptyList.one(enrichmentFailure)
    val failure = Failure.EnrichmentFailures(Instant.now(), messages)
    BadRow.EnrichmentFailures(processor, failure, payload)
  }

  def failedReflection(processor: Processor, payload: Payload.EnrichmentPayload)(error: (String, Throwable)): BadRow = {
    val message = EnrichmentFailureMessage.Simple(s"Failed to extract a property for ${error._1}. ${error._2}")
    val enrichmentFailure = EnrichmentFailure(None, message)
    val messages = NonEmptyList.one(enrichmentFailure)
    val failure = Failure.EnrichmentFailures(Instant.now(), messages)
    BadRow.EnrichmentFailures(processor, failure, payload)
  }


  val supportedSchema: SchemaCriterion =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "extractor_enrichment_config",
      "jsonschema",
      1,
      0,
      0
    )

  def parse(config: Json, schemaKey: SchemaKey, localMode: Boolean): ValidatedNel[String, EnrichmentConf] = {
    isParseable(config, schemaKey)
      .toValidatedNel
      .andThen { _ =>
        val erase = CirceUtils.extract[Boolean](config, "parameters", "erase")
        val extract = CirceUtils.extract[Extractable.Extractables](config, "parameters", "extract")

        (erase, extract).mapN { (er, ex) =>
          EnrichmentConf.ExtractorConf(schemaKey, ex.toSet, er)
        }.toValidatedNel
      }
  }
}
