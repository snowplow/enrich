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

import java.net.URI
import cats.implicits._
import cats.data.{EitherT, NonEmptyList, ValidatedNel}
import cats.effect.{Ref, Resource, Sync}
import com.networknt.schema.JsonSchema
import com.snowplowanalytics.iglu.client.CirceValidator
import io.circe.{Decoder, Encoder, Json, parser}
import io.circe.generic.semiauto._
import io.circe.syntax.EncoderOps
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.EventSpecConf
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EventSpecEnrichment.{
  isEventSpecEntity,
  isValidAgainstSchema,
  mkSpecContext,
  toSDJWithDefaultEventSupport
}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.CirceUtils
import io.circe.jackson.snowplow.circeToJackson

import scala.io.Source

object EventSpecEnrichment extends ParseableEnrichment {

  override val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow.enrichments", "event_spec_enrichment_config", "jsonschema", 1, 0)

  private val localFile = "./event-specs.json"

  /**
   * Represents a plain EventSpec provided in the configuration file
   *
   * This fields in this class should correspond exactly to the fields in a standard Event Spec
   */
  case class Entity(
    schemaKey: SchemaKey,
    minCardinality: Option[Int],
    maxCardinality: Option[Int],
    constraint: Option[Json]
  )

  case class EventSpec(
    id: String,
    name: String,
    // Temporarily Optional for backward compatibility during rollout.
    // Old S3 payloads (pre msc-backend#4498) don't include version and decode as None.
    // Once all deployments send versioned payloads, this will always be Some(n).
    version: Option[Int],
    schemaKey: SchemaKey,
    constraint: Option[Json],
    entities: List[Entity]
  )

  case class EntityCompiled(
    schemaKey: SchemaKey,
    minCardinality: Option[Int],
    maxCardinality: Option[Int],
    constraint: Option[JsonSchema]
  )

  private object EntityCompiled {
    def fromEntity(e: Entity): Either[String, EntityCompiled] =
      e.constraint
        .traverse(x => CirceValidator.compileJsonSchema(x, Int.MaxValue))
        .map { c =>
          EntityCompiled(e.schemaKey, e.minCardinality, e.maxCardinality, c)
        }
        .leftMap(error => s"Could not compile event spec entity schema for ${e.schemaKey.toSchemaUri}: $error")
  }

  case class EventSpecCompiled(
    id: String,
    name: String,
    version: Option[Int],
    schemaKey: SchemaKey,
    constraint: Option[JsonSchema],
    entities: List[EntityCompiled]
  )

  private object EventSpecCompiled {
    def fromEventSpec(e: EventSpec): Either[String, EventSpecCompiled] =
      for {
        c <- e.constraint
               .traverse(x => CirceValidator.compileJsonSchema(x, Int.MaxValue))
               .leftMap(error =>
                 s"Could not compile event spec schema constraint for event spec ${e.id} schema ${e.schemaKey.toSchemaUri}: $error"
               )
        esc <- e.entities
                 .traverse(EntityCompiled.fromEntity)
                 .leftMap(error => s"Error during compilation of entity in event spec ${e.id}: $error")
      } yield EventSpecCompiled(e.id, e.name, e.version, e.schemaKey, c, esc)

  }

  implicit def eventSpecEntityDecoder: Decoder[Entity] = deriveDecoder

  implicit def eventSpecEntityEncoder: Encoder[Entity] = deriveEncoder

  implicit def eventSpecDecoder: Decoder[EventSpec] = deriveDecoder

  implicit def eventSpecEncoder: Encoder[EventSpec] = deriveEncoder

  /**
   * A structure optimized for fast lookups of event specs based on incoming event
   *
   * event -> required entities set -> list of event specs with given event and required entities
   */

  private type EventSpecLookup = Map[SchemaKey, Map[Set[SchemaKey], List[EventSpecCompiled]]]

  /** Compiled specs keyed by (id, version) for fast validation lookups */
  private type ValidationCache = Map[(String, Int), EventSpecCompiled]

  /** Raw JSON strings keyed by (id, version) for on-demand compilation */
  private type Archive = Map[(String, Int), String]

  sealed trait LookupError
  object LookupError {
    case class NotFound(id: String, version: Int) extends LookupError
    case class CompilationFailed(
      id: String,
      version: Int,
      reason: String
    ) extends LookupError
  }

  override def parse(
    config: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, EventSpecConf] =
    (for {
      _ <- isParseable(config, schemaKey).leftMap(NonEmptyList.one)
      conf <- (
                  CirceUtils.extract[String](config, "parameters", "uri").toValidatedNel,
                  CirceUtils.extract[String](config, "parameters", "database").toValidatedNel
              ).mapN((_, _)).toEither
      uri <- getDatabaseUri(conf._1, conf._2).leftMap(NonEmptyList.one)
    } yield EventSpecConf(schemaKey, file(uri, conf._2, localMode))).toValidated

  private def file(
    uri: URI,
    db: String,
    localMode: Boolean
  ): (URI, String) =
    if (localMode)
      (uri, Option(getClass.getResource(db)).getOrElse(getClass.getResource("/" + db)).toURI.getPath)
    else
      (uri, localFile)

  def create[F[_]: Sync](filePath: String): EitherT[F, String, EventSpecEnrichment[F]] =
    for {
      content <- EitherT(readFile[F](filePath))
      eventSpecs <- EitherT.fromEither[F](parseEventSpecs(content))
      tiers <- EitherT.fromEither[F](buildTiers(eventSpecs))
      (inferenceLookup, validationCache, archiveMap) = tiers
      ref <- EitherT.right[String](Ref.of[F, ValidationCache](validationCache))
    } yield new EventSpecEnrichment[F](inferenceLookup, ref, archiveMap)

  private def readFile[F[_]: Sync](path: String): F[Either[String, String]] =
    Resource
      .fromAutoCloseable(Sync[F].blocking(Source.fromFile(path)))
      .use(source => Sync[F].blocking(source.mkString))
      .attempt
      .map(_.leftMap(e => s"Failed to read event specs file: ${e.getMessage}"))

  private def parseEventSpecs(content: String): Either[String, List[EventSpec]] =
    parser
      .decode[Json](content)
      .flatMap(_.hcursor.downField("eventSpecs").as[List[EventSpec]])
      .leftMap(e => s"Failed to parse event specs file: ${e.getMessage}")

  private[registry] def createFromSpecs[F[_]: Sync](eventSpecs: List[EventSpec]): F[Either[String, EventSpecEnrichment[F]]] =
    buildTiers(eventSpecs) match {
      case Right((inferenceLookup, validationCache, archiveMap)) =>
        Ref.of[F, ValidationCache](validationCache).map { ref =>
          Right(new EventSpecEnrichment[F](inferenceLookup, ref, archiveMap))
        }
      case Left(error) =>
        Sync[F].pure(Left(error))
    }

  /**
   * Organize compiled event specs into the optimized lookup structure for fast inference.
   */
  private def organizeIntoLookup(compiledSpecs: List[EventSpecCompiled]): EventSpecLookup =
    compiledSpecs.groupBy(_.schemaKey).map {
      case (eventKey, specs) =>
        val eventSpecByRequiredEntity = specs.groupBy { es =>
          es.entities.filter(_.minCardinality.exists(_ >= 1)).map(_.schemaKey).toSet
        }
        (eventKey, eventSpecByRequiredEntity)
    }

  /**
   * Partition event specs into three tiers based on version.
   *
   * Cache specs are compiled first. The latest version per id (which appears in both
   * inference and cache) is shared between both lookups to avoid compiling the same
   * spec twice. Versionless specs are compiled separately for inference only.
   * Archive specs are serialized to raw JSON strings (~13x less memory than case classes).
   */
  private def buildTiers(
    eventSpecs: List[EventSpec]
  ): Either[String, (EventSpecLookup, ValidationCache, Archive)] = {
    val (versionless, versioned) = eventSpecs.partition(_.version.isEmpty)

    val groupedById: Map[String, List[EventSpec]] =
      versioned.groupBy(_.id).map {
        case (id, specs) => id -> specs.sortBy(_.version.getOrElse(0))(Ordering[Int].reverse)
      }

    val cacheSpecs = groupedById.values.flatMap(_.take(3)).toList
    val archiveRaw = groupedById.values.flatMap(_.drop(3)).toList

    val archive: Archive = archiveRaw.flatMap(es => es.version.map(v => ((es.id, v), es.asJson.noSpaces))).toMap

    for {
      compiledCache <- cacheSpecs.traverse(EventSpecCompiled.fromEventSpec)
      compiledVersionless <- versionless.traverse(EventSpecCompiled.fromEventSpec)
    } yield {
      val validationCache: ValidationCache =
        compiledCache.flatMap(c => c.version.map(v => ((c.id, v), c))).toMap

      val latestPerIdCompiled = groupedById.values.flatMap { specs =>
        for {
          latest <- specs.headOption
          compiled <- compiledCache.find(c => c.id === latest.id && c.version === latest.version)
        } yield compiled
      }.toList

      val inferenceLookup = organizeIntoLookup(compiledVersionless ++ latestPerIdCompiled)
      (inferenceLookup, validationCache, archive)
    }
  }

  private def isValidAgainstSchema(
    data: Json,
    schema: JsonSchema,
    maxJsonDepth: Int
  ): Boolean =
    (for {
      jacksonJson <- circeToJackson(data, maxJsonDepth).toOption
    } yield schema.validate(jacksonJson).isEmpty).getOrElse(false)

  private val eventSpecSchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow", "event_specification", "jsonschema", SchemaVer.Full(1, 0, 3))
  private val pagePingSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "page_ping", "jsonschema", SchemaVer.Full(1, 0, 0))
  private val pageViewSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "page_view", "jsonschema", SchemaVer.Full(1, 0, 0))

  private def isEventSpecEntity(schema: SchemaKey): Boolean =
    schema.vendor === eventSpecSchemaKey.vendor && schema.name === eventSpecSchemaKey.name

  /**
   * Handle the page_pings/page_views the way that they are modelled in console
   *
   * The console models page_pings/page_views as normal json schemas, and allows
   * to create event specs for those schemas. To validate them in the same way as
   * we are validating unstruct_event, we build those non-existing SDJ for them
   */
  def toSDJWithDefaultEventSupport(event: EnrichedEvent): Option[SelfDescribingData[Json]] =
    event.unstruct_event.orElse {
      event.event_name match {
        case "page_ping" => Some(EventSpecEnrichment.buildPagePingSDJ(event))
        case "page_view" => Some(EventSpecEnrichment.buildPageViewSDJ(event))
        case _ => None
      }
    }

  private def buildPagePingSDJ(event: EnrichedEvent): SelfDescribingData[Json] =
    SelfDescribingData(
      pagePingSchemaKey,
      Json.obj(
        "page_url" -> Option(event.page_url).asJson,
        "page_title" -> Option(event.page_title).asJson,
        "page_referrer" -> Option(event.page_referrer).asJson,
        "page_urlscheme" -> Option(event.page_urlscheme).asJson,
        "page_urlhost" -> Option(event.page_urlhost).asJson,
        "page_urlport" -> Option(event.page_urlport).asJson,
        "page_urlpath" -> Option(event.page_urlpath).asJson,
        "page_urlquery" -> Option(event.page_urlquery).asJson,
        "page_urlfragment" -> Option(event.page_urlfragment).asJson,
        "pp_xoffset_min" -> Option(event.pp_xoffset_min).asJson,
        "pp_xoffset_max" -> Option(event.pp_xoffset_max).asJson,
        "pp_yoffset_min" -> Option(event.pp_yoffset_min).asJson,
        "pp_yoffset_max" -> Option(event.pp_yoffset_max).asJson
      )
    )

  private def buildPageViewSDJ(event: EnrichedEvent): SelfDescribingData[Json] =
    SelfDescribingData(
      pageViewSchemaKey,
      Json.obj(
        "page_url" -> Option(event.page_url).asJson,
        "page_title" -> Option(event.page_title).asJson,
        "page_referrer" -> Option(event.page_referrer).asJson,
        "page_urlscheme" -> Option(event.page_urlscheme).asJson,
        "page_urlhost" -> Option(event.page_urlhost).asJson,
        "page_urlport" -> Option(event.page_urlport).asJson,
        "page_urlpath" -> Option(event.page_urlpath).asJson,
        "page_urlquery" -> Option(event.page_urlquery).asJson,
        "page_urlfragment" -> Option(event.page_urlfragment).asJson
      )
    )

  private def mkSpecContext(es: EventSpecCompiled): SelfDescribingData[Json] =
    SelfDescribingData(
      eventSpecSchemaKey,
      Json.obj(
        List(
          Some("id" -> es.id.asJson),
          Some("name" -> es.name.asJson),
          es.version.map(v => "version" -> v.asJson)
        ).flatten: _*
      )
    )
}

class EventSpecEnrichment[F[_]: Sync] private (
  eventSpecs: EventSpecEnrichment.EventSpecLookup,
  validationCacheRef: Ref[F, EventSpecEnrichment.ValidationCache],
  archive: EventSpecEnrichment.Archive
) {

  /**
   * Infer the event spec of an incoming event
   *
   * The returned `SelfDescribingData` will be automatically added to the event as a derived context
   *
   * This method runs on **every** event, so it should be as efficient as possible, and it should
   * make use of the pre-prepared `EventSpecLookup` which is optimized for fast lookups.
   */
  def inferEventSpec(event: EnrichedEvent, maxJsonDepth: Int): List[SelfDescribingData[Json]] = {
    val entitiesKeys = event.contexts.map(_.schema).toSet
    (for {
      // skip the inference if event is declaring it belongs to an event spec (snowtype or manual)
      _ <- if (entitiesKeys.exists(isEventSpecEntity)) None else Some(())
      sdj <- toSDJWithDefaultEventSupport(event)
      requiredEntities <- eventSpecs.get(sdj.schema)
      specsCandidates = requiredEntities
                          .collect {
                            case (required, specs) if required.subsetOf(entitiesKeys) => specs
                          }
                          .flatten
                          .toList
      passingSpecs <- if (
                        // skip the inference if there are no candidates
                        specsCandidates.nonEmpty
                      ) Some {
                        val entitiesMap = event.contexts.groupBy(_.schema)
                        specsCandidates.filter { spec =>
                          spec.constraint.forall(c => isValidAgainstSchema(sdj.data, c, maxJsonDepth)) &&
                          spec.entities.forall { entity =>
                            val relevantEntities = entitiesMap.getOrElse(entity.schemaKey, List.empty)
                            relevantEntities.size >= entity.minCardinality.getOrElse(0) &&
                            relevantEntities.size <= entity.maxCardinality.getOrElse(Int.MaxValue) &&
                            relevantEntities.forall { en =>
                              entity.constraint.forall { c =>
                                isValidAgainstSchema(en.data, c, maxJsonDepth)
                              }
                            }
                          }
                        }
                      }
                      else None
    } yield passingSpecs.map(mkSpecContext)).getOrElse(List.empty)
  }

  /**
   * Look up a compiled spec by (id, version) from the tracker's event_specification entity
   * against the tiers built from the backend's S3 config file.
   * If not found in cache, check the archive and promote (compile + add to cache) on hit.
   */
  def lookupInCache(id: String, version: Int): F[Either[EventSpecEnrichment.LookupError, EventSpecEnrichment.EventSpecCompiled]] =
    validationCacheRef.get.flatMap { cache =>
      cache.get((id, version)) match {
        case Some(hit) => Sync[F].pure(hit.asRight)
        case None =>
          archive.get((id, version)) match {
            case Some(raw) =>
              val compiled = for {
                eventSpec <- parser.decode[EventSpecEnrichment.EventSpec](raw).leftMap(_.getMessage)
                c <- EventSpecEnrichment.EventSpecCompiled.fromEventSpec(eventSpec)
              } yield c
              compiled match {
                case Right(c) =>
                  validationCacheRef.update(_ + ((id, version) -> c)).as(c.asRight)
                case Left(reason) =>
                  Sync[F].pure(EventSpecEnrichment.LookupError.CompilationFailed(id, version, reason).asLeft)
              }
            case None =>
              Sync[F].pure(EventSpecEnrichment.LookupError.NotFound(id, version).asLeft)
          }
      }
    }

}
