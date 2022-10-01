/*
 * Copyright (c) 2021-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.fs2.config

import java.lang.reflect.Field

import _root_.io.circe.Json
import _root_.io.circe.syntax._

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import cats.effect.{Async, Clock, ContextShift, Sync}

import cats.implicits._
import cats.data.{EitherT, NonEmptyList}
import cats.Applicative

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.{IgluCirceClient, Resolver}

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.utils.ConversionUtils
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

import com.snowplowanalytics.snowplow.enrich.common.fs2.{Parsed, ValidationResult}
import com.snowplowanalytics.snowplow.enrich.common.fs2.io.FileSystem
import com.snowplowanalytics.snowplow.enrich.common.fs2.config.io.{Output => OutputConfig}

final case class ParsedConfigs(
  igluJson: Json,
  enrichmentConfigs: List[EnrichmentConf],
  configFile: ConfigFile,
  goodAttributes: EnrichedEvent => Map[String, String],
  piiAttributes: EnrichedEvent => Map[String, String]
)

object ParsedConfigs {

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  /** Schema for all enrichments combined */
  private final val EnrichmentsKey: SchemaKey =
    SchemaKey("com.snowplowanalytics.snowplow", "enrichments", "jsonschema", SchemaVer.Full(1, 0, 0))

  final val enrichedFieldsMap: Map[String, Field] = ConversionUtils.EnrichedFields.map(f => f.getName -> f).toMap

  /** Decode base64-encoded configs, passed via CLI. Read files, validate and parse */
  def parse[F[_]: Async: Clock: ContextShift](config: CliConfig): Parsed[F, ParsedConfigs] =
    for {
      igluJson <- config.resolver.fold(b => EitherT.rightT[F, String](b.value), p => FileSystem.readJson[F](p))
      enrichmentJsons <- config.enrichments match {
                           case Left(base64) =>
                             EitherT.rightT[F, String](base64.value)
                           case Right(path) =>
                             FileSystem
                               .readJsonDir[F](path)
                               .map(jsons => Json.arr(jsons: _*))
                               .map(json => SelfDescribingData(EnrichmentsKey, json).asJson)
                         }
      configFile <- ConfigFile.parse[F](config.config)
      configFile <- validateConfig[F](configFile)
      _ <- EitherT.liftF(
             Logger[F].info(s"Parsed config file: ${configFile}")
           )
      goodAttributes = outputAttributes(configFile.output.good)
      piiAttributes = configFile.output.pii.map(outputAttributes).getOrElse { _: EnrichedEvent => Map.empty[String, String] }
      resolverConfig <-
        EitherT.fromEither[F](Resolver.parseConfig(igluJson)).leftMap(x => show"Cannot decode Iglu resolver from provided json. $x")
      resolver <- Resolver.fromConfig[F](resolverConfig).leftMap(x => show"Cannot create Iglu resolver from provided json. $x")
      client <- EitherT.liftF(IgluCirceClient.fromResolver[F](resolver, resolverConfig.cacheSize))
      _ <- EitherT.liftF(
             Logger[F].info(show"Parsed Iglu Client with following registries: ${resolver.repos.map(_.config.name).mkString(", ")}")
           )
      configs <- EitherT(EnrichmentRegistry.parse[F](enrichmentJsons, client, false).map(_.toEither)).leftMap { x =>
                   show"Cannot decode enrichments - ${x.mkString_(", ")}"
                 }
      _ <- EitherT.liftF(Logger[F].info(show"Parsed following enrichments: ${configs.map(_.schemaKey.name).mkString(", ")}"))
    } yield ParsedConfigs(igluJson, configs, configFile, goodAttributes, piiAttributes)

  private[config] def validateConfig[F[_]: Applicative](configFile: ConfigFile): EitherT[F, String, ConfigFile] = {
    val goodCheck: ValidationResult[OutputConfig] = validateAttributes(configFile.output.good)
    val optPiiCheck: ValidationResult[Option[OutputConfig]] = configFile.output.pii.map(validateAttributes).sequence

    (goodCheck, optPiiCheck)
      .mapN { case (_, _) => configFile }
      .leftMap(nel => s"Invalid attributes: ${nel.toList.mkString("[", ",", "]")}")
      .toEither
      .toEitherT
  }

  private def validateAttributes(output: OutputConfig): ValidationResult[OutputConfig] =
    output match {
      case ps: OutputConfig.PubSub =>
        ps.attributes
          .fold[ValidationResult[OutputConfig]](output.valid) { attributes =>
            val invalidAttributes = attributes.filterNot(enrichedFieldsMap.contains)
            if (invalidAttributes.nonEmpty) NonEmptyList(invalidAttributes.head, invalidAttributes.tail.toList).invalid
            else output.valid
          }
      case OutputConfig.Kinesis(_, _, Some(key), _, _, _, _, _) if !enrichedFieldsMap.contains(key) =>
        NonEmptyList.one(s"Partition key $key not valid").invalid
      case _ =>
        output.valid
    }

  private[config] def outputAttributes(output: OutputConfig): EnrichedEvent => Map[String, String] =
    output match {
      case OutputConfig.PubSub(_, Some(attributes), _, _, _) =>
        val fields = ParsedConfigs.enrichedFieldsMap.filter {
          case (s, _) =>
            attributes.contains(s)
        }
        attributesFromFields(fields)
      case OutputConfig.Kinesis(_, _, Some(key), _, _, _, _, _) =>
        val fields = ParsedConfigs.enrichedFieldsMap.filter {
          case (s, _) =>
            s == key
        }
        attributesFromFields(fields)
      case _ =>
        _ => Map.empty
    }

  private def attributesFromFields(fields: Map[String, Field])(ee: EnrichedEvent): Map[String, String] =
    fields.flatMap {
      case (k, f) =>
        Option(f.get(ee)).map(v => k -> v.toString)
    }
}
