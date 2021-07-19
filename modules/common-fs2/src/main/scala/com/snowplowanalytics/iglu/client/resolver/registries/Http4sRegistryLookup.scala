/*
 * Copyright (c) 2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.iglu.client.resolver.registries

import cats.implicits._
import cats.data.EitherT
import cats.effect.Sync
import io.circe.Json
import org.http4s.circe._
import org.http4s.client.{Client => HttpClient}
import org.http4s.{EntityDecoder, Header, Headers, Request, Uri}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaList}
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
import org.http4s.Method.GET

object Http4sRegistryLookup {

  def apply[F[_]: Sync](client: HttpClient[F]): RegistryLookup[F] =
    new RegistryLookup[F] {
      def lookup(repositoryRef: Registry, schemaKey: SchemaKey): F[Either[RegistryError, Json]] =
        repositoryRef match {
          case Registry.Http(_, connection) => httpLookup(client, connection, schemaKey).value
          case Registry.Embedded(_, path) => RegistryLookup.embeddedLookup[F](path, schemaKey)
          case Registry.InMemory(_, schemas) => Sync[F].pure(RegistryLookup.inMemoryLookup(schemas, schemaKey))
        }

      def list(
        registry: Registry,
        vendor: String,
        name: String,
        model: Int
      ): F[Either[RegistryError, SchemaList]] =
        registry match {
          case Registry.Http(_, connection) => httpList(client, connection, vendor, name, model).value
          case _ => Sync[F].pure(RegistryError.NotFound.asLeft)
        }
    }

  def httpLookup[F[_]: Sync](
    client: HttpClient[F],
    http: Registry.HttpConnection,
    key: SchemaKey
  ): EitherT[F, RegistryError, Json] =
    for {
      uri <- EitherT.fromEither[F](toPath(http, key))
      headers = http.apikey.fold[Headers](Headers.empty)(apikey => Headers.of(Header("apikey", apikey)))
      request = Request[F](method = GET, uri = uri, headers = headers)
      json <- EitherT(Sync[F].attempt(client.expect[Json](request))).leftMap[RegistryError] { e =>
                val error = s"Unexpected exception fetching: $e"
                RegistryError.RepoFailure(error)
              }
    } yield json

  def httpList[F[_]: Sync](
    client: HttpClient[F],
    http: Registry.HttpConnection,
    vendor: String,
    name: String,
    model: Int
  ): EitherT[F, RegistryError, SchemaList] =
    for {
      uri <- EitherT.fromEither[F](toSubpath(http, vendor, name, model))
      sl <- EitherT(Sync[F].attempt(client.expect[SchemaList](uri))).leftMap[RegistryError] { e =>
              val error = s"Unexpected exception listing: $e"
              RegistryError.RepoFailure(error)
            }
    } yield sl

  def toPath(cxn: Registry.HttpConnection, key: SchemaKey): Either[RegistryError, Uri] =
    Uri
      .fromString(s"${cxn.uri.toString.stripSuffix("/")}/schemas/${key.toPath}")
      .leftMap(e => RegistryError.ClientFailure(e.message))

  def toSubpath(
    cxn: Registry.HttpConnection,
    vendor: String,
    name: String,
    model: Int
  ): Either[RegistryError, Uri] =
    Uri
      .fromString(s"${cxn.uri.toString.stripSuffix("/")}/schemas/$vendor/$name/jsonschema/$model")
      .leftMap(e => RegistryError.ClientFailure(e.message))

  implicit def schemaListDecoder[F[_]: Sync]: EntityDecoder[F, SchemaList] = jsonOf[F, SchemaList]
}
