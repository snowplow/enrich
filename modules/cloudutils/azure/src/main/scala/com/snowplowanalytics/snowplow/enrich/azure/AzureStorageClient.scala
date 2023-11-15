/*
 * Copyright (c) 2023 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.azure

import java.net.URI

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.{BlobServiceClientBuilder, BlobUrlParts}

import blobstore.azure.AzureStore
import blobstore.url.exception.{AuthorityParseError, MultipleUrlValidationException, Throwables}
import blobstore.url.{Authority, Path, Url}

import cats.data.Validated.{Invalid, Valid}
import cats.data.ValidatedNec
import cats.implicits._

import cats.effect.kernel.{Async, Resource, Sync}

import fs2.Stream

import com.snowplowanalytics.snowplow.enrich.common.fs2.io.Clients.Client

object AzureStorageClient {

  def mk[F[_]: Async](storageAccountName: String): Resource[F, Client[F]] =
    for {
      store <- createStore(storageAccountName)
    } yield new Client[F] {
      def canDownload(uri: URI): Boolean =
        uri.toString.contains("core.windows.net")

      def download(uri: URI): Stream[F, Byte] =
        createStorageUrlFrom(uri.toString) match {
          case Valid(url) => store.get(url, 16 * 1024)
          case Invalid(errors) => Stream.raiseError[F](MultipleUrlValidationException(errors))
        }
    }

  private def createStore[F[_]: Async](storageAccountName: String): Resource[F, AzureStore[F]] =
    for {
      client <- Resource.eval {
        Sync[F].delay {
          val builder = new BlobServiceClientBuilder().credential(new DefaultAzureCredentialBuilder().build)
          val storageEndpoint = createStorageEndpoint(storageAccountName)
          builder.endpoint(storageEndpoint).buildAsyncClient()
        }
      }
      store <- AzureStore
        .builder[F](client)
        .build
        .fold(
          errors => Resource.eval(Sync[F].raiseError(errors.reduce(Throwables.collapsingSemigroup))),
          s => Resource.pure[F, AzureStore[F]](s)
        )
    } yield store

  private def createStorageUrlFrom(input: String): ValidatedNec[AuthorityParseError, Url[String]] = {
    val inputParts = BlobUrlParts.parse(input)
    Authority
      .parse(inputParts.getBlobContainerName)
      .map(authority => Url(inputParts.getScheme, authority, Path(inputParts.getBlobName)))
  }

  private def createStorageEndpoint(storageAccountName: String): String =
    s"https://$storageAccountName.blob.core.windows.net"
}
