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
package com.snowplowanalytics.snowplow.enrich.cloudutils.azure

import java.net.URI

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.{BlobServiceAsyncClient, BlobServiceClientBuilder, BlobUrlParts}

import blobstore.azure.AzureStore
import blobstore.url.exception.{MultipleUrlValidationException, Throwables}
import blobstore.url.{Authority, Path, Url}

import cats.data.Validated.{Invalid, Valid}
import cats.effect.kernel.{Async, Resource, Sync}
import cats.implicits._

import fs2.Stream

import com.snowplowanalytics.snowplow.enrich.cloudutils.core._

object AzureBlobClient {

  private def canDownload(uri: URI): Boolean =
    uri.toString.contains("core.windows.net")

  def client[F[_]: Async](config: AzureStorageConfig): BlobClient[F] =
    new BlobClient[F] {
      override def canDownload(uri: URI) = AzureBlobClient.canDownload(uri)

      override def mk: Resource[F, BlobClientImpl[F]] =
        createStores(config).map { stores =>
          new BlobClientImpl[F] {

            override def canDownload(uri: URI) = AzureBlobClient.canDownload(uri)

            override def download(uri: URI): Stream[F, Byte] = {
              val inputParts = BlobUrlParts.parse(uri.toString)
              stores.get(inputParts.getAccountName) match {
                case None =>
                  Stream.raiseError[F](new Exception(s"AzureStore for storage account name '${inputParts.getAccountName}' isn't found"))
                case Some(store) =>
                  Authority
                    .parse(inputParts.getBlobContainerName)
                    .map(authority => Url(inputParts.getScheme, authority, Path(inputParts.getBlobName))) match {
                    case Valid(url) => store.get(url, 16 * 1024)
                    case Invalid(errors) => Stream.raiseError[F](MultipleUrlValidationException(errors))
                  }
              }
            }
          }
        }
    }

  private def createStores[F[_]: Async](config: AzureStorageConfig): Resource[F, Map[String, AzureStore[F]]] =
    config.accounts
      .map { account =>
        createStore(account).map(store => (account.name, store))
      }
      .sequence
      .map(_.toMap)

  private def createStore[F[_]: Async](account: AzureStorageConfig.Account): Resource[F, AzureStore[F]] =
    for {
      client <- createClient(account)
      store <- AzureStore
                 .builder[F](client)
                 .build
                 .fold(
                   errors => Resource.eval(Sync[F].raiseError(errors.reduce(Throwables.collapsingSemigroup))),
                   s => Resource.pure[F, AzureStore[F]](s)
                 )
    } yield store

  private def createClient[F[_]: Sync](account: AzureStorageConfig.Account): Resource[F, BlobServiceAsyncClient] =
    Resource.eval {
      Sync[F].delay {
        createClientBuilder(account)
          .endpoint(createStorageEndpoint(account.name))
          .buildAsyncClient()
      }
    }

  private def createClientBuilder(account: AzureStorageConfig.Account): BlobServiceClientBuilder = {
    val builder = new BlobServiceClientBuilder()
    account.auth match {
      case Some(AzureStorageConfig.Account.Auth.DefaultCredentialsChain) =>
        builder.credential(new DefaultAzureCredentialBuilder().build)
      case Some(AzureStorageConfig.Account.Auth.SasToken(tokenValue)) =>
        builder.sasToken(tokenValue)
      case None =>
        builder
    }
  }

  private def createStorageEndpoint(storageAccountName: String): String =
    s"https://$storageAccountName.blob.core.windows.net"
}
