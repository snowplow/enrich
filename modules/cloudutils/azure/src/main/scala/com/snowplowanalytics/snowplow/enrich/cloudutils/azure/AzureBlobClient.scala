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

import com.azure.core.http.HttpHeaderName
import com.azure.core.http.rest.Response
import com.azure.core.util.BinaryData
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.{BlobAsyncClient, BlobServiceAsyncClient, BlobServiceClientBuilder, BlobUrlParts}
import com.azure.storage.blob.models.{BlobRequestConditions, BlobStorageException}

import cats.effect.kernel.{Async, Resource, Sync}
import cats.implicits._

import com.snowplowanalytics.snowplow.enrich.cloudutils.core._

object AzureBlobClient {

  def client[F[_]: Async](config: AzureStorageConfig): BlobClientFactory[F] =
    new BlobClientFactory[F] {
      override def canDownload(uri: URI) =
        uri.toString.contains("core.windows.net")

      override def mk: Resource[F, BlobClient[F]] =
        createClients(config).map { clients =>
          new BlobClient[F] {

            override def get(uri: URI): F[BlobClient.GetResult] =
              findMatchingClient(uri, clients).flatMap { blobClient =>
                Async[F]
                  .fromCompletableFuture {
                    Sync[F].delay(blobClient.downloadContentWithResponse(null, null).toFuture)
                  }
                  .map(processResponse)
              }

            override def getIfNeeded(uri: URI, etag: String): F[BlobClient.GetIfNeededResult] =
              findMatchingClient(uri, clients).flatMap { blobClient =>
                val conditions = new BlobRequestConditions().setIfNoneMatch(etag)

                Async[F]
                  .fromCompletableFuture {
                    Sync[F].delay(blobClient.downloadContentWithResponse(null, conditions).toFuture)
                  }
                  .map[BlobClient.GetIfNeededResult](processResponse)
                  .recoverWith {
                    case bse: BlobStorageException if bse.getStatusCode === 304 =>
                      // 304 Not Modified - etag matched
                      Async[F].pure(BlobClient.EtagMatched)
                  }
              }
          }
        }
    }

  private def createClients[F[_]: Async](config: AzureStorageConfig): Resource[F, Map[String, BlobServiceAsyncClient]] =
    config.accounts
      .traverse { account =>
        createClient(account).map(client => (account.name, client))
      }
      .map(_.toMap)

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

  /**
   * Selects a BlobAsyncClient from the available BlobServiceAsyncClients
   *
   *  The returned client is the one matching the storage account for this uri.
   */
  private def findMatchingClient[F[_]: Async](
    uri: URI,
    clients: Map[String, BlobServiceAsyncClient]
  ): F[BlobAsyncClient] = {
    val inputParts = BlobUrlParts.parse(uri.toString)
    clients.get(inputParts.getAccountName) match {
      case None =>
        Async[F].raiseError(new IllegalStateException(s"AzureStore for storage account name '${inputParts.getAccountName}' isn't found"))
      case Some(serviceClient) =>
        Async[F].pure(
          serviceClient
            .getBlobContainerAsyncClient(inputParts.getBlobContainerName)
            .getBlobAsyncClient(inputParts.getBlobName)
        )
    }
  }

  private def processResponse(response: Response[BinaryData]): BlobClient.GetResult = {
    val content = response.getValue.toByteBuffer
    Option(response.getHeaders.getValue(HttpHeaderName.ETAG)) match {
      case Some(etag) => BlobClient.ContentWithEtag(content, etag)
      case None => BlobClient.ContentNoEtag(content)
    }
  }

}
