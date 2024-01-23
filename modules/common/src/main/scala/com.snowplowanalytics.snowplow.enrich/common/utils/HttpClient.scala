/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import cats.effect.kernel.Sync
import cats.implicits._
import fs2.Stream
import org.typelevel.ci.CIString
import org.http4s.client.{Client => Http4sClient}
import org.http4s.headers.Authorization
import org.http4s.{BasicCredentials, EmptyBody, EntityBody, Header, Headers, Method, Request, Status, Uri}

trait HttpClient[F[_]] {
  def getResponse(
    uri: String,
    authUser: Option[String],
    authPassword: Option[String],
    body: Option[String],
    method: String
  ): F[Either[Throwable, String]]
}

object HttpClient {

  private[utils] def getHeaders(authUser: Option[String], authPassword: Option[String]): Headers = {
    val alwaysIncludedHeaders = List(
      Header.Raw(CIString("content-type"), "application/json"),
      Header.Raw(CIString("accept"), "*/*")
    )
    if (authUser.isDefined || authPassword.isDefined)
      Headers(
        Authorization(BasicCredentials(authUser.getOrElse(""), authPassword.getOrElse(""))),
        alwaysIncludedHeaders
      )
    else Headers(alwaysIncludedHeaders)
  }

  def fromHttp4sClient[F[_]: Sync](http4sClient: Http4sClient[F]): HttpClient[F] =
    new HttpClient[F] {
      override def getResponse(
        uri: String,
        authUser: Option[String],
        authPassword: Option[String],
        body: Option[String],
        method: String
      ): F[Either[Throwable, String]] =
        Uri.fromString(uri) match {
          case Left(parseFailure) =>
            Sync[F].pure(new IllegalArgumentException(s"uri [$uri] is not valid: ${parseFailure.sanitized}").asLeft[String])
          case Right(validUri) =>
            val request = Request[F](
              uri = validUri,
              method = Method.fromString(method).getOrElse(Method.GET),
              body = body.fold[EntityBody[F]](EmptyBody)(s => Stream.emits(s.getBytes)),
              headers = getHeaders(authUser, authPassword)
            )
            http4sClient
              .run(request)
              .use[Either[Throwable, String]] { response =>
                val body = response.bodyText.compile.string
                response.status.responseClass match {
                  case Status.Successful => body.map(_.asRight[Throwable])
                  case _ => Sync[F].pure(new Exception(s"Request failed with status ${response.status.code}").asLeft[String])
                }
              }
              .handleError(_.asLeft[String])
        }
    }
}
