/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.enrich.common.utils

import cats.effect.Sync
import cats.implicits._
import fs2.Stream
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
    val alwaysIncludedHeaders = List(Header("content-type", "application/json"), Header("accept", "*/*"))
    if (authUser.isDefined || authPassword.isDefined)
      Headers(Authorization(BasicCredentials(authUser.getOrElse(""), authPassword.getOrElse(""))) :: alwaysIncludedHeaders)
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
              .use[F, Either[Throwable, String]] { response =>
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
