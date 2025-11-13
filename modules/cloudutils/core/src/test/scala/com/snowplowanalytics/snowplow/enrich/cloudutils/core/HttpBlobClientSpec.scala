/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.cloudutils.core

import cats.implicits._
import cats.effect.IO
import cats.effect.Deferred
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect

import org.http4s.client.Client
import org.http4s.HttpApp
import org.http4s.headers.ETag
import org.http4s.dsl.io._

import java.net.URI
import java.nio.charset.StandardCharsets

class HttpBlobClientSpec extends Specification with CatsEffect {
  import HttpBlobClientSpec._

  def is = s2"""
  The HttpBlobClient `get` method should:
    Return content and etag, for a server that supports etags $e1
    Return content without etag, for a server that does not support etags $e2
    Return an error for a 404 response $e3
  The HttpBlobClient `getIfNeeded` method should:
    Send If-None-Match headers with the request $e4
    Return content and etag if requested etag does not match server's etag $e5
    Return EtagMatched if requested etag matches server's etag $e6
  """

  def e1 = {
    val blobClient = HttpBlobClient.wrapHttp4sClient(testHttp4sClient(supportEtag = true))
    blobClient.mk
      .use { impl =>
        impl.get(testUri)
      }
      .map { result =>
        result must beLike {
          case BlobClient.ContentWithEtag(content, TestEtag) =>
            StandardCharsets.UTF_8.decode(content).toString must beEqualTo(testContent)
        }
      }
  }

  def e2 = {
    val blobClient = HttpBlobClient.wrapHttp4sClient(testHttp4sClient(supportEtag = false))
    blobClient.mk
      .use { impl =>
        impl.get(testUri)
      }
      .map { result =>
        result must beLike {
          case BlobClient.ContentNoEtag(content) =>
            StandardCharsets.UTF_8.decode(content).toString must beEqualTo(testContent)
        }
      }
  }

  def e3 = {
    val blobClient = HttpBlobClient.wrapHttp4sClient(testHttp4sClient(supportEtag = true))
    val wrongUri = URI.create("https://example.com/different-path")
    blobClient.mk
      .use { impl =>
        impl.get(wrongUri).attempt
      }
      .map { result =>
        result must beLeft(haveClass[HttpBlobClient.HttpErrorResponse])
      }
  }

  def e4 =
    recordingHttp4sClient.flatMap {
      case (client, deferred) =>
        val blobClient = HttpBlobClient.wrapHttp4sClient(client)
        val testEtag = "my-etag"
        blobClient.mk.use { impl =>
          impl.getIfNeeded(testUri, testEtag)
        } *> deferred.get.map { headers =>
          headers must contain(s"""If-None-Match: "$testEtag"""")
        }
    }

  def e5 = {
    val blobClient = HttpBlobClient.wrapHttp4sClient(testHttp4sClient(supportEtag = true))
    val wrongEtag = "different-etag"
    blobClient.mk
      .use { impl =>
        impl.getIfNeeded(testUri, wrongEtag)
      }
      .map { result =>
        result must beLike {
          case BlobClient.ContentWithEtag(content, TestEtag) =>
            StandardCharsets.UTF_8.decode(content).toString must beEqualTo(testContent)
        }
      }
  }

  def e6 = {
    val blobClient = HttpBlobClient.wrapHttp4sClient(testHttp4sClient(supportEtag = true, return304 = true))
    blobClient.mk
      .use { impl =>
        impl.getIfNeeded(testUri, TestEtag)
      }
      .map { result =>
        result must beEqualTo(BlobClient.EtagMatched)
      }
  }
}

object HttpBlobClientSpec {

  val testUri = URI.create("https://example.com/foo")
  val testContent = "test content"
  val TestEtag = "test-etag-123"

  /**
   * A test http4s client which returns a 200 response code only if the get request matches exactly the testUri.  Otherwise returns a 404.
   *
   *  @param supportEtag Whether we are mocking a server that supports Etags
   *  @param return304 Whether to return a 304 Not Modified response
   */
  def testHttp4sClient(supportEtag: Boolean, return304: Boolean = false): Client[IO] =
    Client.fromHttpApp(
      HttpApp[IO] { request =>
        if (request.uri.renderString === testUri.toString)
          if (return304)
            // Return 304 Not Modified
            NotModified()
          else {
            // Return 200 with content and optionally an etag
            val response = Ok(testContent)
            if (supportEtag)
              response.map(_.withHeaders(ETag(ETag.EntityTag(TestEtag))))
            else
              response
          }
        else
          // Return 404 for any other URI
          NotFound()
      }
    )

  /**
   * A test http4s client that records request headers and returns OK for all requests.
   *  Returns a tuple of (client, deferred) where the deferred will contain the recorded headers.
   */
  def recordingHttp4sClient: IO[(Client[IO], Deferred[IO, List[String]])] =
    Deferred[IO, List[String]].map { deferred =>
      val client = Client.fromHttpApp(
        HttpApp[IO] { request =>
          val headers = request.headers.headers.map(_.show).toList
          deferred.complete(headers) *> Ok()
        }
      )
      (client, deferred)
    }
}
