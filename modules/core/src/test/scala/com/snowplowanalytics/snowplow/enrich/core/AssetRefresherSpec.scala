/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.core

import cats.implicits._
import cats.effect.{IO, Ref, Resource}
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect

import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.runtime.processing.Coldswap
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf.IpLookupsConf
import com.snowplowanalytics.snowplow.enrich.cloudutils.core.{BlobClient, BlobClientFactory}

class AssetRefresherSpec extends Specification with CatsEffect {
  import AssetRefresherSpec._

  def is = s2"""
  The AssetRefresher should:
    Open a single blob client when all uri prefixes are the same $e1
    Open multiple blob clients when uri prefixes are all different $e2

    When refreshing assets from a server that returns Etags:
      Do not close and re-open enrichment registry if etags do not change $e3
      Close and re-open enrichment registry if etags change $e4

    When refreshing assets from a server that does *not* return Etags:
      Do not close and re-open enrichment registry if asset content does not change $e5
      Close and re-open enrichment registry if asset content changes $e6
  """

  def e1 = {
    val enrichmentConf = ipLookupsConf("foo", "foo", "foo", "foo", "foo") // An IPLookupsConf with same prefix for all 5 URIs
    testResources.use {
      case (coldswap, state) =>
        val blobClients = blobClientsForTests(state, TestBlobClientConfig())
        AssetRefresher.fromEnrichmentConfs(List(enrichmentConf), blobClients, coldswap).use { ar =>
          for {
            _ <- ar.refreshAndOpenRegistry
            result <- state.get
          } yield result must beLike {
            case Vector(
                  Action.OpenedBlobClient("foo"),
                  Action.GettingUri(uri1),
                  Action.GettingUri(uri2),
                  Action.GettingUri(uri3),
                  Action.GettingUri(uri4),
                  Action.GettingUri(uri5),
                  Action.OpenedEnrichmentRegistry
                ) =>
              (List(uri1, uri2, uri3, uri4, uri5) must contain(
                allOf("foo://example.com/connection",
                      "foo://example.com/domain",
                      "foo://example.com/isp",
                      "foo://example.com/geo",
                      "foo://example.com/asn"
                )
              ))
          }
        }
    }
  }

  def e2 = {
    val enrichmentConf = ipLookupsConf("foo", "bar", "baz", "qux", "bat") // An IPLookupsConf with different prefix for all 5 URIs
    testResources.use {
      case (coldswap, state) =>
        val blobClients = blobClientsForTests(state, TestBlobClientConfig())
        AssetRefresher.fromEnrichmentConfs(List(enrichmentConf), blobClients, coldswap).use { ar =>
          for {
            _ <- ar.refreshAndOpenRegistry
            result <- state.get
          } yield result must beLike {
            case Vector(
                  Action.OpenedBlobClient(p1),
                  Action.OpenedBlobClient(p2),
                  Action.OpenedBlobClient(p3),
                  Action.OpenedBlobClient(p4),
                  Action.OpenedBlobClient(p5),
                  Action.GettingUri(uri1),
                  Action.GettingUri(uri2),
                  Action.GettingUri(uri3),
                  Action.GettingUri(uri4),
                  Action.GettingUri(uri5),
                  Action.OpenedEnrichmentRegistry
                ) =>
              (List(p1, p2, p3, p4, p5) must contain(allOf("foo", "bar", "baz", "qux", "bat"))) and
                (List(uri1, uri2, uri3, uri4, uri5) must contain(
                  allOf("foo://example.com/connection",
                        "bar://example.com/domain",
                        "baz://example.com/isp",
                        "qux://example.com/geo",
                        "bat://example.com/asn"
                  )
                ))
          }
        }
    }
  }

  def e3 = {
    val enrichmentConf = ipLookupsConf("foo", "foo", "foo", "foo", "foo") // An IPLookupsConf with same prefix for all 5 URIs
    testResources.use {
      case (coldswap, state) =>
        val blobClients = blobClientsForTests(state, TestBlobClientConfig(randomizeContent = false, returnEtags = true))
        AssetRefresher.fromEnrichmentConfs(List(enrichmentConf), blobClients, coldswap).use { ar =>
          for {
            _ <- ar.refreshAndOpenRegistry
            _ <- ar.refreshAndOpenRegistry
            result <- state.get
          } yield result must beLike {
            case Vector(
                  Action.OpenedBlobClient("foo"),
                  Action.GettingUri(_),
                  Action.GettingUri(_),
                  Action.GettingUri(_),
                  Action.GettingUri(_),
                  Action.GettingUri(_),
                  Action.OpenedEnrichmentRegistry,
                  Action.GettingUriIfNeeded(uri1, ConsistentTestEtag),
                  Action.GettingUriIfNeeded(uri2, ConsistentTestEtag),
                  Action.GettingUriIfNeeded(uri3, ConsistentTestEtag),
                  Action.GettingUriIfNeeded(uri4, ConsistentTestEtag),
                  Action.GettingUriIfNeeded(uri5, ConsistentTestEtag)
                ) =>
              (List(uri1, uri2, uri3, uri4, uri5) must contain(
                allOf("foo://example.com/connection",
                      "foo://example.com/domain",
                      "foo://example.com/isp",
                      "foo://example.com/geo",
                      "foo://example.com/asn"
                )
              ))
          }
        }
    }
  }

  def e4 = {
    val enrichmentConf = ipLookupsConf("foo", "foo", "foo", "foo", "foo") // An IPLookupsConf with same prefix for all 5 URIs
    testResources.use {
      case (coldswap, state) =>
        val blobClients = blobClientsForTests(state, TestBlobClientConfig(randomizeContent = true, returnEtags = true))
        AssetRefresher.fromEnrichmentConfs(List(enrichmentConf), blobClients, coldswap).use { ar =>
          for {
            _ <- ar.refreshAndOpenRegistry
            _ <- ar.refreshAndOpenRegistry
            result <- state.get
          } yield result must beLike {
            case Vector(
                  Action.OpenedBlobClient("foo"),
                  Action.GettingUri(_),
                  Action.GettingUri(_),
                  Action.GettingUri(_),
                  Action.GettingUri(_),
                  Action.GettingUri(_),
                  Action.OpenedEnrichmentRegistry,
                  Action.GettingUriIfNeeded(_, _),
                  Action.GettingUriIfNeeded(_, _),
                  Action.GettingUriIfNeeded(_, _),
                  Action.GettingUriIfNeeded(_, _),
                  Action.GettingUriIfNeeded(_, _),
                  Action.ClosedEnrichmentRegistry,
                  Action.OpenedEnrichmentRegistry
                ) =>
              ok
          }
        }
    }
  }

  def e5 = {
    val enrichmentConf = ipLookupsConf("foo", "foo", "foo", "foo", "foo") // An IPLookupsConf with same prefix for all 5 URIs
    testResources.use {
      case (coldswap, state) =>
        val blobClients = blobClientsForTests(state, TestBlobClientConfig(randomizeContent = false, returnEtags = false))
        AssetRefresher.fromEnrichmentConfs(List(enrichmentConf), blobClients, coldswap).use { ar =>
          for {
            _ <- ar.refreshAndOpenRegistry
            _ <- ar.refreshAndOpenRegistry
            result <- state.get
          } yield result must beLike {
            case Vector(
                  Action.OpenedBlobClient("foo"),
                  Action.GettingUri(_),
                  Action.GettingUri(_),
                  Action.GettingUri(_),
                  Action.GettingUri(_),
                  Action.GettingUri(_),
                  Action.OpenedEnrichmentRegistry,
                  Action.GettingUri(uri1),
                  Action.GettingUri(uri2),
                  Action.GettingUri(uri3),
                  Action.GettingUri(uri4),
                  Action.GettingUri(uri5)
                ) =>
              (List(uri1, uri2, uri3, uri4, uri5) must contain(
                allOf("foo://example.com/connection",
                      "foo://example.com/domain",
                      "foo://example.com/isp",
                      "foo://example.com/geo",
                      "foo://example.com/asn"
                )
              ))
          }
        }
    }
  }

  def e6 = {
    val enrichmentConf = ipLookupsConf("foo", "foo", "foo", "foo", "foo") // An IPLookupsConf with same prefix for all 5 URIs
    testResources.use {
      case (coldswap, state) =>
        val blobClients = blobClientsForTests(state, TestBlobClientConfig(randomizeContent = true, returnEtags = false))
        AssetRefresher
          .fromEnrichmentConfs(List(enrichmentConf), blobClients, coldswap)
          .use { ar =>
            for {
              _ <- ar.refreshAndOpenRegistry
              _ <- ar.refreshAndOpenRegistry
              result <- state.get
            } yield result must beLike {
              case Vector(
                    Action.OpenedBlobClient("foo"),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.OpenedEnrichmentRegistry,
                    Action.GettingUri(uri1),
                    Action.GettingUri(uri2),
                    Action.GettingUri(uri3),
                    Action.GettingUri(uri4),
                    Action.GettingUri(uri5),
                    Action.ClosedEnrichmentRegistry,
                    Action.OpenedEnrichmentRegistry
                  ) =>
                (List(uri1, uri2, uri3, uri4, uri5) must contain(
                  allOf("foo://example.com/connection",
                        "foo://example.com/domain",
                        "foo://example.com/isp",
                        "foo://example.com/geo",
                        "foo://example.com/asn"
                  )
                ))
            }
          }
    }
  }
}

object AssetRefresherSpec {

  // For recording the actions invoked by the asset refresher during the spec
  sealed trait Action
  object Action {
    case object OpenedEnrichmentRegistry extends Action
    case object ClosedEnrichmentRegistry extends Action
    case class OpenedBlobClient(prefix: String) extends Action
    case class ClosedBlobClient(prefix: String) extends Action
    case class GettingUri(uri: String) extends Action
    case class GettingUriIfNeeded(uri: String, etag: String) extends Action
    case object InvalidOperation extends Action
  }

  // A re-usable etag that never changes
  val ConsistentTestEtag: String = UUID.randomUUID.toString

  /**
   * Configures the BlobClient used for tests
   *
   *  @param randomizeContent Whether the blob client should return different content for each `get` request. Useful for checking the md5 feature.  If Etags are enabled, it also enables returning a different Etag each time.
   *  @param returnEtags Whether to simulate a server that is aware of Etags.  If true, the blob client returns the content and an etag.
   */
  case class TestBlobClientConfig(randomizeContent: Boolean = false, returnEtags: Boolean = true)

  /** A list of 5 blob clients, each of which is able to download from a different uri-prefix */
  def blobClientsForTests(
    state: Ref[IO, Vector[Action]],
    config: TestBlobClientConfig
  ): List[BlobClientFactory[IO]] =
    List(
      new TestBlobClientFactory("foo", state, config),
      new TestBlobClientFactory("bar", state, config),
      new TestBlobClientFactory("baz", state, config),
      new TestBlobClientFactory("qux", state, config),
      new TestBlobClientFactory("bat", state, config)
    )

  def ipLookupsConf(
    prefix1: String,
    prefix2: String,
    prefix3: String,
    prefix4: String,
    prefix5: String
  ): IpLookupsConf =
    IpLookupsConf(
      schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "ip_lookups", "jsonschema", SchemaVer.Full(2, 0, 1)),
      connectionTypeFile = Some(URI.create(s"$prefix1://example.com/connection") -> "connection-type-file"),
      domainFile = Some(URI.create(s"$prefix2://example.com/domain") -> "domain-file"),
      ispFile = Some(URI.create(s"$prefix3://example.com/isp") -> "isp-file"),
      geoFile = Some(URI.create(s"$prefix4://example.com/geo") -> "geo-file"),
      asnFile = Some(URI.create(s"$prefix5://example.com/asn") -> "asn-file")
    )

  def testResources: Resource[IO, (Coldswap[IO, EnrichmentRegistry[IO]], Ref[IO, Vector[Action]])] =
    for {
      ref <- Resource.eval(Ref[IO].of(Vector.empty[Action]))
      c <- testColdswap(ref)
    } yield (c, ref)

  def testColdswap(state: Ref[IO, Vector[Action]]): Resource[IO, Coldswap[IO, EnrichmentRegistry[IO]]] =
    Coldswap.make {
      Resource
        .make(state.update(_ :+ Action.OpenedEnrichmentRegistry))(_ => state.update(_ :+ Action.ClosedEnrichmentRegistry))
        .as(EnrichmentRegistry())
    }

  class TestBlobClientFactory(
    prefix: String,
    state: Ref[IO, Vector[Action]],
    config: TestBlobClientConfig
  ) extends BlobClientFactory[IO] {

    override def canDownload(uri: URI): Boolean =
      uri.getScheme === prefix

    override def mk: Resource[IO, BlobClient[IO]] =
      Resource.make(state.update(_ :+ Action.OpenedBlobClient(prefix)))(_ => state.update(_ :+ Action.ClosedBlobClient(prefix))).as {
        new TestBlobClient(prefix, state, config)
      }
  }

  private def newTestByteBuffer(randomizeContent: Boolean) = {
    val stringContent = if (randomizeContent) UUID.randomUUID.toString else "foo"
    ByteBuffer.wrap(stringContent.getBytes(StandardCharsets.UTF_8))
  }

  class TestBlobClient(
    prefix: String,
    state: Ref[IO, Vector[Action]],
    config: TestBlobClientConfig
  ) extends BlobClient[IO] {

    def get(uri: URI): IO[BlobClient.GetResult] =
      if (uri.getScheme === prefix)
        state.update(_ :+ Action.GettingUri(uri.toString)).as {
          val returnedEtag = if (config.randomizeContent) UUID.randomUUID.toString else ConsistentTestEtag
          if (config.returnEtags) BlobClient.ContentWithEtag(newTestByteBuffer(config.randomizeContent), returnedEtag)
          else BlobClient.ContentNoEtag(newTestByteBuffer(config.randomizeContent))
        }
      else
        state.update(_ :+ Action.InvalidOperation) >> IO.raiseError(new IllegalStateException("Blob client invoked with invalid uri"))

    def getIfNeeded(uri: URI, etag: String): IO[BlobClient.GetIfNeededResult] =
      if (uri.getScheme === prefix)
        state.update(_ :+ Action.GettingUriIfNeeded(uri.toString, etag)).as {
          if (config.returnEtags) {
            val returnedEtag = if (config.randomizeContent) UUID.randomUUID.toString else ConsistentTestEtag
            BlobClient.ContentWithEtag(newTestByteBuffer(config.randomizeContent), returnedEtag)
          } else BlobClient.ContentNoEtag(newTestByteBuffer(config.randomizeContent))
        }
      else
        state.update(_ :+ Action.InvalidOperation) >> IO.raiseError(new IllegalStateException("Blob client invoked with invalid uri"))
  }
}
