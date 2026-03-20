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
import cats.effect.std.{AtomicCell, NonEmptyHotswap}

import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.UUID

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
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

    With multiple enrichment groups:
      Only trigger a swap for the group whose assets changed, not for the unchanged group $e7
      Crash the application when a group's download fails $e8
      Crash the application when a group's onUpdate fails $e9
  """

  def e1 = {
    val enrichmentConf = ipLookupsConf("foo", "foo", "foo", "foo", "foo")
    Ref[IO].of(Vector.empty[Action]).flatMap { state =>
      val blobClients = blobClientsForTests(state, TestBlobClientConfig())
      AssetRefresher
        .initialDownload[IO](List(enrichmentConf), blobClients)
        .use { _ =>
          state.get.map { result =>
            result must beLike {
              case Vector(
                    Action.OpenedBlobClient("foo"),
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
  }

  def e2 = {
    val enrichmentConf = ipLookupsConf("foo", "bar", "baz", "qux", "bat")
    Ref[IO].of(Vector.empty[Action]).flatMap { state =>
      val blobClients = blobClientsForTests(state, TestBlobClientConfig())
      AssetRefresher
        .initialDownload[IO](List(enrichmentConf), blobClients)
        .use { _ =>
          state.get.map { result =>
            result must beLike {
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
                    Action.GettingUri(uri5)
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
  }

  def e3 = {
    val enrichmentConf = ipLookupsConf("foo", "foo", "foo", "foo", "foo")
    testResources.use {
      case (state, onUpdate) =>
        val blobClients = blobClientsForTests(state, TestBlobClientConfig(randomizeContent = false, returnEtags = true))
        AssetRefresher
          .initialDownload[IO](List(enrichmentConf), blobClients)
          .flatMap(d => makeAssetRefresherWithTrackedOnUpdate(d, onUpdate))
          .use { ar =>
            for {
              _ <- ar.refresh
              result <- state.get
            } yield result must beLike {
              case Vector(
                    Action.OpenedEnrichmentRegistry,
                    Action.OpenedBlobClient("foo"),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
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
    val enrichmentConf = ipLookupsConf("foo", "foo", "foo", "foo", "foo")
    testResources.use {
      case (state, onUpdate) =>
        val blobClients = blobClientsForTests(state, TestBlobClientConfig(randomizeContent = true, returnEtags = true))
        AssetRefresher
          .initialDownload[IO](List(enrichmentConf), blobClients)
          .flatMap(d => makeAssetRefresherWithTrackedOnUpdate(d, onUpdate))
          .use { ar =>
            for {
              _ <- ar.refresh
              result <- state.get
            } yield result must beLike {
              case Vector(
                    Action.OpenedEnrichmentRegistry,
                    Action.OpenedBlobClient("foo"),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUriIfNeeded(_, _),
                    Action.GettingUriIfNeeded(_, _),
                    Action.GettingUriIfNeeded(_, _),
                    Action.GettingUriIfNeeded(_, _),
                    Action.GettingUriIfNeeded(_, _),
                    Action.OpenedEnrichmentRegistry,
                    Action.ClosedEnrichmentRegistry
                  ) =>
                ok
            }
          }
    }
  }

  def e5 = {
    val enrichmentConf = ipLookupsConf("foo", "foo", "foo", "foo", "foo")
    testResources.use {
      case (state, onUpdate) =>
        val blobClients = blobClientsForTests(state, TestBlobClientConfig(randomizeContent = false, returnEtags = false))
        AssetRefresher
          .initialDownload[IO](List(enrichmentConf), blobClients)
          .flatMap(d => makeAssetRefresherWithTrackedOnUpdate(d, onUpdate))
          .use { ar =>
            for {
              _ <- ar.refresh
              result <- state.get
            } yield result must beLike {
              case Vector(
                    Action.OpenedEnrichmentRegistry,
                    Action.OpenedBlobClient("foo"),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
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
    val enrichmentConf = ipLookupsConf("foo", "foo", "foo", "foo", "foo")
    testResources.use {
      case (state, onUpdate) =>
        val blobClients = blobClientsForTests(state, TestBlobClientConfig(randomizeContent = true, returnEtags = false))
        AssetRefresher
          .initialDownload[IO](List(enrichmentConf), blobClients)
          .flatMap(d => makeAssetRefresherWithTrackedOnUpdate(d, onUpdate))
          .use { ar =>
            for {
              _ <- ar.refresh
              result <- state.get
            } yield result must beLike {
              case Vector(
                    Action.OpenedEnrichmentRegistry,
                    Action.OpenedBlobClient("foo"),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(_),
                    Action.GettingUri(uri1),
                    Action.GettingUri(uri2),
                    Action.GettingUri(uri3),
                    Action.GettingUri(uri4),
                    Action.GettingUri(uri5),
                    Action.OpenedEnrichmentRegistry,
                    Action.ClosedEnrichmentRegistry
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

  // Two groups: group A has changing assets, group B has stable assets.
  // Only group A's onUpdate should fire.
  def e7 = {
    val confA = ipLookupsConf("foo", "foo", "foo", "foo", "foo", "-e7a")
    val confB = ipLookupsConf("bar", "bar", "bar", "bar", "bar", "-e7b")
    for {
      onUpdateCountA <- Ref[IO].of(0)
      onUpdateCountB <- Ref[IO].of(0)
      state <- Ref[IO].of(Vector.empty[Action])
      result <- {
        val blobClients = List(
          new TestBlobClientFactory("foo", state, TestBlobClientConfig(randomizeContent = true, returnEtags = true)),
          new TestBlobClientFactory("bar", state, TestBlobClientConfig(randomizeContent = false, returnEtags = true))
        )
        AssetRefresher
          .initialDownload[IO](List(confA, confB), blobClients)
          .flatMap { downloaded =>
            makeAssetRefresherWithPerGroupOnUpdates(
              downloaded,
              List(onUpdateCountA.update(_ + 1), onUpdateCountB.update(_ + 1))
            )
          }
          .use { ar =>
            for {
              _ <- ar.refresh
              countA <- onUpdateCountA.get
              countB <- onUpdateCountB.get
            } yield (countA must_== 1) and (countB must_== 0)
          }
      }
    } yield result
  }

  // Group A's download fails; refresh should raise the error and crash.
  def e8 = {
    val confB = ipLookupsConf("bar", "bar", "bar", "bar", "bar", "-e8")
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      result <- {
        val blobClients = List(
          new TestBlobClientFactory("bar", state, TestBlobClientConfig(randomizeContent = true, returnEtags = true))
        )
        AssetRefresher
          .initialDownload[IO](List(confB), blobClients)
          .flatMap { downloaded =>
            // Manually build the failing group so that its download always errors during refresh.
            val failingClient = new TestBlobClient("foo", state, TestBlobClientConfig(failDownload = true))
            val failingAsset = AssetRefresher.Asset[IO](
              failingClient,
              URI.create("foo://example.com/geo"),
              Paths.get("/tmp/failing-e8-asset"),
              AssetRefresher.NotDownloaded
            )
            val failingGroup = AssetRefresher.EnrichmentAssetGroup[IO](
              "failing-test-enrichment",
              List(failingAsset),
              IO.unit
            )
            val groups =
              failingGroup :: downloaded.groups.map(dg =>
                AssetRefresher.EnrichmentAssetGroup[IO](dg.conf.schemaKey.name, dg.assets, IO.unit)
              )
            Resource.eval(AtomicCell[IO].of(groups)).map(new AssetRefresher(_))
          }
          .use { ar =>
            ar.refresh.attempt.map(_ must beLeft)
          }
      }
    } yield result
  }

  // Group A's onUpdate throws; refresh should raise the error and crash.
  def e9 = {
    val confA = ipLookupsConf("foo", "foo", "foo", "foo", "foo", "-e9a")
    val confB = ipLookupsConf("bar", "bar", "bar", "bar", "bar", "-e9b")
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      result <- {
        val blobClients = List(
          new TestBlobClientFactory("foo", state, TestBlobClientConfig(randomizeContent = true, returnEtags = true)),
          new TestBlobClientFactory("bar", state, TestBlobClientConfig(randomizeContent = true, returnEtags = true))
        )
        AssetRefresher
          .initialDownload[IO](List(confA, confB), blobClients)
          .flatMap { downloaded =>
            val failingOnUpdate = IO.raiseError[Unit](new RuntimeException("Simulated onUpdate failure"))
            makeAssetRefresherWithPerGroupOnUpdates(
              downloaded,
              List(failingOnUpdate, IO.unit)
            )
          }
          .use { ar =>
            ar.refresh.attempt.map(_ must beLeft)
          }
      }
    } yield result
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
   *  @param randomizeContent Whether the blob client should return different content for each `get` request.
   *  @param returnEtags Whether to simulate a server that is aware of Etags.
   *  @param failDownload Whether all download attempts (`get` and `getIfNeeded`) should raise an error.
   */
  case class TestBlobClientConfig(
    randomizeContent: Boolean = false,
    returnEtags: Boolean = true,
    failDownload: Boolean = false
  )

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
    prefix5: String,
    localPathSuffix: String = ""
  ): IpLookupsConf =
    IpLookupsConf(
      schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "ip_lookups", "jsonschema", SchemaVer.Full(2, 0, 1)),
      connectionTypeFile = Some(URI.create(s"$prefix1://example.com/connection") -> s"connection-type-file$localPathSuffix"),
      domainFile = Some(URI.create(s"$prefix2://example.com/domain") -> s"domain-file$localPathSuffix"),
      ispFile = Some(URI.create(s"$prefix3://example.com/isp") -> s"isp-file$localPathSuffix"),
      geoFile = Some(URI.create(s"$prefix4://example.com/geo") -> s"geo-file$localPathSuffix"),
      asnFile = Some(URI.create(s"$prefix5://example.com/asn") -> s"asn-file$localPathSuffix")
    )

  /**
   * Resources for tests that need to track open/close events.
   * Returns a state ref and an `onUpdate` callback that records the events.
   * The hotswap lifecycle (initial Open, subsequent Close+Open on swap) is tracked via the
   * `onUpdate` IO rather than via a real hotswap.
   */
  def testResources: Resource[IO, (Ref[IO, Vector[Action]], IO[Unit])] =
    for {
      ref <- Resource.eval(Ref[IO].of(Vector.empty[Action]))
      // We use a NonEmptyHotswap to track the open/close lifecycle accurately.
      // The tracked resource records Open when acquired and Close when released.
      trackedResource: Resource[IO, Unit] =
        Resource.make(ref.update(_ :+ Action.OpenedEnrichmentRegistry))(_ => ref.update(_ :+ Action.ClosedEnrichmentRegistry))
      hs <- NonEmptyHotswap[IO, Unit](trackedResource)
      onUpdate = hs.swap(trackedResource)
    } yield (ref, onUpdate)

  /**
   * Builds an [[AssetRefresher]] from downloaded assets, using the given `onUpdate` IO
   * instead of calling real enrichment constructors. This allows tests to verify
   * Open/Close lifecycle events without requiring real enrichment files on disk.
   */
  def makeAssetRefresherWithTrackedOnUpdate(
    downloaded: AssetRefresher.DownloadedAssets[IO],
    onUpdate: IO[Unit]
  ): Resource[IO, AssetRefresher[IO]] = {
    // Build groups that use the tracked onUpdate instead of the real enrichment constructor.
    val groups = downloaded.groups.map { dg =>
      AssetRefresher.EnrichmentAssetGroup[IO](dg.conf.schemaKey.name, dg.assets, onUpdate)
    }
    Resource.eval(AtomicCell[IO].of(groups)).map(new AssetRefresher(_))
  }

  /**
   * Builds an [[AssetRefresher]] from downloaded assets, with a different `onUpdate` callback
   * per enrichment group. Callbacks are paired with groups in the order they appear.
   */
  def makeAssetRefresherWithPerGroupOnUpdates(
    downloaded: AssetRefresher.DownloadedAssets[IO],
    onUpdates: List[IO[Unit]]
  ): Resource[IO, AssetRefresher[IO]] = {
    val groups = downloaded.groups.zip(onUpdates).map {
      case (dg, onUpdate) => AssetRefresher.EnrichmentAssetGroup[IO](dg.conf.schemaKey.name, dg.assets, onUpdate)
    }
    Resource.eval(AtomicCell[IO].of(groups)).map(new AssetRefresher(_))
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
        if (config.failDownload)
          IO.raiseError(new RuntimeException(s"Simulated download failure for $uri"))
        else
          state.update(_ :+ Action.GettingUri(uri.toString)).as {
            val returnedEtag = if (config.randomizeContent) UUID.randomUUID.toString else ConsistentTestEtag
            if (config.returnEtags) BlobClient.ContentWithEtag(newTestByteBuffer(config.randomizeContent), returnedEtag)
            else BlobClient.ContentNoEtag(newTestByteBuffer(config.randomizeContent))
          }
      else
        state.update(_ :+ Action.InvalidOperation) >> IO.raiseError(new IllegalStateException("Blob client invoked with invalid uri"))

    def getIfNeeded(uri: URI, etag: String): IO[BlobClient.GetIfNeededResult] =
      if (uri.getScheme === prefix)
        if (config.failDownload)
          IO.raiseError(new RuntimeException(s"Simulated download failure for $uri"))
        else
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
