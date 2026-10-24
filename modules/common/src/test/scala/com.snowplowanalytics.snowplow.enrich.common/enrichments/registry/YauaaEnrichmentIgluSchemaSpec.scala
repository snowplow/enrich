/**
 * Copyright (c) 2019-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import java.io.InputStream
import java.util.jar.JarFile

import scala.jdk.CollectionConverters._

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import cats.implicits._

import org.specs2.Specification

import nl.basjes.parse.useragent.UserAgentAnalyzer

import org.yaml.snakeyaml.{LoaderOptions, Yaml}

import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.{JavaNetRegistryLookup, Registry, RegistryLookup}

/**
 * Validates that all user agents in the YAUAA library's own test corpus produce an entity that
 * conforms to the iglu:nl.basjes/yauaa_context/jsonschema/1-0-5 schema.
 *
 * The corpus is loaded directly from the yauaa jar on the classpath, so bumping the library
 * version in Dependencies.scala automatically updates the test corpus with no other changes needed.
 */
class YauaaEnrichmentIgluSchemaSpec extends Specification with CatsEffect {
  def is = s2"""
  The YAUAA test corpus contains user agents                               $e1
  All user agents in the YAUAA test corpus produce a schema-valid entity  $e2
  """

  private val yauaaEnrichment = YauaaEnrichment(None)
  implicit private val registryLookup: RegistryLookup[IO] = JavaNetRegistryLookup.ioLookupInstance[IO]

  // SnakeYAML 2.x caps non-scalar YAML aliases at 50 by default as a DoS protection.
  // Some yauaa YAML files exceed this limit. Since we're loading controlled library
  // content rather than user input, it is safe to remove the cap.
  private val yamlAliasLimit: Int = Int.MaxValue

  // Guards against the schema validation test passing vacuously: if the corpus were
  // empty it would trivially succeed (no user agents means no failures). That could
  // happen silently if yauaa stops bundling YAML test cases in the jar, or if the
  // YAML structure changes such that no user_agent_string fields are extracted.
  def e1 = {
    val userAgents = userAgentsFromYauaaCorpus()
    userAgents must not be empty
  }

  def e2 =
    for {
      client <- igluClient
      userAgents = userAgentsFromYauaaCorpus()
      results <- userAgents.traverse { ua =>
                   val sdj = yauaaEnrichment.getYauaaContext(ua, Nil)
                   client.check(sdj).value.map(ua -> _)
                 }
      failures = results.collect { case (ua, Left(err)) => s"[$ua]: $err" }
    } yield failures must beEmpty

  private def igluClient: IO[IgluCirceClient[IO]] =
    IgluCirceClient.fromResolver[IO](
      Resolver[IO](List(Registry.EmbeddedRegistry), None),
      cacheSize = 500,
      maxJsonDepth = 40
    )

  private def userAgentsFromYauaaCorpus(): List[String] = {
    val jarUrl = classOf[UserAgentAnalyzer].getProtectionDomain.getCodeSource.getLocation
    val jarFile = new JarFile(new java.io.File(jarUrl.toURI))
    val loaderOptions = new LoaderOptions()
    loaderOptions.setMaxAliasesForCollections(yamlAliasLimit)
    val yaml = new Yaml(loaderOptions)
    jarFile
      .entries()
      .asScala
      .filter(e => e.getName.startsWith("UserAgents/") && e.getName.endsWith(".yaml"))
      .flatMap { entry =>
        val stream = jarFile.getInputStream(entry)
        try extractUserAgents(yaml, stream)
        finally stream.close()
      }
      .toList
  }

  private def extractUserAgents(yaml: Yaml, stream: InputStream): List[String] = {
    // Each yauaa YAML file has the structure:
    //   config:
    //     - matcher: ...     <- skipped
    //     - test:
    //         input:
    //           user_agent_string: '...'
    //         expected: ...  <- ignored
    def asJavaMap(obj: Any): Option[java.util.Map[String, Any]] =
      obj match {
        case m: java.util.Map[_, _] => Some(m.asInstanceOf[java.util.Map[String, Any]])
        case _ => None
      }

    val loaded = yaml.loadAs(stream, classOf[java.util.Map[String, Any]])
    val configItems: List[Any] = (for {
      topLevel <- Option(loaded)
      config <- Option(topLevel.get("config"))
      list <- config match {
                case l: java.util.List[_] => Some(l.asScala.toList)
                case _ => None
              }
    } yield list).getOrElse(Nil)

    configItems.flatMap { item =>
      for {
        itemMap <- asJavaMap(item)
        testNode <- Option(itemMap.get("test"))
        testMap <- asJavaMap(testNode)
        inputNode <- Option(testMap.get("input"))
        inputMap <- asJavaMap(inputNode)
        ua <- Option(inputMap.get("user_agent_string"))
      } yield ua.toString
    }
  }
}
