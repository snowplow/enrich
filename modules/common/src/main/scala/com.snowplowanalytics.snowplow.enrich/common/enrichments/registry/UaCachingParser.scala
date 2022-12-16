package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import cats.Id
import com.snowplowanalytics.lrumap.{CreateLruMap, LruMap}
import ua_parser._

import java.io.InputStream

object UaCachingParser {
  private val defaultCacheSize = 10000

  def create(): Parser = {
    val cache = createCache()
    new NoCustomRegexYaml(cache)
  }

  def create(regexYaml: InputStream): Parser = {
    val cache = createCache()
    new CustomRegexYaml(cache, regexYaml)
  }

  private def createCache(): ParserCache =
    ParserCache(
      client = createLruMap[Client],
      userAgent = createLruMap[UserAgent],
      device = createLruMap[Device],
      os = createLruMap[OS]
    )

  private final case class ParserCache(
    client: LruMap[Id, String, Client],
    userAgent: LruMap[Id, String, UserAgent],
    device: LruMap[Id, String, Device],
    os: LruMap[Id, String, OS]
  )

  private trait LruMapCachingParser extends Parser {
    def cache: ParserCache

    override def parse(agentString: String): Client =
      lookupOrEvaluate(key = agentString, cache = cache.client, onMiss = super.parse)

    override def parseUserAgent(agentString: String): UserAgent =
      lookupOrEvaluate(key = agentString, cache = cache.userAgent, onMiss = super.parseUserAgent)

    override def parseDevice(agentString: String): Device =
      lookupOrEvaluate(key = agentString, cache = cache.device, onMiss = super.parseDevice)

    override def parseOS(agentString: String): OS =
      lookupOrEvaluate(key = agentString, cache = cache.os, onMiss = super.parseOS)

    private def lookupOrEvaluate[VALUE >: Null](
      key: String,
      cache: LruMap[Id, String, VALUE],
      onMiss: String => VALUE
    ): VALUE =
      if (key == null) null
      else
        cache.get(key) match {
          case Some(cachedValue) => cachedValue
          case None =>
            val newValue = onMiss(key)
            cache.put(key, newValue)
            newValue

        }

  }

  private final class NoCustomRegexYaml(override val cache: ParserCache) extends Parser() with LruMapCachingParser
  private final class CustomRegexYaml(override val cache: ParserCache, regexYaml: InputStream)
      extends Parser(regexYaml)
      with LruMapCachingParser

  private def createLruMap[VALUE]: LruMap[Id, String, VALUE] =
    CreateLruMap[Id, String, VALUE].create(defaultCacheSize)
}
