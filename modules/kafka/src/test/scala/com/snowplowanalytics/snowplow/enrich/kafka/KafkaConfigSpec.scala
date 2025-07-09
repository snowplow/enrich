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
package com.snowplowanalytics.snowplow.enrich.kafka

import java.nio.file.Paths
import java.util.UUID

import scala.concurrent.duration.DurationInt

import org.specs2.Specification

import cats.Id
import cats.implicits._
import cats.effect.{ExitCode, IO}

import cats.effect.testing.specs2.CatsEffect

import org.http4s.Uri

import com.comcast.ip4s.Port

import com.snowplowanalytics.snowplow.runtime.Metrics.StatsdConfig
import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, ConfigParser, Retrying, Telemetry}

import com.snowplowanalytics.snowplow.streams.kafka.{KafkaSinkConfig, KafkaSinkConfigM, KafkaSourceConfig}

import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers.{adaptersSchemas, atomicFieldLimitsDefaults}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.AtomicFields
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

import com.snowplowanalytics.snowplow.enrich.cloudutils.azure.AzureStorageConfig

import com.snowplowanalytics.snowplow.enrich.core.Config

class KafkaConfigSpec extends Specification with CatsEffect {

  def is = s2"""
  Config parse should be able to parse
    minimal kafka config $minimal
    reference kafka config $reference
  """

  private def minimal =
    assert(
      resource = "/config.kafka.minimal.hocon",
      expectedResult = Right(
        KafkaConfigSpec.minimalConfig
      )
    )

  private def reference =
    assert(
      resource = "/config.kafka.reference.hocon",
      expectedResult = Right(
        KafkaConfigSpec.referenceConfig
      )
    )

  private def assert(
    resource: String,
    expectedResult: Either[ExitCode, Config[EmptyConfig, KafkaSourceConfig, KafkaSinkConfig, AzureStorageConfig]]
  ) = {
    val path = Paths.get(getClass.getResource(resource).toURI)
    ConfigParser.configFromFile[IO, Config[EmptyConfig, KafkaSourceConfig, KafkaSinkConfig, AzureStorageConfig]](path).value.map { result =>
      result must beEqualTo(expectedResult)
    }
  }

}

object KafkaConfigSpec {
  private val minimalConfig = Config[EmptyConfig, KafkaSourceConfig, KafkaSinkConfig, AzureStorageConfig](
    license = AcceptedLicense(),
    input = KafkaSourceConfig(
      topicName = "snowplow-collector-payloads",
      bootstrapServers = "localhost:9092",
      debounceCommitOffsets = 10.seconds,
      consumerConf = Map(
        "group.id" -> "enrich-kafka",
        "allow.auto.create.topics" -> "false",
        "auto.offset.reset" -> "latest",
        "security.protocol" -> "SASL_SSL",
        "sasl.mechanism" -> "OAUTHBEARER",
        "sasl.jaas.config" -> "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
      )
    ),
    output = Config.Output(
      good = Config.SinkWithMetadata(
        sink = KafkaSinkConfigM[Id](
          topicName = "snowplow-enriched",
          bootstrapServers = "localhost:9092",
          producerConf = Map(
            "client.id" -> "enrich-kafka",
            "security.protocol" -> "SASL_SSL",
            "sasl.mechanism" -> "OAUTHBEARER",
            "sasl.jaas.config" -> "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
          )
        ),
        maxRecordSize = 1000000,
        partitionKey = None,
        attributes = Nil
      ),
      failed = None,
      bad = Config.SinkWithMetadata(
        sink = KafkaSinkConfigM[Id](
          topicName = "snowplow-bad",
          bootstrapServers = "localhost:9092",
          producerConf = Map(
            "client.id" -> "enrich-kafka",
            "security.protocol" -> "SASL_SSL",
            "sasl.mechanism" -> "OAUTHBEARER",
            "sasl.jaas.config" -> "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
          )
        ),
        maxRecordSize = 1000000,
        partitionKey = None,
        attributes = Nil
      )
    ),
    streams = EmptyConfig(),
    cpuParallelismFraction = BigDecimal(1),
    sinkParallelismFraction = BigDecimal(2),
    monitoring = Config.Monitoring(
      metrics = Config.Metrics(None),
      sentry = None,
      healthProbe = Config.HealthProbe(port = Port.fromInt(8000).get, unhealthyLatency = 2.minutes)
    ),
    assetsUpdatePeriod = 7.days,
    validation = Config.Validation(
      acceptInvalid = false,
      atomicFieldsLimits = AtomicFields.from(atomicFieldLimitsDefaults),
      maxJsonDepth = 50,
      exitOnJsCompileError = true
    ),
    telemetry = Telemetry.Config(
      disable = false,
      interval = 15.minutes,
      collectorUri = Uri.unsafeFromString("https://collector-g.snowplowanalytics.com"),
      userProvidedId = None,
      autoGeneratedId = None,
      instanceId = None,
      moduleName = None,
      moduleVersion = None
    ),
    metadata = None,
    identity = None,
    blobClients = AzureStorageConfig(Nil),
    adaptersSchemas = adaptersSchemas
  )

  private val referenceConfig = Config[EmptyConfig, KafkaSourceConfig, KafkaSinkConfig, AzureStorageConfig](
    license = AcceptedLicense(),
    input = KafkaSourceConfig(
      topicName = "snowplow-collector-payloads",
      bootstrapServers = "localhost:9092",
      debounceCommitOffsets = 10.seconds,
      consumerConf = Map(
        "group.id" -> "enrich-kafka",
        "enable.auto.commit" -> "false",
        "allow.auto.create.topics" -> "false",
        "auto.offset.reset" -> "earliest",
        "security.protocol" -> "SASL_SSL",
        "sasl.mechanism" -> "OAUTHBEARER",
        "sasl.jaas.config" -> "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
      )
    ),
    output = Config.Output(
      good = Config.SinkWithMetadata(
        sink = KafkaSinkConfigM[Id](
          topicName = "snowplow-enriched",
          bootstrapServers = "localhost:9092",
          producerConf = Map(
            "client.id" -> "enrich-kafka",
            "security.protocol" -> "SASL_SSL",
            "sasl.mechanism" -> "OAUTHBEARER",
            "sasl.jaas.config" -> "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
          )
        ),
        maxRecordSize = 1000000,
        partitionKey = Some(EnrichedEvent.atomicFields.find(_.getName === "user_id").get),
        attributes = List(EnrichedEvent.atomicFields.find(_.getName === "app_id").get)
      ),
      failed = Some(
        Config.SinkWithMetadata(
          sink = KafkaSinkConfigM[Id](
            topicName = "snowplow-failed",
            bootstrapServers = "localhost:9092",
            producerConf = Map(
              "client.id" -> "enrich-kafka",
              "security.protocol" -> "SASL_SSL",
              "sasl.mechanism" -> "OAUTHBEARER",
              "sasl.jaas.config" -> "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
            )
          ),
          maxRecordSize = 1000000,
          partitionKey = None,
          attributes = Nil
        )
      ),
      bad = Config.SinkWithMetadata(
        sink = KafkaSinkConfigM[Id](
          topicName = "snowplow-bad",
          bootstrapServers = "localhost:9092",
          producerConf = Map(
            "client.id" -> "enrich-kafka",
            "security.protocol" -> "SASL_SSL",
            "sasl.mechanism" -> "OAUTHBEARER",
            "sasl.jaas.config" -> "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
          )
        ),
        maxRecordSize = 1000000,
        partitionKey = None,
        attributes = Nil
      )
    ),
    streams = EmptyConfig(),
    cpuParallelismFraction = BigDecimal(1),
    sinkParallelismFraction = BigDecimal(2),
    monitoring = Config.Monitoring(
      metrics = Config.Metrics(
        statsd = Some(
          StatsdConfig(
            hostname = "127.0.0.1",
            port = 8125,
            tags = Map("env" -> "prod"),
            period = 1.minute,
            prefix = "snowplow.enrich"
          )
        )
      ),
      sentry = Some(Config.SentryM[Id](dsn = "https://public@sentry.example.com/1", tags = Map("myTag" -> "xyz"))),
      healthProbe = Config.HealthProbe(
        port = Port.fromInt(8000).get,
        unhealthyLatency = 2.minutes
      )
    ),
    assetsUpdatePeriod = 7.days,
    validation = Config.Validation(
      acceptInvalid = false,
      atomicFieldsLimits = AtomicFields.from(atomicFieldLimitsDefaults ++ Map("app_id" -> 5, "mkt_clickid" -> 100000)),
      maxJsonDepth = 50,
      exitOnJsCompileError = true
    ),
    telemetry = Telemetry.Config(
      disable = false,
      interval = 15.minutes,
      collectorUri = Uri.unsafeFromString("https://collector-g.snowplowanalytics.com"),
      userProvidedId = Some("my_pipeline"),
      autoGeneratedId = Some("hfy67e5ydhtrd"),
      instanceId = Some("665bhft5u6udjf"),
      moduleName = Some("enrich-kinesis-ce"),
      moduleVersion = Some("1.0.0")
    ),
    metadata = Some(
      Config.MetadataM[Id](
        endpoint = Uri.unsafeFromString("https://my_pipeline.my_domain.com/iglu"),
        interval = 5.minutes,
        organizationId = UUID.fromString("c5f3a09f-75f8-4309-bec5-fea560f78455"),
        pipelineId = UUID.fromString("75a13583-5c99-40e3-81fc-541084dfc784"),
        maxBodySize = 150000
      )
    ),
    identity = Some(
      Config.IdentityM[Id](
        endpoint = Uri.unsafeFromString("http://identity-api"),
        concurrency = 10,
        username = "snowplow",
        password = "sn0wp10w",
        retries = Retrying.Config.ForTransient(100.millis, 3)
      )
    ),
    blobClients = AzureStorageConfig(
      List(
        AzureStorageConfig.Account("storageAccount1", None),
        AzureStorageConfig.Account("storageAccount2", Some(AzureStorageConfig.Account.Auth.DefaultCredentialsChain)),
        AzureStorageConfig.Account("storageAccount3", Some(AzureStorageConfig.Account.Auth.SasToken("tokenValue")))
      )
    ),
    adaptersSchemas = adaptersSchemas
  )
}
