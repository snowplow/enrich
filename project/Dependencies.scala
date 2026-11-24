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
// =======================================================
// scalafmt: {align.tokens = ["%", "%%"]}
// =======================================================
import sbt._

object Dependencies {

  val resolutionRepos = Seq(
    // For Twitter's util functions
    "Twitter Maven Repo" at "https://maven.twttr.com/",
    // For Confluent libs
    ("Confluent Repository" at "https://packages.confluent.io/maven/")
  )

  object V {
    // Snowplow libraries
    val refererParser = "4.0.0"
    val maxmindIplookups = "0.9.1"
    val scalaForex = "3.0.0"
    val scalaWeather = "2.0.0"
    val iabClient = "0.3.1"
    val badRows = "2.3.0"
    val igluClient = "4.3.1"
    val streams = "0.29.0"

    // Scala and FP core
    val catsEffect = "3.7.0"
    val fs2 = "3.12.2"
    val decline = "2.4.1"
    val circe = "0.14.3"
    val circeOptics = "0.14.1"
    val circeJackson = "0.14.0"
    val http4s = "0.23.35"
    val scalaUri = "1.5.1"
    val sprayJson = "1.3.6"

    // Libraries the enrichments build on
    val jodaTime = "2.10.1"
    val useragent = "1.21"
    val uaParser = "1.5.4"
    val yauaa = "8.1.1"
    val snakeYaml = "2.3"
    val jaywayJsonpath = "2.10.0"
    val gatlingJsonpath = "3.15.1"
    val ipaddress = "5.5.1"
    val guava = "33.7.2-jre"
    val nashorn = "15.6"
    val jackson = "2.22.4"
    val httpclient = "4.5.13"

    // SQL enrichment
    val postgresDriver = "42.7.14"
    val mysqlClient = "3.4.4"
    val hikariCP = "5.0.1"

    // Collector payloads
    val thrift = "0.24.0"
    val javaxAnnotation = "1.3.2"

    // Cloud SDKs
    val gcpSdk = "2.69.0"
    val awsSdk = "2.33.1"
    val azureIdentity = "1.18.7"
    val azureStorageBlob = "12.33.0"

    // Compression
    val zstd = "1.5.7-4"

    // Logging and error reporting
    val slf4j = "2.0.3"
    val log4jToSlf4j = "2.25.5"
    val sentry = "7.16.0"

    // Versions held above what something upstream asks for, to keep a vulnerable copy out of the
    // images. See the *SecurityOverrides Seqs below for where each one applies.
    val netty = "4.2.17.Final"
    val kafka = "4.3.2"

    // Test only
    val specs2 = "4.20.3"
    val specs2Cats = "4.20.3"
    val specs2CE = "1.5.0"
    val testcontainers = "0.40.10"
    val dockerJava = "3.3.6"

    // Compiler plugins
    val betterMonadicFor = "0.3.1"
  }

  object Libraries {
    // Snowplow libraries
    val refererParser = "com.snowplowanalytics"    %% "scala-referer-parser"          % V.refererParser
    val maxmindIplookups = "com.snowplowanalytics" %% "scala-maxmind-iplookups"       % V.maxmindIplookups
    val scalaForex = "com.snowplowanalytics"       %% "scala-forex"                   % V.scalaForex
    val scalaWeather = "com.snowplowanalytics"     %% "scala-weather"                 % V.scalaWeather
    val iabClient = "com.snowplowanalytics"         % "iab-spiders-and-robots-client" % V.iabClient
    val badRows = "com.snowplowanalytics"          %% "snowplow-badrows"              % V.badRows
    val igluClient = "com.snowplowanalytics"       %% "iglu-scala-client"             % V.igluClient
    val igluClientHttp4s = "com.snowplowanalytics" %% "iglu-scala-client-http4s"      % V.igluClient
    val streams = "com.snowplowanalytics"          %% "streams-core"                  % V.streams
    val kinesisSnowplow = "com.snowplowanalytics"  %% "kinesis"                       % V.streams
    val pubsubSnowplow = "com.snowplowanalytics"   %% "pubsub"                        % V.streams
    val kafkaSnowplow = ("com.snowplowanalytics"   %% "kafka"                         % V.streams)
      .exclude("org.lz4", "lz4-java") // abandoned in favour of the at.yawk.lz4 fork
    val nsqSnowplow = "com.snowplowanalytics" %% "nsq"            % V.streams
    val runtime = "com.snowplowanalytics"     %% "runtime-common" % V.streams

    // Scala and FP core
    val catsEffect = "org.typelevel"   %% "cats-effect"         % V.catsEffect
    val fs2 = "co.fs2"                 %% "fs2-core"            % V.fs2
    val declineEffect = "com.monovore" %% "decline-effect"      % V.decline
    val circeGeneric = "io.circe"      %% "circe-generic"       % V.circe
    val circeLiteral = "io.circe"      %% "circe-literal"       % V.circe
    val circeOptics = "io.circe"       %% "circe-optics"        % V.circeOptics
    val circeJackson = "io.circe"      %% "circe-jackson210"    % V.circeJackson
    val http4sClient = "org.http4s"    %% "http4s-ember-client" % V.http4s
    val scalaUri = "io.lemonlabs"      %% "scala-uri"           % V.scalaUri
    // scala-uri's public suffix support needs spray-json, which scala-uri marks as optional
    val sprayJson = "io.spray" %% "spray-json" % V.sprayJson

    // Libraries the enrichments build on
    val jodaTime = "joda-time"                 % "joda-time"      % V.jodaTime
    val useragent = "eu.bitwalker"             % "UserAgentUtils" % V.useragent
    val uaParser = "com.github.ua-parser"      % "uap-java"       % V.uaParser
    val yauaa = "nl.basjes.parse.useragent"    % "yauaa"          % V.yauaa
    val snakeYaml = "org.yaml"                 % "snakeyaml"      % V.snakeYaml
    val jaywayJsonpath = "com.jayway.jsonpath" % "json-path"      % V.jaywayJsonpath
    // The standalone `io.gatling.jsonpath % jsonpath` artifact has no Scala 2.13 build. This module
    // of the Gatling framework is the same engine, folded in-tree, which is why a load-testing
    // framework's module backs `JsonPath.compileQuery`.
    val gatlingJsonpath = "io.gatling"                 % "gatling-jsonpath" % V.gatlingJsonpath
    val ipaddress = "com.github.seancfoley"            % "ipaddress"        % V.ipaddress
    val guava = "com.google.guava"                     % "guava"            % V.guava
    val nashorn = "org.openjdk.nashorn"                % "nashorn-core"     % V.nashorn
    val jacksonDatabind = "com.fasterxml.jackson.core" % "jackson-databind" % V.jackson
    val httpclient = "org.apache.httpcomponents"       % "httpclient"       % V.httpclient

    // SQL enrichment
    val postgresDriver = "org.postgresql" % "postgresql"          % V.postgresDriver
    val mysqlClient = "org.mariadb.jdbc"  % "mariadb-java-client" % V.mysqlClient
    val hikariCP = ("com.zaxxer"          % "HikariCP"            % V.hikariCP)
      .exclude("org.slf4j", "slf4j-api")

    // Collector payloads
    // These are only used by libthrift's HTTP and servlet transports. Exclude them here rather than in
    // `exclusions`, because kinesis and kafka need httpclient5 and httpcore5 through the AWS SDK.
    val thrift = ("org.apache.thrift" % "libthrift" % V.thrift)
      .excludeAll(
        ExclusionRule("org.apache.httpcomponents.client5"),
        ExclusionRule("org.apache.httpcomponents.core5"),
        ExclusionRule("jakarta.servlet"),
        ExclusionRule("jakarta.annotation")
      )
    // The Thrift-generated CollectorPayload is annotated with javax.annotation.Generated
    val javaxAnnotation = "javax.annotation" % "javax.annotation-api" % V.javaxAnnotation

    // Cloud SDKs
    val gcs = "com.google.cloud"       % "google-cloud-storage" % V.gcpSdk
    val s3 = "software.amazon.awssdk"  % "s3"                   % V.awsSdk
    val sts = "software.amazon.awssdk" % "sts"                  % V.awsSdk % Runtime
    val azureIdentity = "com.azure"    % "azure-identity"       % V.azureIdentity
    val azureStorageBlob = "com.azure" % "azure-storage-blob"   % V.azureStorageBlob

    // Compression
    val zstd = "com.github.luben" % "zstd-jni" % V.zstd

    // Logging and error reporting
    val slf4j = "org.slf4j" % "slf4j-simple" % V.slf4j
    // yauaa logs through log4j-api but leaves it to the application to provide one; route it to slf4j
    val log4jToSlf4j = "org.apache.logging.log4j" % "log4j-to-slf4j" % V.log4jToSlf4j
    val sentry = "io.sentry"                      % "sentry"         % V.sentry

    // Artifacts named only to force a version up; grouped into the *SecurityOverrides Seqs below
    val nettyAll = "io.netty"              % "netty-all"     % V.netty
    val kafkaClients = ("org.apache.kafka" % "kafka-clients" % V.kafka)
      .exclude("org.lz4", "lz4-java") // abandoned in favour of the at.yawk.lz4 fork

    // Test only
    val specs2 = "org.specs2"                %% "specs2-core"                % V.specs2         % Test
    val specs2Cats = "org.specs2"            %% "specs2-cats"                % V.specs2Cats     % Test
    val specs2Scalacheck = "org.specs2"      %% "specs2-scalacheck"          % V.specs2         % Test
    val specs2Mock = "org.specs2"            %% "specs2-mock"                % V.specs2         % Test
    val specs2CE = "org.typelevel"           %% "cats-effect-testing-specs2" % V.specs2CE       % Test
    val catsEffectTestkit = "org.typelevel"  %% "cats-effect-testkit"        % V.catsEffect     % Test
    val http4sDsl = "org.http4s"             %% "http4s-dsl"                 % V.http4s         % Test
    val testContainers = "com.dimafeng"      %% "testcontainers-scala-core"  % V.testcontainers % Test
    val dockerJava = "com.github.docker-java" % "docker-java"                % V.dockerJava     % Test

    // Compiler plugins
    val betterMonadicFor = "com.olegpy" %% "better-monadic-for" % V.betterMonadicFor

    // Security overrides, grouped by the module they apply to. An entry is needed only while it
    // still evicts something: once the upstream catches up, delete it. To check, drop the line and
    // compare the resolved runtime classpath before and after; reading the dependency tree for
    // eviction markers is unreliable, because sbt truncates long lines.

    // Nothing in common uses Netty: netty-all sits here so that every app inherits it. It lifts the
    // Netty modules that arrive on an older line via the AWS SDK's netty-nio-client (kinesis, and
    // kafka and nsq through aws-utils) and the Azure SDK's reactor-netty (kafka and nsq).
    val commonSecurityOverrides = Seq(nettyAll)

    // fs2-kafka, via common-streams' kafka module, trails kafka-clients' fixes. Naming kafka-clients
    // pulls snappy-java up alongside it.
    val kafkaSecurityOverrides = Seq(kafkaClients)

    val commonDependencies = Seq(
      jodaTime,
      useragent,
      jacksonDatabind,
      uaParser,
      snakeYaml,
      postgresDriver,
      mysqlClient,
      hikariCP,
      jaywayJsonpath,
      iabClient,
      ipaddress,
      yauaa,
      log4jToSlf4j,
      guava,
      circeOptics,
      circeJackson,
      refererParser,
      maxmindIplookups,
      scalaUri,
      scalaForex,
      scalaWeather,
      gatlingJsonpath,
      badRows,
      igluClient,
      http4sClient,
      httpclient,
      thrift,
      javaxAnnotation,
      sprayJson,
      specs2,
      specs2Cats,
      specs2Scalacheck,
      specs2Mock,
      specs2CE,
      circeLiteral % Test,
      nashorn
    ) ++ commonSecurityOverrides

    val coreDependencies = Seq(
      catsEffect,
      circeLiteral,
      declineEffect,
      igluClientHttp4s,
      runtime,
      sentry,
      streams,
      slf4j,
      zstd,
      specs2,
      catsEffectTestkit,
      http4sDsl
    )

    val cloudUtilsDependencies = Seq(
      fs2,
      http4sClient,
      specs2CE,
      specs2,
      http4sDsl
    )

    val awsUtilsDependencies = Seq(
      s3
    )

    val gcpUtilsDependencies = Seq(
      gcs
    )

    val azureUtilsDependencies = Seq(
      azureIdentity,
      azureStorageBlob,
      circeGeneric
    )

    val pubsubDependencies = Seq(
      pubsubSnowplow
    )

    val kinesisDependencies = Seq(
      kinesisSnowplow,
      sts
    )

    val kafkaDependencies = Seq(
      kafkaSnowplow
    ) ++ kafkaSecurityOverrides

    val nsqDependencies = Seq(
      nsqSnowplow
    )

    val itDependencies = Seq(
      specs2CE,
      testContainers,
      dockerJava
    )

    // exclusions
    val exclusions = Seq(
      "org.apache.tomcat.embed" % "tomcat-embed-core"
    )
  }
}
