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
    // For some Twitter libs and uaParser utils
    "Concurrent Maven Repo"          at "https://conjars.org/repo",
    // For Twitter's util functions
    "Twitter Maven Repo"             at "https://maven.twttr.com/",
    // For legacy Snowplow libs
    ("Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/").withAllowInsecureProtocol(true),
    // For Confluent libs
    ("Confluent Repository"          at "https://packages.confluent.io/maven/")
  )

  object V {
    // Java
    val commonsCodec     = "1.16.0"
    val commonsText      = "1.10.0"
    val commonsLang      = "3.18.0"
    val commonsIO        = "2.14.0"
    val jodaTime         = "2.10.1"
    val useragent        = "1.21"
    val uaParser         = "1.5.4"
    val snakeYaml        = "2.3"
    val postgresDriver   = "42.7.2"
    val mysqlClient      = "3.4.0"
    val hikariCP         = "5.0.1"
    val jaywayJsonpath   = "2.7.0"
    val jsonsmart        = "2.5.2"
    val iabClient        = "0.2.0"
    val ipaddress        = "5.5.1"
    val yauaa            = "7.30.0"
    val log4jToSlf4j     = "2.18.0"
    val guava            = "33.1.0-jre"
    val slf4j            = "2.0.3"
    val thrift           = "0.15.0"
    val httpclient       = "4.5.13"
    val sprayJson        = "1.3.6"
    val netty            = "4.1.125.Final"
    val protobuf         = "4.28.3"
    val nashorn          = "15.6"
    val zstd             = "1.5.7-4"

    val refererParser    = "2.1.0"
    val maxmindIplookups = "0.8.1"
    val circe            = "0.14.3"
    val circeOptics      = "0.14.1"
    val circeJackson     = "0.14.0"
    val scalaForex       = "3.0.0"
    val scalaWeather     = "2.0.0"
    val gatlingJsonpath  = "0.6.14"
    val scalaUri         = "1.5.1"
    val badRows          = "2.3.0"
    val igluClient       = "4.0.2"

    val snowplowRawEvent = "0.1.0"
    val collectorPayload = "0.0.0"
    val schemaSniffer    = "0.0.0"

    val gcpSdk           = "2.45.0"
    val awsSdk           = "2.33.1"
    val kafka            = "3.9.1"
    val jackson          = "2.18.1"

    val decline          = "2.4.1"
    val fs2              = "3.10.2"
    val catsEffect       = "3.5.4"
    val fs2BlobStorage   = "0.9.15"
    val azureIdentity    = "1.12.2"
    val nimbusJoseJwt    = "10.0.2"
    val http4s           = "0.23.25"

    val streams          = "0.14.0"

    val specs2            = "4.20.3"
    val specs2Cats        = "4.20.3"
    val specs2CE          = "1.5.0"
    val testcontainers    = "0.40.10"
    val dockerJava        = "3.3.6"
    val parserCombinators = "2.1.1"
    val sentry            = "7.16.0"

    val betterMonadicFor = "0.3.1"
  }

  object Libraries {
    val commonsCodec     = "commons-codec"              %  "commons-codec"                 % V.commonsCodec
    val commonsText      = "org.apache.commons"         %  "commons-text"                  % V.commonsText
    val commonsLang      = "org.apache.commons"         %  "commons-lang3"                 % V.commonsLang
    val commonsIO        = "commons-io"                 %  "commons-io"                    % V.commonsIO
    val jodaTime         = "joda-time"                  %  "joda-time"                     % V.jodaTime
    val useragent        = "eu.bitwalker"               %  "UserAgentUtils"                % V.useragent
    val jacksonDatabind  = "com.fasterxml.jackson.core" %  "jackson-databind"              % V.jackson
    val snakeYaml        = "org.yaml"                   %  "snakeyaml"                     % V.snakeYaml
    val uaParser         = "com.github.ua-parser"       %  "uap-java"                      % V.uaParser
    val postgresDriver   = "org.postgresql"             %  "postgresql"                    % V.postgresDriver
    val mysqlClient      = "org.mariadb.jdbc"           %  "mariadb-java-client"           % V.mysqlClient
    val hikariCP         = ("com.zaxxer"                %  "HikariCP"                      % V.hikariCP)
                             .exclude("org.slf4j", "slf4j-api")
    val jaywayJsonpath   = "com.jayway.jsonpath"        %  "json-path"                     % V.jaywayJsonpath
    val jsonsmart        = "net.minidev"                %  "json-smart"                    % V.jsonsmart
    val yauaa            = "nl.basjes.parse.useragent"  %  "yauaa"                         % V.yauaa
    val log4jToSlf4j     = "org.apache.logging.log4j"   % "log4j-to-slf4j"                 % V.log4jToSlf4j
    val guava            = "com.google.guava"           %  "guava"                         % V.guava
    val nashorn          = "org.openjdk.nashorn"        % "nashorn-core"                   % V.nashorn
    val zstd             = "com.github.luben"           % "zstd-jni"                       % V.zstd

    val circeGeneric     = "io.circe"                   %% "circe-generic"                 % V.circe
    val circeLiteral     = "io.circe"                   %% "circe-literal"                 % V.circe
    val circeOptics      = "io.circe"                   %% "circe-optics"                  % V.circeOptics
    val circeJackson     = "io.circe"                   %% "circe-jackson210"              % V.circeJackson
    val scalaUri         = "io.lemonlabs"               %% "scala-uri"                     % V.scalaUri
    val gatlingJsonpath  = "io.gatling"                 %% "jsonpath"                      % V.gatlingJsonpath
    val scalaForex       = "com.snowplowanalytics"      %% "scala-forex"                   % V.scalaForex
    val refererParser    = "com.snowplowanalytics"      %% "scala-referer-parser"          % V.refererParser
    val maxmindIplookups = "com.snowplowanalytics"      %% "scala-maxmind-iplookups"       % V.maxmindIplookups
    val scalaWeather     = "com.snowplowanalytics"      %% "scala-weather"                 % V.scalaWeather
    val badRows          = "com.snowplowanalytics"      %% "snowplow-badrows"              % V.badRows
    val igluClient       = "com.snowplowanalytics"      %% "iglu-scala-client"             % V.igluClient
    val igluClientHttp4s = "com.snowplowanalytics"      %% "iglu-scala-client-http4s"      % V.igluClient
    val snowplowRawEvent = "com.snowplowanalytics"      %  "snowplow-thrift-raw-event"     % V.snowplowRawEvent
    val collectorPayload = "com.snowplowanalytics"      %  "collector-payload-1"           % V.collectorPayload
    val schemaSniffer    = "com.snowplowanalytics"      %  "schema-sniffer-1"              % V.schemaSniffer
    val iabClient        = "com.snowplowanalytics"      %  "iab-spiders-and-robots-client" % V.iabClient
    val ipaddress        = "com.github.seancfoley"      %  "ipaddress"                     % V.ipaddress
    val thrift           = "org.apache.thrift"          %  "libthrift"                     % V.thrift
    val httpclient       = "org.apache.httpcomponents"  %  "httpclient"                    % V.httpclient
    val sprayJson        = "io.spray"                   %% "spray-json"                    % V.sprayJson
    val nettyAll         = "io.netty"                   %  "netty-all"                     % V.netty
    val nettyCodec       = "io.netty"                   %  "netty-codec"                   % V.netty
    val slf4j            = "org.slf4j"                  %  "slf4j-simple"                  % V.slf4j
    val sentry           = "io.sentry"                  %  "sentry"                        % V.sentry
    val protobuf         = "com.google.protobuf"        %  "protobuf-java"                 % V.protobuf

    val streams         = "com.snowplowanalytics" %% "streams-core"   % V.streams
    val kinesisSnowplow = "com.snowplowanalytics" %% "kinesis"        % V.streams
    val pubsubSnowplow  = "com.snowplowanalytics" %% "pubsub"         % V.streams
    val kafkaSnowplow   = "com.snowplowanalytics" %% "kafka"          % V.streams
    val nsqSnowplow     = "com.snowplowanalytics" %% "nsq"            % V.streams
    val runtime         = "com.snowplowanalytics" %% "runtime-common" % V.streams

    val specs2             = "org.specs2"             %% "specs2-core"                   % V.specs2            % Test
    val specs2Cats         = "org.specs2"             %% "specs2-cats"                   % V.specs2Cats        % Test
    val specs2Scalacheck   = "org.specs2"             %% "specs2-scalacheck"             % V.specs2            % Test
    val specs2Mock         = "org.specs2"             %% "specs2-mock"                   % V.specs2            % Test
    val specs2CE           = "org.typelevel"          %% "cats-effect-testing-specs2"    % V.specs2CE          % Test
    val catsEffectTestkit  = "org.typelevel"          %% "cats-effect-testkit"           % V.catsEffect        % Test
    val parserCombinators  = "org.scala-lang.modules" %% "scala-parser-combinators"      % V.parserCombinators % Test
    val testContainers     = "com.dimafeng"           %% "testcontainers-scala-core"     % V.testcontainers    % Test
    val dockerJava         = "com.github.docker-java" %  "docker-java"                   % V.dockerJava        % Test

    val gcs              = "com.google.cloud"                 %  "google-cloud-storage"              % V.gcpSdk
    val kafkaClients     = "org.apache.kafka"                 %  "kafka-clients"                     % V.kafka

    val declineEffect    = "com.monovore"                     %% "decline-effect"                        % V.decline
    val fs2              = "co.fs2"                           %% "fs2-core"                              % V.fs2
    val s3               = "software.amazon.awssdk"           %  "s3"                                    % V.awsSdk
    val sts              = "software.amazon.awssdk"           %  "sts"                                   % V.awsSdk         % Runtime
    val azureIdentity    = "com.azure"                        % "azure-identity"                         % V.azureIdentity
    val nimbusJoseJwt    = "com.nimbusds"                     % "nimbus-jose-jwt"                        % V.nimbusJoseJwt
    val jacksonDfXml     = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml"                 % V.jackson
    val http4sClient     = "org.http4s"                       %% "http4s-ember-client"                   % V.http4s
    val fs2BlobS3        = "com.github.fs2-blobstore"         %% "s3"                                    % V.fs2BlobStorage
    val fs2BlobGcs       = "com.github.fs2-blobstore"         %% "gcs"                                   % V.fs2BlobStorage
    val fs2BlobAzure     = "com.github.fs2-blobstore"         %% "azure"                                 % V.fs2BlobStorage
    val http4sDsl        = "org.http4s"                       %% "http4s-dsl"                            % V.http4s          % Test

    // compiler plugins
    val betterMonadicFor = "com.olegpy" %% "better-monadic-for" % V.betterMonadicFor

    val commonDependencies = Seq(
      jodaTime,
      commonsCodec,
      commonsText,
      commonsLang, // for security vulnerabilities
      commonsIO, // for security vulnerabilities
      useragent,
      jacksonDatabind,
      uaParser,
      snakeYaml,
      postgresDriver,
      mysqlClient,
      hikariCP,
      jaywayJsonpath,
      jsonsmart,
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
      snowplowRawEvent,
      http4sClient,
      collectorPayload,
      schemaSniffer,
      thrift,
      sprayJson,
      nettyAll,
      nettyCodec,
      protobuf,
      specs2,
      specs2Cats,
      specs2Scalacheck,
      specs2Mock,
      specs2CE,
      circeLiteral % Test,
      parserCombinators,
      nashorn
    )

    val coreDependencies = Seq(
      circeLiteral,
      declineEffect,
      igluClientHttp4s,
      runtime,
      sentry,
      streams,
      slf4j,
      zstd,
      // Tests
      specs2,
      catsEffectTestkit,
      http4sDsl
    )

    val cloudUtilsDependencies = Seq(
      fs2,
      http4sClient
    )

    val awsUtilsDependencies = Seq(
      fs2BlobS3,
      s3
    )

    val gcpUtilsDependencies = Seq(
      fs2BlobGcs,
      gcs
    )

    val azureUtilsDependencies = Seq(
      fs2BlobAzure,
      azureIdentity,
      circeGeneric,
      jacksonDfXml, // for security vulnerabilities
      nimbusJoseJwt // for security vulnerabilities
    )

    val pubsubDependencies = Seq(
      pubsubSnowplow
    )

    val kinesisDependencies = Seq(
      kinesisSnowplow,
      sts
    )

    val kafkaDependencies = Seq(
      kafkaSnowplow,
      kafkaClients, // for security vulnerabilities
      httpclient // for security vulnerabilities
    )

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
