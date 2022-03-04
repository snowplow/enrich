/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
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
    val commonsCodec     = "1.15"
    val jodaTime         = "2.10.1"
    val useragent        = "1.21"
    val uaParser         = "1.4.3"
    val postgresDriver   = "42.2.25"
    val mysqlConnector   = "8.0.16"
    val jaywayJsonpath   = "2.7.0"
    val iabClient        = "0.2.0"
    val yauaa            = "5.23"
    val guava            = "28.1-jre"
    val slf4j            = "1.7.32"
    val log4j            = "2.17.0" // CVE-2021-44228
    val thrift           = "0.15.0" // override transitive dependency to mitigate security vulnerabilities
    val sprayJson        = "1.3.6" // override transitive dependency to mitigate security vulnerabilities
    val netty            = "4.1.68.Final" // override transitive dependency to mitigate security vulnerabilities

    val refererParser    = "1.1.0"
    val maxmindIplookups = "0.7.1"
    val circe            = "0.14.1"
    val circeOptics      = "0.14.1"
    val circeConfig      = "0.7.0"
    val circeJackson     = "0.14.0"
    val scalaForex       = "1.0.0"
    val scalaWeather     = "1.0.0"
    val gatlingJsonpath  = "0.6.14"
    val scalaUri         = "1.5.1"
    val badRows          = "2.1.1"
    val igluClient       = "1.0.2"

    val snowplowRawEvent = "0.1.0"
    val collectorPayload = "0.0.0"
    val schemaSniffer    = "0.0.0"

    val awsSdk           = "1.12.129"
    val gcpSdk           = "1.118.1"
    val kinesisClient    = "1.14.5"
    val kafka            = "2.2.1"
    val nsqClient        = "1.3.0"
    val jackson          = "2.11.4"
    val config           = "1.3.4"
    val gson             = "2.9.0" // override transitive dependency of gcpSdk

    val decline          = "1.0.0"
    val fs2              = "2.5.5"
    val catsEffect       = "2.5.0"
    val fs2PubSub        = "0.18.1"
    val fs2Aws           = "3.1.1"
    val fs2BlobStorage   = "0.7.3"
    val http4s           = "0.21.22"
    val log4cats         = "1.3.0"
    val catsRetry        = "2.1.0"

    val scopt            = "3.7.1"
    val pureconfig       = "0.11.0"
    val pureconfig013    = "0.13.0"
    val snowplowTracker  = "1.0.0"

    val specs2           = "4.5.1"
    val specs2CE         = "0.4.1"
    val scalacheck       = "1.14.0"
    val jinJava          = "2.5.0"

    val sentry           = "1.7.30"
    val scio             = "0.11.1"
    val beam             = "2.33.0"
    val grpc             = "1.32.2"
    val macros           = "2.1.1"
    val scalaTest        = "3.0.8"
  }

  object Libraries {
    val commonsCodec     = "commons-codec"              %  "commons-codec"                 % V.commonsCodec
    val jodaTime         = "joda-time"                  %  "joda-time"                     % V.jodaTime
    val useragent        = "eu.bitwalker"               %  "UserAgentUtils"                % V.useragent
    val jacksonDatabind  = "com.fasterxml.jackson.core" %  "jackson-databind"              % V.jackson
    val uaParser         = "com.github.ua-parser"       %  "uap-java"                      % V.uaParser
    val postgresDriver   = "org.postgresql"             %  "postgresql"                    % V.postgresDriver
    val mysqlConnector   = "mysql"                      %  "mysql-connector-java"          % V.mysqlConnector
    val jaywayJsonpath   = "com.jayway.jsonpath"        %  "json-path"                     % V.jaywayJsonpath
    val yauaa            = "nl.basjes.parse.useragent"  %  "yauaa"                         % V.yauaa
    val guava            = "com.google.guava"           %  "guava"                         % V.guava
    val log4j            = "org.apache.logging.log4j"   % "log4j-core"                     % V.log4j
    val log4jApi         = "org.apache.logging.log4j"   % "log4j-api"                      % V.log4j

    val circeCore        = "io.circe"                   %% "circe-core"                    % V.circe
    val circeGeneric     = "io.circe"                   %% "circe-generic"                 % V.circe
    val circeExtras      = "io.circe"                   %% "circe-generic-extras"          % V.circe
    val circeParser      = "io.circe"                   %% "circe-parser"                  % V.circe
    val circeLiteral     = "io.circe"                   %% "circe-literal"                 % V.circe
    val circeJava8       = "io.circe"                   %% "circe-java8"                   % V.circe
    val circeJawn        = "io.circe"                   %% "circe-jawn"                    % V.circe
    val circeConfig      = "io.circe"                   %% "circe-config"                  % V.circeConfig
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
    val snowplowRawEvent = "com.snowplowanalytics"      %  "snowplow-thrift-raw-event"     % V.snowplowRawEvent
    val collectorPayload = "com.snowplowanalytics"      %  "collector-payload-1"           % V.collectorPayload
    val schemaSniffer    = "com.snowplowanalytics"      %  "schema-sniffer-1"              % V.schemaSniffer
    val iabClient        = "com.snowplowanalytics"      %  "iab-spiders-and-robots-client" % V.iabClient
    val thrift           = "org.apache.thrift"          %  "libthrift"                     % V.thrift
    val sprayJson        = "io.spray"                   %% "spray-json"                    % V.sprayJson
    val nettyAll         = "io.netty"                   % "netty-all"                      % V.netty
    val nettyCodec       = "io.netty"                   % "netty-codec"                    % V.netty

    val specs2           = "org.specs2"                 %% "specs2-core"                   % V.specs2                    % Test
    val specs2Cats       = "org.specs2"                 %% "specs2-cats"                   % V.specs2                    % Test
    val specs2Scalacheck = "org.specs2"                 %% "specs2-scalacheck"             % V.specs2                    % Test
    val specs2Mock       = "org.specs2"                 %% "specs2-mock"                   % V.specs2                    % Test
    val specs2CE         = "com.codecommit"             %% "cats-effect-testing-specs2"    % V.specs2CE                  % Test

    // Beam
    val sentry           = "io.sentry"                  %  "sentry"                                   % V.sentry
    val scioCore         = "com.spotify"                %% "scio-core"                                % V.scio
    val scioGCP          = "com.spotify"                %% "scio-google-cloud-platform"               % V.scio
    val beam             = "org.apache.beam"            % "beam-runners-google-cloud-dataflow-java"   % V.beam
    val grpc             = "io.grpc"                    % "grpc-bom"                                  % V.grpc pomOnly()
    val slf4j            = "org.slf4j"                  % "slf4j-simple"                              % V.slf4j
    val scioTest         = "com.spotify"                %% "scio-test"                                % V.scio            % Test
    val scalaTest        = "org.scalatest"              %% "scalatest"                                % V.scalaTest       % Test

    // Stream
    val kinesisSdk       = "com.amazonaws"                    %  "aws-java-sdk-kinesis"              % V.awsSdk
    val dynamodbSdk      = "com.amazonaws"                    %  "aws-java-sdk-dynamodb"             % V.awsSdk
    val s3Sdk            = "com.amazonaws"                    %  "aws-java-sdk-s3"                   % V.awsSdk
    val kinesisClient    = "com.amazonaws"                    %  "amazon-kinesis-client"             % V.kinesisClient
    val sts              = "com.amazonaws"                    %  "aws-java-sdk-sts"                  % V.awsSdk           % Runtime
    val gcs              = "com.google.cloud"                 %  "google-cloud-storage"              % V.gcpSdk
    val gson             = "com.google.code.gson"             %  "gson"                              % V.gson
    val kafkaClients     = "org.apache.kafka"                 %  "kafka-clients"                     % V.kafka
    val jacksonCbor      = "com.fasterxml.jackson.dataformat" %  "jackson-dataformat-cbor"           % V.jackson
    val config           = "com.typesafe"                     %  "config"                            % V.config
    val log4jOverSlf4j   = "org.slf4j"                        %  "log4j-over-slf4j"                  % V.slf4j
    val scopt            = "com.github.scopt"                 %% "scopt"                             % V.scopt
    val pureconfig       = "com.github.pureconfig"            %% "pureconfig"                        % V.pureconfig
    val nsqClient        = "com.snowplowanalytics"            %  "nsq-java-client"                   % V.nsqClient
    val catsEffect       = "org.typelevel"                    %% "cats-effect"                       % V.catsEffect
    val snowplowTracker  = "com.snowplowanalytics"            %% "snowplow-scala-tracker-emitter-id" % V.snowplowTracker
    val scalacheck       = "org.scalacheck"                   %% "scalacheck"                        % V.scalacheck      % Test
    val kafka            = "org.apache.kafka"                 %% "kafka"                             % V.kafka           % Test
    val jinJava          = "com.hubspot.jinjava"              %  "jinjava"                           % V.jinJava         % Test

    // FS2
    val decline          = "com.monovore"                     %% "decline"                           % V.decline
    val fs2PubSub        = "com.permutive"                    %% "fs2-google-pubsub-grpc"            % V.fs2PubSub
    val fs2Aws           = "io.laserdisc"                     %% "fs2-aws"                           % V.fs2Aws
    val fs2              = "co.fs2"                           %% "fs2-core"                          % V.fs2
    val fs2Io            = "co.fs2"                           %% "fs2-io"                            % V.fs2
    val http4sClient     = "org.http4s"                       %% "http4s-blaze-client"               % V.http4s
    val http4sCirce      = "org.http4s"                       %% "http4s-circe"                      % V.http4s
    val log4cats         = "org.typelevel"                    %% "log4cats-slf4j"                    % V.log4cats
    val catsRetry        = "com.github.cb372"                 %% "cats-retry"                        % V.catsRetry
    val fs2BlobS3        = "com.github.fs2-blobstore"         %% "s3"                                % V.fs2BlobStorage
    val fs2BlobGcs       = "com.github.fs2-blobstore"         %% "gcs"                               % V.fs2BlobStorage
    val pureconfigCats   = "com.github.pureconfig"            %% "pureconfig-cats-effect"            % V.pureconfig
    val pureconfigCirce  = "com.github.pureconfig"            %% "pureconfig-circe"                  % V.pureconfig
    val http4sDsl        = "org.http4s"                       %% "http4s-dsl"                        % V.http4s          % Test
    val http4sServer     = "org.http4s"                       %% "http4s-blaze-server"               % V.http4s          % Test

    // exclusions
    val exclusions = Seq(
      "org.apache.tomcat.embed" % "tomcat-embed-core"
    )
  }
}
