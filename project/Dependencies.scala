/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
    // For modern Snowplow libs
    "Snowplow Bintray"               at "https://snowplow.bintray.com/snowplow-maven/",
    // For legacy Snowplow libs
    ("Snowplow Analytics Maven repo" at "http://maven.snplow.com/releases/").withAllowInsecureProtocol(true)
  )

  object V {
    // Java
    val commonsCodec     = "1.12"
    val jodaTime         = "2.10.1"
    val useragent        = "1.21"
    val uaParser         = "1.4.3"
    val postgresDriver   = "42.2.5"
    val mysqlConnector   = "8.0.16"
    val jaywayJsonpath   = "2.4.0"
    val iabClient        = "0.2.0"
    val yauaa            = "5.8"
    val guava            = "28.1-jre" //used only for InetAddress (because it doesn't do dns lookup)
    val slf4j            = "1.7.26"

    val refererParser    = "1.0.0"
    val maxmindIplookups = "0.6.1"
    val circe            = "0.11.1"
    val circeOptics      = "0.11.0"
    val circeJackson     = "0.11.1"
    val scalaForex       = "0.7.0"
    val scalaWeather     = "0.5.0"
    val gatlingJsonpath  = "0.6.14"
    val scalaUri         = "1.4.5"
    val scalaLruMap      = "0.3.0"
    val badRows          = "1.0.0"

    val snowplowRawEvent = "0.1.0"
    val collectorPayload = "0.0.0"
    val schemaSniffer    = "0.0.0"

    val awsSdk           = "1.11.566"
    val gcpSdk           = "1.106.0"
    val kinesisClient    = "1.10.0"
    val kafka            = "2.2.1"
    val nsqClient        = "1.2.0"
    val jackson          = "2.9.9"
    val config           = "1.3.4"

    val scopt            = "3.7.1"
    val pureconfig       = "0.11.0"
    val snowplowTracker  = "0.6.1"

    val specs2           = "4.5.1"
    val scalacheck       = "1.14.0"
    val jinJava          = "2.5.0"

    val sentry           = "1.7.30"
    val scio             = "0.8.1"
    val beam             = "2.18.0"
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

    val circeCore        = "io.circe"                   %% "circe-core"                    % V.circe
    val circeGeneric     = "io.circe"                   %% "circe-generic"                 % V.circe
    val circeParser      = "io.circe"                   %% "circe-parser"                  % V.circe
    val circeLiteral     = "io.circe"                   %% "circe-literal"                 % V.circe
    val circeJava8       = "io.circe"                   %% "circe-java8"                   % V.circe
    val circeOptics      = "io.circe"                   %% "circe-optics"                  % V.circeOptics
    val circeJackson     = "io.circe"                   %% "circe-jackson29"               % V.circeJackson
    val scalaUri         = "io.lemonlabs"               %% "scala-uri"                     % V.scalaUri
    val gatlingJsonpath  = "io.gatling"                 %% "jsonpath"                      % V.gatlingJsonpath
    val scalaForex       = "com.snowplowanalytics"      %% "scala-forex"                   % V.scalaForex
    val refererParser    = "com.snowplowanalytics"      %% "scala-referer-parser"          % V.refererParser
    val maxmindIplookups = "com.snowplowanalytics"      %% "scala-maxmind-iplookups"       % V.maxmindIplookups
    val scalaWeather     = "com.snowplowanalytics"      %% "scala-weather"                 % V.scalaWeather
    val scalaLruMap      = "com.snowplowanalytics"      %% "scala-lru-map"                 % V.scalaLruMap
    val badRows          = "com.snowplowanalytics"      %% "snowplow-badrows"              % V.badRows
    val snowplowRawEvent = "com.snowplowanalytics"      %  "snowplow-thrift-raw-event"     % V.snowplowRawEvent
    val collectorPayload = "com.snowplowanalytics"      %  "collector-payload-1"           % V.collectorPayload
    val schemaSniffer    = "com.snowplowanalytics"      %  "schema-sniffer-1"              % V.schemaSniffer
    val iabClient        = "com.snowplowanalytics"      %  "iab-spiders-and-robots-client" % V.iabClient

    val specs2           = "org.specs2"                 %% "specs2-core"                   % V.specs2                    % Test
    val specs2Cats       = "org.specs2"                 %% "specs2-cats"                   % V.specs2                    % Test
    val specs2Scalacheck = "org.specs2"                 %% "specs2-scalacheck"             % V.specs2                    % Test
    val specs2Mock       = "org.specs2"                 %% "specs2-mock"                   % V.specs2                    % Test

    // Beam
    val sentry           = "io.sentry"                  %  "sentry"                                   % V.sentry
    val scio             = "com.spotify"                %% "scio-core"                                % V.scio
    val beam             = "org.apache.beam"            % "beam-runners-google-cloud-dataflow-java"   % V.beam
    val slf4j            = "org.slf4j"                  % "slf4j-simple"                              % V.slf4j
    val scioTest         = "com.spotify"                %% "scio-test"                                % V.scio            % Test
    val scalaTest        = "org.scalatest"              %% "scalatest"                                % V.scalaTest       % Test

    // Stream
    val kinesisSdk       = "com.amazonaws"                    %  "aws-java-sdk-kinesis"              % V.awsSdk
    val dynamodbSdk      = "com.amazonaws"                    %  "aws-java-sdk-dynamodb"             % V.awsSdk
    val s3Sdk            = "com.amazonaws"                    %  "aws-java-sdk-s3"                   % V.awsSdk
    val kinesisClient    = "com.amazonaws"                    %  "amazon-kinesis-client"             % V.kinesisClient
    val gsSdk            = "com.google.cloud"                 %  "google-cloud-storage"              % V.gcpSdk
    val kafkaClients     = "org.apache.kafka"                 %  "kafka-clients"                     % V.kafka
    val jacksonCbor      = "com.fasterxml.jackson.dataformat" %  "jackson-dataformat-cbor"           % V.jackson
    val config           = "com.typesafe"                     %  "config"                            % V.config
    val log4jOverSlf4j   = "org.slf4j"                        %  "log4j-over-slf4j"                  % V.slf4j
    val scopt            = "com.github.scopt"                 %% "scopt"                             % V.scopt
    val pureconfig       = "com.github.pureconfig"            %% "pureconfig"                        % V.pureconfig
    val nsqClient        = "com.snowplowanalytics"            %  "nsq-java-client"                   % V.nsqClient
    val snowplowTracker  = "com.snowplowanalytics"            %% "snowplow-scala-tracker-emitter-id" % V.snowplowTracker
    val scalacheck       = "org.scalacheck"                   %% "scalacheck"                        % V.scalacheck      % Test
    val kafka            = "org.apache.kafka"                 %% "kafka"                             % V.kafka           % Test
    val jinJava          = "com.hubspot.jinjava"              %  "jinjava"                           % V.jinJava         % Test
  }
}
