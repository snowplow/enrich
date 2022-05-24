/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
// =======================================================
// scalafmt: {align.tokens = [":="]}
// =======================================================

lazy val root = project.in(file("."))
  .settings(name := "enrich")
  .settings(BuildSettings.basicSettings)
  .aggregate(common, commonFs2, pubsub, kinesis, streamCommon, streamKinesis, streamKafka, streamNsq, streamStdin)

lazy val common = project
  .in(file("modules/common"))
  .settings(
    name := "snowplow-common-enrich",
    description := "Common functionality for enriching raw Snowplow events"
  )
  .settings(BuildSettings.formatting)
  .settings(BuildSettings.basicSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.scoverageSettings)
  .settings(Test / parallelExecution := false)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.jodaTime,
      Dependencies.Libraries.commonsCodec,
      Dependencies.Libraries.useragent,
      Dependencies.Libraries.jacksonDatabind,
      Dependencies.Libraries.uaParser,
      Dependencies.Libraries.postgresDriver,
      Dependencies.Libraries.mysqlConnector,
      Dependencies.Libraries.hikariCP,
      Dependencies.Libraries.jaywayJsonpath,
      Dependencies.Libraries.iabClient,
      Dependencies.Libraries.yauaa,
      Dependencies.Libraries.guava,
      Dependencies.Libraries.circeOptics,
      Dependencies.Libraries.circeJackson,
      Dependencies.Libraries.refererParser,
      Dependencies.Libraries.maxmindIplookups,
      Dependencies.Libraries.scalaUri,
      Dependencies.Libraries.scalaForex,
      Dependencies.Libraries.scalaWeather,
      Dependencies.Libraries.gatlingJsonpath,
      Dependencies.Libraries.badRows,
      Dependencies.Libraries.igluClient,
      Dependencies.Libraries.snowplowRawEvent,
      Dependencies.Libraries.http4sClient,
      Dependencies.Libraries.collectorPayload,
      Dependencies.Libraries.schemaSniffer,
      Dependencies.Libraries.thrift,
      Dependencies.Libraries.sprayJson,
      Dependencies.Libraries.nettyAll,
      Dependencies.Libraries.nettyCodec,
      Dependencies.Libraries.protobuf,
      Dependencies.Libraries.specs2,
      Dependencies.Libraries.specs2Cats,
      Dependencies.Libraries.specs2Scalacheck,
      Dependencies.Libraries.specs2Mock,
      Dependencies.Libraries.circeLiteral % Test
    )
  )
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)

lazy val allStreamSettings = BuildSettings.basicSettings ++ BuildSettings.sbtAssemblySettings ++
  BuildSettings.formatting ++
  Seq(libraryDependencies ++= Seq(
    Dependencies.Libraries.config,
    Dependencies.Libraries.sentry,
    Dependencies.Libraries.slf4j,
    Dependencies.Libraries.log4jOverSlf4j,
    Dependencies.Libraries.s3Sdk,
    Dependencies.Libraries.gcs,
    Dependencies.Libraries.scopt,
    Dependencies.Libraries.pureconfig,
    Dependencies.Libraries.snowplowTracker,
    Dependencies.Libraries.jacksonCbor,
    Dependencies.Libraries.specs2,
    Dependencies.Libraries.scalacheck
  ))

lazy val streamCommon = project
  .in(file("modules/stream/common"))
  .settings(allStreamSettings)
  .settings(moduleName := "snowplow-stream-enrich")
  .settings(BuildSettings.scoverageSettings)
  .settings(coverageMinimum := 20)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](organization, name, version, "commonEnrichVersion" -> version.value),
    buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.stream.generated"
  )
  .dependsOn(common)
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)

lazy val streamKinesis = project
  .in(file("modules/stream/kinesis"))
  .settings(allStreamSettings)
  .settings(moduleName := "snowplow-stream-enrich-kinesis")
  .settings(BuildSettings.dockerSettings)
  .settings(Docker / packageName := "stream-enrich-kinesis")
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.kinesisClient,
    Dependencies.Libraries.kinesisSdk,
    Dependencies.Libraries.dynamodbSdk,
    Dependencies.Libraries.sts
  ))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(streamCommon)
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)

lazy val streamKafka = project
  .in(file("modules/stream/kafka"))
  .settings(moduleName := "snowplow-stream-enrich-kafka")
  .settings(allStreamSettings)
  .settings(BuildSettings.dockerSettings)
  .settings(Docker / packageName := "stream-enrich-kafka")
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.kafkaClients,
    Dependencies.Libraries.mskAuth
  ))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(streamCommon)
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)

lazy val streamNsq = project
  .in(file("modules/stream/nsq"))
  .settings(moduleName := "snowplow-stream-enrich-nsq")
  .settings(allStreamSettings)
  .settings(BuildSettings.dockerSettings)
  .settings(Docker / packageName := "stream-enrich-nsq")
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.log4j,
    Dependencies.Libraries.log4jApi,
    Dependencies.Libraries.nsqClient
  ))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(streamCommon)
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)

lazy val streamStdin = project
  .in(file("modules/stream/stdin"))
  .settings(allStreamSettings)
  .settings(
    moduleName := "snowplow-stream-enrich-stdin",
  )
  .dependsOn(streamCommon)
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)

Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val commonFs2 = project
  .in(file("modules/common-fs2"))
  .dependsOn(common)
  .settings(BuildSettings.basicSettings)
  .settings(BuildSettings.formatting)
  .settings(BuildSettings.scoverageSettings)
  .settings(BuildSettings.addExampleConfToTestCp)
  .settings(
    name := "snowplow-enrich-common-fs2",
    description := "Common functionality for fs2 enrich assets",
  )
  .settings(Test / parallelExecution := false)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.decline,
      Dependencies.Libraries.circeExtras,
      Dependencies.Libraries.circeLiteral,
      Dependencies.Libraries.circeConfig,
      Dependencies.Libraries.catsEffect,
      Dependencies.Libraries.fs2,
      Dependencies.Libraries.fs2Io,
      Dependencies.Libraries.slf4j,
      Dependencies.Libraries.sentry,
      Dependencies.Libraries.log4cats,
      Dependencies.Libraries.catsRetry,
      Dependencies.Libraries.igluClient,
      Dependencies.Libraries.igluClientHttp4s,
      Dependencies.Libraries.http4sClient,
      Dependencies.Libraries.http4sCirce,
      Dependencies.Libraries.pureconfig.withRevision(Dependencies.V.pureconfig013),
      Dependencies.Libraries.pureconfigCats.withRevision(Dependencies.V.pureconfig013),
      Dependencies.Libraries.pureconfigCirce.withRevision(Dependencies.V.pureconfig013),
      Dependencies.Libraries.trackerCore,
      Dependencies.Libraries.emitterHttps,
      Dependencies.Libraries.specs2,
      Dependencies.Libraries.specs2CE,
      Dependencies.Libraries.scalacheck,
      Dependencies.Libraries.specs2Scalacheck,
      Dependencies.Libraries.http4sDsl,
      Dependencies.Libraries.http4sServer,
      Dependencies.Libraries.eventGen,
      Dependencies.Libraries.specsDiff,
      Dependencies.Libraries.circeCore % Test,
      Dependencies.Libraries.circeGeneric % Test,
      Dependencies.Libraries.circeParser % Test
    ),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
  )
  .enablePlugins(BuildInfoPlugin)

lazy val pubsub = project
  .in(file("modules/pubsub"))
  .dependsOn(commonFs2)
  .settings(BuildSettings.basicSettings)
  .settings(BuildSettings.formatting)
  .settings(BuildSettings.scoverageSettings)
  .settings(BuildSettings.sbtAssemblySettings)
  .settings(
    name := "snowplow-enrich-pubsub",
    description := "High-performance streaming Snowplow Enrich job for PubSub built on top of functional streams"
  )
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](organization, name, version, description),
    buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.pubsub.generated",
  )
  .settings(Docker / packageName := "snowplow-enrich-pubsub")
  .settings(Test / parallelExecution := false)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.fs2BlobGcs,
      Dependencies.Libraries.gcs,
      Dependencies.Libraries.fs2PubSub
    ),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
  )
  .enablePlugins(BuildInfoPlugin)
  .settings(BuildSettings.dockerSettings)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, DockerPlugin)
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)

lazy val kinesis = project
  .in(file("modules/kinesis"))
  .dependsOn(commonFs2)
  .settings(BuildSettings.basicSettings)
  .settings(BuildSettings.formatting)
  .settings(BuildSettings.scoverageSettings)
  .settings(BuildSettings.sbtAssemblySettings)
  .settings(
    name := "snowplow-enrich-kinesis",
    description := "High-performance app built on top of functional streams that enriches Snowplow events from Kinesis",
    buildInfoKeys := Seq[BuildInfoKey](organization, name, version, description),
    buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.kinesis.generated",
    Docker / packageName := "snowplow-enrich-kinesis",
  )
  .settings(Test / parallelExecution := false)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.dynamodbSdk,
      Dependencies.Libraries.kinesisSdk,
      Dependencies.Libraries.fs2BlobS3,
      Dependencies.Libraries.fs2Aws,
      Dependencies.Libraries.kinesisClient2,
      Dependencies.Libraries.sts,
      Dependencies.Libraries.specs2CEIt,
      Dependencies.Libraries.specs2ScalacheckIt
    ),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
  )
  .enablePlugins(BuildInfoPlugin)
  .settings(BuildSettings.dockerSettings)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, DockerPlugin)
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)

lazy val bench = project
  .in(file("modules/bench"))
  .dependsOn(pubsub % "test->test")
  .enablePlugins(JmhPlugin)
