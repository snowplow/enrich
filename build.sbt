/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
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
  .aggregate(common, pubsub, kinesis, beam, streamCommon, streamKinesis, streamKafka, streamNsq, streamStdin)

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
      Dependencies.Libraries.collectorPayload,
      Dependencies.Libraries.schemaSniffer,
      Dependencies.Libraries.specs2,
      Dependencies.Libraries.specs2Cats,
      Dependencies.Libraries.specs2Scalacheck,
      Dependencies.Libraries.specs2Mock,
      Dependencies.Libraries.circeLiteral % Test
    )
  )

lazy val allStreamSettings = BuildSettings.basicSettings ++ BuildSettings.sbtAssemblySettings ++
  BuildSettings.dockerSettings ++ BuildSettings.formatting ++
  Seq(libraryDependencies ++= Seq(
    Dependencies.Libraries.config,
    Dependencies.Libraries.sentry,
    Dependencies.Libraries.slf4j,
    Dependencies.Libraries.log4jOverSlf4j,
    Dependencies.Libraries.s3Sdk,
    Dependencies.Libraries.gsSdk,
    Dependencies.Libraries.scopt,
    Dependencies.Libraries.pureconfig,
    Dependencies.Libraries.snowplowTracker,
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

lazy val streamKinesis = project
  .in(file("modules/stream/kinesis"))
  .settings(allStreamSettings)
  .settings(moduleName := "snowplow-stream-enrich-kinesis")
  .settings(Docker / packageName := "snowplow/stream-enrich-kinesis")
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.kinesisClient,
    Dependencies.Libraries.kinesisSdk,
    Dependencies.Libraries.dynamodbSdk,
    Dependencies.Libraries.jacksonCbor
  ))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(streamCommon)

lazy val streamKafka = project
  .in(file("modules/stream/kafka"))
  .settings(moduleName := "snowplow-stream-enrich-kafka")
  .settings(allStreamSettings)
  .settings(
    Docker / packageName := "snowplow/stream-enrich-kafka",
  )
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.kafkaClients
  ))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(streamCommon)

lazy val streamNsq = project
  .in(file("modules/stream/nsq"))
  .settings(moduleName := "snowplow-stream-enrich-nsq")
  .settings(allStreamSettings)
  .settings(
    Docker / packageName := "snowplow/stream-enrich-nsq",
  )
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.log4j,
    Dependencies.Libraries.log4jApi,
    Dependencies.Libraries.nsqClient
  ))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(streamCommon)

lazy val streamStdin = project
  .in(file("modules/stream/stdin"))
  .settings(allStreamSettings)
  .settings(
    moduleName := "snowplow-stream-enrich-stdin",
  )
  .dependsOn(streamCommon)

lazy val beam =
  project
    .in(file("modules/beam"))
    .dependsOn(common)
    .settings(BuildSettings.basicSettings)
    .settings(BuildSettings.dataflowDockerSettings)
    .settings(BuildSettings.formatting)
    .settings(BuildSettings.scoverageSettings)
    .settings(BuildSettings.sbtAssemblySettings)
    .settings(
      name := "beam-enrich",
      description := "Streaming enrich job written using SCIO",
      buildInfoKeys := Seq[BuildInfoKey](organization, name, version, "sceVersion" -> version.value),
      buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.beam.generated",
      libraryDependencies ++= Seq(
        Dependencies.Libraries.scioCore,
        Dependencies.Libraries.scioGCP,
        Dependencies.Libraries.beam,
        Dependencies.Libraries.grpc,
        Dependencies.Libraries.sentry,
        Dependencies.Libraries.slf4j,
        Dependencies.Libraries.scioTest,
        Dependencies.Libraries.scalaTest,
        Dependencies.Libraries.circeLiteral % Test,
      ),
      dependencyOverrides ++= Seq(
        "io.grpc" % "grpc-alts" % Dependencies.V.grpc,
        "io.grpc" % "grpc-auth" % Dependencies.V.grpc,
        "io.grpc" % "grpc-core" % Dependencies.V.grpc,
        "io.grpc" % "grpc-context" % Dependencies.V.grpc,
        "io.grpc" % "grpc-grpclb" % Dependencies.V.grpc,
        "io.grpc" % "grpc-netty" % Dependencies.V.grpc,
        "io.grpc" % "grpc-netty-shaded" % Dependencies.V.grpc,
        "io.grpc" % "grpc-api" % Dependencies.V.grpc,
        "io.grpc" % "grpc-stub" % Dependencies.V.grpc,
        "io.grpc" % "grpc-protobuf" % Dependencies.V.grpc,
        "io.grpc" % "grpc-protobuf-lite" % Dependencies.V.grpc,
      ),
      Docker / packageName := "snowplow/beam-enrich"
    )
    .settings(
      libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)
    )
    .settings(
      publish := {},
      publishLocal := {},
      publishArtifact := false,
      Test / testGrouping := BuildSettings.oneJVMPerTest((Test / definedTests).value)
    )
    .enablePlugins(JavaAppPackaging, DockerPlugin, BuildInfoPlugin)

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
      Dependencies.Libraries.http4sClient,
      Dependencies.Libraries.http4sCirce,
      Dependencies.Libraries.fs2BlobS3,
      Dependencies.Libraries.fs2BlobGcs,
      Dependencies.Libraries.pureconfig.withRevision(Dependencies.V.pureconfig013),
      Dependencies.Libraries.pureconfigCats.withRevision(Dependencies.V.pureconfig013),
      Dependencies.Libraries.pureconfigCirce.withRevision(Dependencies.V.pureconfig013),
      Dependencies.Libraries.specs2,
      Dependencies.Libraries.specs2CE,
      Dependencies.Libraries.scalacheck,
      Dependencies.Libraries.specs2Scalacheck,
      Dependencies.Libraries.http4sDsl,
      Dependencies.Libraries.http4sServer
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
    description := "High-performance streaming Snowplow Enrich job for PubSub built on top of functional streams",
    buildInfoKeys := Seq[BuildInfoKey](organization, name, version, description),
    buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.pubsub.generated",
    Docker / packageName := "snowplow/snowplow-enrich-pubsub",
  )
  .settings(Test / parallelExecution := false)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.fs2PubSub,
    ),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
  )
  .enablePlugins(BuildInfoPlugin)
  .settings(BuildSettings.dockerSettings)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, DockerPlugin)

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
    Docker / packageName := "snowplow/snowplow-enrich-kinesis",
  )
  .settings(Test / parallelExecution := false)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.fs2Aws,
    ),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
  )
  .enablePlugins(BuildInfoPlugin)
  .settings(BuildSettings.dockerSettings)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, DockerPlugin)

lazy val bench = project
  .in(file("modules/bench"))
  .dependsOn(pubsub % "test->test")
  .enablePlugins(JmhPlugin)
