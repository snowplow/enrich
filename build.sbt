/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
  .aggregate(common, beam, stream, kinesis, kafka, nsq, stdin, integrationTests)

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
  .settings(parallelExecution in Test := false)
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

lazy val stream = project
  .in(file("modules/stream"))
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

lazy val kinesis = project
  .in(file("modules/kinesis"))
  .settings(allStreamSettings)
  .settings(moduleName := "snowplow-stream-enrich-kinesis")
  .settings(packageName in Docker := "snowplow/stream-enrich-kinesis")
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.kinesisClient,
    Dependencies.Libraries.kinesisSdk,
    Dependencies.Libraries.dynamodbSdk,
    Dependencies.Libraries.jacksonCbor
  ))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(stream)

lazy val kafka = project
  .in(file("modules/kafka"))
  .settings(moduleName := "snowplow-stream-enrich-kafka")
  .settings(allStreamSettings)
  .settings(
    packageName in Docker := "snowplow/stream-enrich-kafka",
  )
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.kafkaClients
  ))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(stream)

lazy val nsq = project
  .in(file("modules/nsq"))
  .settings(moduleName := "snowplow-stream-enrich-nsq")
  .settings(allStreamSettings)
  .settings(
    packageName in Docker := "snowplow/stream-enrich-nsq",
  )
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.nsqClient
  ))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .dependsOn(stream)

lazy val stdin = project
  .in(file("modules/stdin"))
  .settings(allStreamSettings)
  .settings(
    moduleName := "snowplow-stream-enrich-stdin",
  )
  .dependsOn(stream)

lazy val beam =
  project
    .in(file("modules/beam"))
    .dependsOn(common)
    .settings(BuildSettings.basicSettings)
    .settings(BuildSettings.dataflowDockerSettings)
    .settings(BuildSettings.formatting)
    .settings(BuildSettings.scoverageSettings)
    .settings(
      name := "beam-enrich",
      description := "Streaming enrich job written using SCIO",
      buildInfoKeys := Seq[BuildInfoKey](organization, name, version, "sceVersion" -> version.value),
      buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.beam.generated",
      libraryDependencies ++= Seq(
        Dependencies.Libraries.scio,
        Dependencies.Libraries.beam,
        Dependencies.Libraries.sentry,
        Dependencies.Libraries.slf4j,
        Dependencies.Libraries.scioTest,
        Dependencies.Libraries.scalaTest,
        Dependencies.Libraries.circeLiteral % Test,
      ),
      packageName in Docker := "snowplow/beam-enrich"
    )
    .settings(
      libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)
    )
    .settings(
      publish := {},
      publishLocal := {},
      publishArtifact := false,
      testGrouping in Test := BuildSettings.oneJVMPerTest((definedTests in Test).value)
    )
    .enablePlugins(JavaAppPackaging, DockerPlugin, BuildInfoPlugin)

lazy val integrationTests = project
  .in(file("modules/integration-tests"))
  .settings(moduleName := "integration-tests")
  .settings(allStreamSettings)
  .settings(BuildSettings.addExampleConfToTestCp)
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.kafka,
    Dependencies.Libraries.jinJava
  ))
  .dependsOn(stream % "test->test", kafka % "test->compile")
