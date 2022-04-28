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
  .settings(BuildSettings.projectSettings)
  .settings(BuildSettings.compilerSettings)
  .settings(BuildSettings.resolverSettings)
  .aggregate(common, commonFs2, pubsub, pubsubDistroless, kinesis, kinesisDistroless, streamCommon, streamKinesis, streamKinesisDistroless, streamKafka, streamKafkaDistroless, streamNsq, streamNsqDistroless, streamStdin)

lazy val common = project
  .in(file("modules/common"))
  .settings(BuildSettings.commonBuildSettings)
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

lazy val streamCommonDependencies = Seq(
  libraryDependencies ++= Seq(
    Dependencies.Libraries.config,
    Dependencies.Libraries.sentry,
    Dependencies.Libraries.slf4j,
    Dependencies.Libraries.log4jOverSlf4j,
    Dependencies.Libraries.s3Sdk,
    Dependencies.Libraries.gcs,
    Dependencies.Libraries.gson,
    Dependencies.Libraries.scopt,
    Dependencies.Libraries.pureconfig,
    Dependencies.Libraries.snowplowTracker,
    Dependencies.Libraries.jacksonCbor,
    Dependencies.Libraries.specs2,
    Dependencies.Libraries.scalacheck
  )
)

lazy val streamCommon = project
  .in(file("modules/stream/common"))
  .enablePlugins(BuildInfoPlugin)
  .settings(BuildSettings.streamCommonBuildSettings)
  .settings(streamCommonDependencies)
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)
  .dependsOn(common)

lazy val streamKinesis = project
  .in(file("modules/stream/kinesis"))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(BuildSettings.streamKinesisBuildSettings)
  .settings(streamCommonDependencies)
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.kinesisClient,
    Dependencies.Libraries.kinesisSdk,
    Dependencies.Libraries.dynamodbSdk,
    Dependencies.Libraries.sts
  ))
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)
  .dependsOn(streamCommon)

lazy val streamKinesisDistroless = project
  .in(file("modules/distroless/stream/kinesis"))
  .enablePlugins(JavaAppPackaging, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (streamKinesis / sourceDirectory).value)
  .settings(BuildSettings.streamKinesisDistrolessBuildSettings)
  .dependsOn(streamKinesis)

lazy val streamKafka = project
  .in(file("modules/stream/kafka"))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(BuildSettings.streamKafkaBuildSettings)
  .settings(streamCommonDependencies)
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.kafkaClients,
    Dependencies.Libraries.mskAuth
  ))
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)
  .dependsOn(streamCommon)

lazy val streamKafkaDistroless = project
  .in(file("modules/distroless/stream/kafka"))
  .enablePlugins(JavaAppPackaging, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (streamKafka / sourceDirectory).value)
  .settings(BuildSettings.streamKafkaDistrolessBuildSettings)
  // Dependencies need to be stated explictly, otherwise compilation fails.
  .settings(streamCommonDependencies)
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.kafkaClients,
    Dependencies.Libraries.mskAuth
  ))
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)
  .dependsOn(streamCommon)

lazy val streamNsq = project
  .in(file("modules/stream/nsq"))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(BuildSettings.streamNsqBuildSettings)
  .settings(streamCommonDependencies)
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.log4j,
    Dependencies.Libraries.log4jApi,
    Dependencies.Libraries.nsqClient
  ))
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)
  .dependsOn(streamCommon)

lazy val streamNsqDistroless = project
  .in(file("modules/distroless/stream/nsq"))
  .enablePlugins(JavaAppPackaging, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (streamNsq / sourceDirectory).value)
  .settings(BuildSettings.streamNsqDistrolessBuildSettings)
  // Dependencies need to be stated explictly, otherwise compilation fails.
  .settings(streamCommonDependencies)
  .settings(libraryDependencies ++= Seq(
    Dependencies.Libraries.log4j,
    Dependencies.Libraries.log4jApi,
    Dependencies.Libraries.nsqClient
  ))
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)
  .dependsOn(streamCommon)

lazy val streamStdin = project
  .in(file("modules/stream/stdin"))
  .settings(BuildSettings.streamStdinBuildSettings)
  .settings(streamCommonDependencies)
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)
  .dependsOn(streamCommon)

lazy val commonFs2 = project
  .in(file("modules/common-fs2"))
  .enablePlugins(BuildInfoPlugin)
  .settings(BuildSettings.commonFs2BuildSettings)
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
  .dependsOn(common)


lazy val pubsub = project
  .in(file("modules/pubsub"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, DockerPlugin)
  .settings(BuildSettings.pubsubBuildSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.fs2BlobGcs,
      Dependencies.Libraries.fs2PubSub,
      Dependencies.Libraries.gson,
      Dependencies.Libraries.googleAuth
    ),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
  )
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)
  .dependsOn(commonFs2)

lazy val pubsubDistroless = project
  .in(file("modules/distroless/pubsub"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (pubsub / sourceDirectory).value)
  .settings(BuildSettings.pubsubDistrolessBuildSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.fs2BlobGcs,
      Dependencies.Libraries.fs2PubSub,
      Dependencies.Libraries.gson,
      Dependencies.Libraries.googleAuth
    ),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
  )
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)
  .dependsOn(commonFs2)


lazy val kinesis = project
  .in(file("modules/kinesis"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, DockerPlugin)
  .settings(BuildSettings.kinesisBuildSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.dynamodbSdk,
      Dependencies.Libraries.fs2BlobS3,
      Dependencies.Libraries.fs2Aws,
      Dependencies.Libraries.sts,
      Dependencies.Libraries.specs2CEIt,
      Dependencies.Libraries.specs2ScalacheckIt
    ),
    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
  )
  .settings(excludeDependencies ++= Dependencies.Libraries.exclusions)
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .dependsOn(commonFs2)

lazy val kinesisDistroless = project
  .in(file("modules/distroless/kinesis"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (kinesis / sourceDirectory).value)
  .settings(BuildSettings.kinesisDistrolessBuildSettings)
  .dependsOn(kinesis)

lazy val bench = project
  .in(file("modules/bench"))
  .dependsOn(pubsub % "test->test")
  .enablePlugins(JmhPlugin)
