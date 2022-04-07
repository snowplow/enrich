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
import Dependencies.Libraries._
import BuildSettings._

lazy val root = project.in(file("."))
  .settings(name := "enrich")
  .settings(projectSettings)
  .settings(compilerSettings)
  .settings(resolverSettings)
  .aggregate(common, commonFs2, pubsub, pubsubDistroless, kinesis, kinesisDistroless, streamCommon, streamKinesis, streamKinesisDistroless, streamKafka, streamKafkaDistroless, streamNsq, streamNsqDistroless, streamStdin)

lazy val common = project
  .in(file("modules/common"))
  .settings(commonBuildSettings)
  .settings(libraryDependencies ++= commonDependencies)
  .settings(excludeDependencies ++= exclusions)

lazy val streamCommon = project
  .in(file("modules/stream/common"))
  .enablePlugins(BuildInfoPlugin)
  .settings(streamCommonBuildSettings)
  .settings(libraryDependencies ++= streamCommonDependencies)
  .settings(excludeDependencies ++= exclusions)
  .dependsOn(common)

lazy val streamKinesis = project
  .in(file("modules/stream/kinesis"))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(streamKinesisBuildSettings)
  .settings(libraryDependencies ++= streamKinesisDependencies)
  .settings(excludeDependencies ++= exclusions)
  .dependsOn(streamCommon)

lazy val streamKinesisDistroless = project
  .in(file("modules/distroless/stream/kinesis"))
  .enablePlugins(JavaAppPackaging, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (streamKinesis / sourceDirectory).value)
  .settings(streamKinesisDistrolessBuildSettings)
  .settings(libraryDependencies ++= streamKinesisDependencies)
  .settings(excludeDependencies ++= exclusions)
  .dependsOn(streamCommon)

lazy val streamKafka = project
  .in(file("modules/stream/kafka"))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(streamKafkaBuildSettings)
  .settings(libraryDependencies ++= streamKafkaDependencies)
  .settings(excludeDependencies ++= exclusions)
  .dependsOn(streamCommon)

lazy val streamKafkaDistroless = project
  .in(file("modules/distroless/stream/kafka"))
  .enablePlugins(JavaAppPackaging, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (streamKafka / sourceDirectory).value)
  .settings(streamKafkaDistrolessBuildSettings)
  .settings(libraryDependencies ++= streamKafkaDependencies)
  .settings(excludeDependencies ++= exclusions)
  .dependsOn(streamCommon)

lazy val streamNsq = project
  .in(file("modules/stream/nsq"))
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(streamNsqBuildSettings)
  .settings(libraryDependencies ++= streamNsqDependencies)
  .settings(excludeDependencies ++= exclusions)
  .dependsOn(streamCommon)

lazy val streamNsqDistroless = project
  .in(file("modules/distroless/stream/nsq"))
  .enablePlugins(JavaAppPackaging, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (streamNsq / sourceDirectory).value)
  .settings(streamNsqDistrolessBuildSettings)
  .settings(libraryDependencies ++= streamNsqDependencies)
  .settings(excludeDependencies ++= exclusions)
  .dependsOn(streamCommon)

lazy val streamStdin = project
  .in(file("modules/stream/stdin"))
  .settings(streamStdinBuildSettings)
  .settings(libraryDependencies ++= streamCommonDependencies)
  .settings(excludeDependencies ++= exclusions)
  .dependsOn(streamCommon)

lazy val commonFs2 = project
  .in(file("modules/common-fs2"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonFs2BuildSettings)
  .settings(libraryDependencies ++= commonFs2Dependencies)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common)


lazy val pubsub = project
  .in(file("modules/pubsub"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, DockerPlugin)
  .settings(pubsubBuildSettings)
  .settings(libraryDependencies ++= pubsubDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(commonFs2)

lazy val pubsubDistroless = project
  .in(file("modules/distroless/pubsub"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (pubsub / sourceDirectory).value)
  .settings(pubsubDistrolessBuildSettings)
  .settings(libraryDependencies ++= pubsubDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(commonFs2)


lazy val kinesis = project
  .in(file("modules/kinesis"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, DockerPlugin)
  .settings(kinesisBuildSettings)
  .settings(libraryDependencies ++= (kinesisDependencies) ++ Seq(
      // integration test dependencies
      specs2CEIt,
      specs2ScalacheckIt
  ))
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .dependsOn(commonFs2)

lazy val kinesisDistroless = project
  .in(file("modules/distroless/kinesis"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, DockerPlugin, LauncherJarPlugin)
  .settings(sourceDirectory := (kinesis / sourceDirectory).value)
  .settings(kinesisDistrolessBuildSettings)
  .settings(libraryDependencies ++= kinesisDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(commonFs2)

lazy val bench = project
  .in(file("modules/bench"))
  .dependsOn(pubsub % "test->test")
  .enablePlugins(JmhPlugin)
