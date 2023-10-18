/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
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
  .aggregate(common, commonFs2, pubsub, pubsubDistroless, kinesis, kinesisDistroless, kafka, kafkaDistroless, nsq, nsqDistroless)

lazy val common = project
  .in(file("modules/common"))
  .settings(commonBuildSettings)
  .settings(libraryDependencies ++= commonDependencies)
  .settings(excludeDependencies ++= exclusions)

lazy val commonFs2 = project
  .in(file("modules/common-fs2"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonFs2BuildSettings)
  .settings(libraryDependencies ++= commonFs2Dependencies)
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "test->test;compile->compile")


lazy val pubsub = project
  .in(file("modules/pubsub"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(pubsubBuildSettings)
  .settings(libraryDependencies ++= pubsubDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(commonFs2)

lazy val pubsubDistroless = project
  .in(file("modules/distroless/pubsub"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)
  .settings(sourceDirectory := (pubsub / sourceDirectory).value)
  .settings(pubsubDistrolessBuildSettings)
  .settings(libraryDependencies ++= pubsubDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(commonFs2)


lazy val kinesis = project
  .in(file("modules/kinesis"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(kinesisBuildSettings)
  .settings(libraryDependencies ++= kinesisDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(commonFs2)

lazy val kinesisDistroless = project
  .in(file("modules/distroless/kinesis"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)
  .settings(sourceDirectory := (kinesis / sourceDirectory).value)
  .settings(kinesisDistrolessBuildSettings)
  .settings(libraryDependencies ++= kinesisDependencies ++ Seq(
    // integration tests dependencies
    specs2CEIt,
    testContainersIt
  ))
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(commonFs2 % "compile->compile;it->it")
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings((IntegrationTest / test) := (IntegrationTest / test).dependsOn(Docker / publishLocal).value)
  .settings((IntegrationTest / testOnly) := (IntegrationTest / testOnly).dependsOn(Docker / publishLocal).evaluated)

lazy val kafka = project
  .in(file("modules/kafka"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(kafkaBuildSettings)
  .settings(libraryDependencies ++= kafkaDependencies ++ Seq(
    // integration tests dependencies
    specs2CEIt
  ))
  .settings(excludeDependencies ++= exclusions)
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(commonFs2 % "compile->compile;it->it")

lazy val kafkaDistroless = project
  .in(file("modules/distroless/kafka"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)
  .settings(sourceDirectory := (kafka / sourceDirectory).value)
  .settings(kafkaDistrolessBuildSettings)
  .settings(libraryDependencies ++= kafkaDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(commonFs2)

lazy val nsq = project
  .in(file("modules/nsq"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(nsqBuildSettings)
  .settings(libraryDependencies ++= nsqDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(commonFs2)

lazy val nsqDistroless = project
  .in(file("modules/distroless/nsq"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)
  .settings(sourceDirectory := (nsq / sourceDirectory).value)
  .settings(nsqDistrolessBuildSettings)
  .settings(libraryDependencies ++= nsqDependencies ++ Seq(
    // integration tests dependencies
    specs2CEIt,
    testContainersIt
  ))
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(commonFs2 % "compile->compile;it->it")
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings((IntegrationTest / test) := (IntegrationTest / test).dependsOn(Docker / publishLocal).value)
  .settings((IntegrationTest / testOnly) := (IntegrationTest / testOnly).dependsOn(Docker / publishLocal).evaluated)
