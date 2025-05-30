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
// scalafmt: {align.tokens = [":="]}
// =======================================================
import Dependencies.Libraries._
import BuildSettings._

lazy val root = project.in(file("."))
  .settings(name := "enrich")
  .settings(projectSettings)
  .settings(compilerSettings)
  .settings(resolverSettings)
  .aggregate(common, commonFs2, commonStreams, pubsub, kinesis, kinesisStreams, kafka, nsq)

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

lazy val commonStreams = project
  .in(file("modules/common-streams/core"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonStreamsBuildSettings)
  .settings(libraryDependencies ++= commonStreamsDependencies)
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "test->test;compile->compile")
  .dependsOn(cloudUtilsStreams % "test->test;compile->compile")
  .settings(Test / igluUris := Seq(
    "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
    "iglu:com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0"
  ))

lazy val cloudUtilsStreams = project
  .in(file("modules/common-streams/cloudutils/core"))
  .settings(cloudUtilsStreamsBuildSettings)
  .settings(libraryDependencies ++= cloudUtilsStreamsDependencies)

lazy val awsUtils = project
  .in(file("modules/cloudutils/aws"))
  .settings(awsUtilsBuildSettings)
  .settings(libraryDependencies ++= awsUtilsDependencies)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(commonFs2 % "test->test;compile->compile")

lazy val awsUtilsStreams = project
  .in(file("modules/common-streams/cloudutils/aws"))
  .settings(awsUtilsStreamsBuildSettings)
  .settings(libraryDependencies ++= awsUtilsDependencies)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(cloudUtilsStreams % "compile->compile")

lazy val gcpUtils = project
  .in(file("modules/cloudutils/gcp"))
  .settings(gcpUtilsBuildSettings)
  .settings(libraryDependencies ++= gcpUtilsDependencies)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(commonFs2 % "test->test;compile->compile")

lazy val azureUtils = project
  .in(file("modules/cloudutils/azure"))
  .settings(azureUtilsBuildSettings)
  .settings(libraryDependencies ++= azureUtilsDependencies)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(commonFs2 % "test->test;compile->compile")

lazy val pubsub = project
  .in(file("modules/pubsub"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(pubsubBuildSettings)
  .settings(libraryDependencies ++= pubsubDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(commonFs2 % "test->test;compile->compile")
  .dependsOn(gcpUtils % "compile->compile")

lazy val pubsubDistroless = project
  .in(file("modules/distroless/pubsub"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)
  .settings(sourceDirectory := (pubsub / sourceDirectory).value)
  .settings(pubsubDistrolessBuildSettings)
  .settings(libraryDependencies ++= pubsubDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(commonFs2 % "test->test;compile->compile")
  .dependsOn(gcpUtils % "compile->compile")

lazy val kinesis = project
  .in(file("modules/kinesis"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(kinesisBuildSettings)
  .settings(libraryDependencies ++= kinesisDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(commonFs2 % "test->test;compile->compile")
  .dependsOn(awsUtils % "compile->compile")

lazy val kinesisDistroless = project
  .in(file("modules/distroless/kinesis"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)
  .settings(sourceDirectory := (kinesis / sourceDirectory).value)
  .settings(kinesisDistrolessBuildSettings)
  .settings(libraryDependencies ++= kinesisDependencies ++ Seq(
    // integration tests dependencies
    specs2CEIt,
    testContainersIt,
    dockerJavaIt
  ))
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(commonFs2 % "compile->compile;it->it")
  .dependsOn(awsUtils % "compile->compile")
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings((IntegrationTest / test) := (IntegrationTest / test).dependsOn(Docker / publishLocal).value)
  .settings((IntegrationTest / testOnly) := (IntegrationTest / testOnly).dependsOn(Docker / publishLocal).evaluated)

lazy val kinesisStreams = project
  .in(file("modules/common-streams/kinesis"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(kinesisStreamsBuildSettings)
  .settings(libraryDependencies ++= kinesisStreamsDependencies ++ Seq(
    // integration tests dependencies
    specs2CEIt,
    testContainersIt,
    dockerJavaIt
  ))
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(commonStreams % "test->test;compile->compile;it->it")
  .dependsOn(awsUtilsStreams % "compile->compile")
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings((IntegrationTest / test) := (IntegrationTest / test).dependsOn(Docker / publishLocal).value)
  .settings((IntegrationTest / testOnly) := (IntegrationTest / testOnly).dependsOn(Docker / publishLocal).evaluated)

lazy val kinesisStreamsDistroless = project
  .in(file("modules/distroless/streams/kinesis"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)
  .settings(sourceDirectory := (kinesisStreams / sourceDirectory).value)
  .settings(kinesisStreamsDistrolessBuildSettings)
  .settings(libraryDependencies ++= kinesisStreamsDependencies ++ Seq(
    // integration tests dependencies
    specs2CEIt,
    testContainersIt,
    dockerJavaIt
  ))
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(commonStreams % "compile->compile;it->it")
  .dependsOn(awsUtilsStreams % "compile->compile")
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
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(commonFs2 % "compile->compile;test->test;it->it")
  .dependsOn(awsUtils % "compile->compile")
  .dependsOn(gcpUtils % "compile->compile")
  .dependsOn(azureUtils % "compile->compile")

lazy val kafkaDistroless = project
  .in(file("modules/distroless/kafka"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)
  .settings(sourceDirectory := (kafka / sourceDirectory).value)
  .settings(kafkaDistrolessBuildSettings)
  .settings(libraryDependencies ++= kafkaDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(commonFs2)
  .dependsOn(awsUtils % "compile->compile")
  .dependsOn(gcpUtils % "compile->compile")
  .dependsOn(azureUtils % "compile->compile")

lazy val nsq = project
  .in(file("modules/nsq"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(nsqBuildSettings)
  .settings(libraryDependencies ++= nsqDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(commonFs2 % "compile->compile;test->test")
  .dependsOn(awsUtils % "compile->compile")
  .dependsOn(gcpUtils % "compile->compile")
  .dependsOn(azureUtils % "compile->compile")

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
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(commonFs2 % "compile->compile;it->it")
  .dependsOn(awsUtils % "compile->compile")
  .dependsOn(gcpUtils % "compile->compile")
  .dependsOn(azureUtils % "compile->compile")
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings((IntegrationTest / test) := (IntegrationTest / test).dependsOn(Docker / publishLocal).value)
  .settings((IntegrationTest / testOnly) := (IntegrationTest / testOnly).dependsOn(Docker / publishLocal).evaluated)
