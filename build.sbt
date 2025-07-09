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
  .aggregate(common, core, cloudUtils, awsUtils, azureUtils, gcpUtils, pubsub, kinesis, kafka, nsq)

lazy val common = project
  .in(file("modules/common"))
  .settings(commonBuildSettings)
  .settings(libraryDependencies ++= commonDependencies)
  .settings(excludeDependencies ++= exclusions)

lazy val core = project
  .in(file("modules/core"))
  .enablePlugins(BuildInfoPlugin)
  .settings(coreBuildSettings)
  .settings(libraryDependencies ++= coreDependencies ++ Seq(
    // integration tests dependencies
    dockerJavaIt
  ))
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "test->test;compile->compile")
  .dependsOn(cloudUtils % "test->test;compile->compile")
  .settings(Test / igluUris := Seq(
    "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
    "iglu:com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0"
  ))

lazy val cloudUtils = project
  .in(file("modules/cloudutils/core"))
  .settings(cloudUtilsBuildSettings)
  .settings(libraryDependencies ++= cloudUtilsDependencies)

lazy val awsUtils = project
  .in(file("modules/cloudutils/aws"))
  .settings(awsUtilsBuildSettings)
  .settings(libraryDependencies ++= awsUtilsDependencies)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(cloudUtils % "compile->compile")

lazy val gcpUtils = project
  .in(file("modules/cloudutils/gcp"))
  .settings(gcpUtilsBuildSettings)
  .settings(libraryDependencies ++= gcpUtilsDependencies)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(cloudUtils % "compile->compile")

lazy val azureUtils = project
  .in(file("modules/cloudutils/azure"))
  .settings(azureUtilsBuildSettings)
  .settings(libraryDependencies ++= azureUtilsDependencies)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(cloudUtils % "compile->compile")

lazy val pubsub = project
  .in(file("modules/pubsub"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(pubsubBuildSettings)
  .settings(libraryDependencies ++= pubsubDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(core % "compile->compile")
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
  .dependsOn(core % "compile->compile")
  .dependsOn(gcpUtils % "compile->compile")

lazy val kinesis = project
  .in(file("modules/kinesis"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(kinesisBuildSettings)
  .settings(libraryDependencies ++= kinesisDependencies ++ Seq(
    // integration tests dependencies
    specs2CEIt,
    testContainersIt
  ))
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(core % "test->test;compile->compile;it->it")
  .dependsOn(awsUtils % "compile->compile")
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings((IntegrationTest / test) := (IntegrationTest / test).dependsOn(Docker / publishLocal).value)
  .settings((IntegrationTest / testOnly) := (IntegrationTest / testOnly).dependsOn(Docker / publishLocal).evaluated)

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
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(core % "compile->compile;it->it")
  .dependsOn(awsUtils % "compile->compile")
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
    specs2CEIt,
    testContainersIt
  ))
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(core % "compile->compile;it->it")
  .dependsOn(azureUtils % "compile->compile")
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings((IntegrationTest / test) := (IntegrationTest / test).dependsOn(Docker / publishLocal).value)
  .settings((IntegrationTest / testOnly) := (IntegrationTest / testOnly).dependsOn(Docker / publishLocal).evaluated)

lazy val kafkaDistroless = project
  .in(file("modules/distroless/kafka"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)
  .settings(sourceDirectory := (kafka / sourceDirectory).value)
  .settings(kafkaDistrolessBuildSettings)
  .settings(libraryDependencies ++= kafkaDependencies ++ Seq(
    // integration tests dependencies
    specs2CEIt,
    testContainersIt
  ))
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(core % "compile->compile;it->it")
  .dependsOn(azureUtils % "compile->compile")
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings((IntegrationTest / test) := (IntegrationTest / test).dependsOn(Docker / publishLocal).value)
  .settings((IntegrationTest / testOnly) := (IntegrationTest / testOnly).dependsOn(Docker / publishLocal).evaluated)

lazy val nsq = project
  .in(file("modules/nsq"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(nsqBuildSettings)
  .settings(libraryDependencies ++= nsqDependencies ++ Seq(
    // integration tests dependencies
    specs2CEIt,
    testContainersIt
  ))
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(core % "compile->compile;it->it")
  .dependsOn(azureUtils % "compile->compile")
  .dependsOn(awsUtils % "compile->compile")
  .dependsOn(gcpUtils % "compile->compile")
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings((IntegrationTest / test) := (IntegrationTest / test).dependsOn(Docker / publishLocal).value)
  .settings((IntegrationTest / testOnly) := (IntegrationTest / testOnly).dependsOn(Docker / publishLocal).evaluated)

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
  .dependsOn(core % "compile->compile;it->it")
  .dependsOn(azureUtils % "compile->compile")
  .dependsOn(awsUtils % "compile->compile")
  .dependsOn(gcpUtils % "compile->compile")
  .settings(Defaults.itSettings)
  .configs(IntegrationTest)
  .settings((IntegrationTest / test) := (IntegrationTest / test).dependsOn(Docker / publishLocal).value)
  .settings((IntegrationTest / testOnly) := (IntegrationTest / testOnly).dependsOn(Docker / publishLocal).evaluated)