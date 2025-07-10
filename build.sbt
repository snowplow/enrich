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
  .aggregate(common, core, cloudUtils, awsUtils, azureUtils, gcpUtils, pubsub, kinesis, kafka, nsq, itCore, itKinesis, itKafka, itNsq)

lazy val common = project
  .in(file("modules/common"))
  .settings(commonBuildSettings)
  .settings(libraryDependencies ++= commonDependencies)
  .settings(excludeDependencies ++= exclusions)

lazy val core = project
  .in(file("modules/core"))
  .enablePlugins(BuildInfoPlugin)
  .settings(coreBuildSettings)
  .settings(libraryDependencies ++= coreDependencies)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(cloudUtils)
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
  .dependsOn(cloudUtils)

lazy val gcpUtils = project
  .in(file("modules/cloudutils/gcp"))
  .settings(gcpUtilsBuildSettings)
  .settings(libraryDependencies ++= gcpUtilsDependencies)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(cloudUtils)

lazy val azureUtils = project
  .in(file("modules/cloudutils/azure"))
  .settings(azureUtilsBuildSettings)
  .settings(libraryDependencies ++= azureUtilsDependencies)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(cloudUtils)

lazy val pubsub = project
  .in(file("modules/pubsub"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(pubsubBuildSettings)
  .settings(libraryDependencies ++= pubsubDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(core)
  .dependsOn(gcpUtils)

lazy val pubsubDistroless = project
  .in(file("modules/distroless/pubsub"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)
  .settings(sourceDirectory := (pubsub / sourceDirectory).value)
  .settings(pubsubDistrolessBuildSettings)
  .settings(libraryDependencies ++= pubsubDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(core)
  .dependsOn(gcpUtils)

lazy val kinesis = project
  .in(file("modules/kinesis"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(kinesisBuildSettings)
  .settings(libraryDependencies ++= kinesisDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(core)
  .dependsOn(awsUtils)

lazy val kinesisDistroless = project
  .in(file("modules/distroless/kinesis"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)
  .settings(sourceDirectory := (kinesis / sourceDirectory).value)
  .settings(kinesisDistrolessBuildSettings)
  .settings(libraryDependencies ++= kinesisDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(core)
  .dependsOn(awsUtils)

lazy val kafka = project
  .in(file("modules/kafka"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(kafkaBuildSettings)
  .settings(libraryDependencies ++= kafkaDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(core)
  .dependsOn(azureUtils)

lazy val kafkaDistroless = project
  .in(file("modules/distroless/kafka"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)
  .settings(sourceDirectory := (kafka / sourceDirectory).value)
  .settings(kafkaDistrolessBuildSettings)
  .settings(libraryDependencies ++= kafkaDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(core)
  .dependsOn(azureUtils)

lazy val nsq = project
  .in(file("modules/nsq"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)
  .settings(nsqBuildSettings)
  .settings(libraryDependencies ++= nsqDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(core)
  .dependsOn(azureUtils)
  .dependsOn(awsUtils)
  .dependsOn(gcpUtils)

lazy val nsqDistroless = project
  .in(file("modules/distroless/nsq"))
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)
  .settings(sourceDirectory := (nsq / sourceDirectory).value)
  .settings(nsqDistrolessBuildSettings)
  .settings(libraryDependencies ++= nsqDependencies)
  .settings(excludeDependencies ++= exclusions)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(common % "compile->compile;test->test")
  .dependsOn(core)
  .dependsOn(azureUtils)
  .dependsOn(awsUtils)
  .dependsOn(gcpUtils)

lazy val itCore = project
  .in(file("modules/it/core"))
  .settings(itSettings)
  .settings(libraryDependencies ++= itDependencies)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(core)
  
lazy val itKinesis = project
  .in(file("modules/it/kinesis"))
  .settings(itSettings)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(kinesis)
  .dependsOn(itCore % "test->test")
  .settings((Test / test) := (Test / test).dependsOn(kinesisDistroless / Docker / publishLocal).value)
  .settings((Test / testOnly) := (Test / testOnly).dependsOn(kinesisDistroless / Docker / publishLocal).evaluated)

lazy val itKafka = project
  .in(file("modules/it/kafka"))
  .settings(itSettings)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(kafka)
  .dependsOn(itCore % "test->test")
  .settings((Test / test) := (Test / test).dependsOn(kafkaDistroless / Docker / publishLocal).value)
  .settings((Test / testOnly) := (Test / testOnly).dependsOn(kafkaDistroless / Docker / publishLocal).evaluated)

lazy val itNsq = project
  .in(file("modules/it/nsq"))
  .settings(itSettings)
  .settings(addCompilerPlugin(betterMonadicFor))
  .dependsOn(nsq)
  .dependsOn(itCore % "test->test")
  .settings((Test / test) := (Test / test).dependsOn(nsqDistroless / Docker / publishLocal).value)
  .settings((Test / testOnly) := (Test / testOnly).dependsOn(nsqDistroless / Docker / publishLocal).evaluated)