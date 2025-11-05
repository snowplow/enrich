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

import sbt._
import sbt.Keys._
import sbt.nio.Keys.{ReloadOnSourceChanges, onChangedBuildSource}
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtdynver.DynVerPlugin.autoImport._
import com.typesafe.sbt.SbtNativePackager.autoImport._
import com.typesafe.sbt.packager.archetypes.jar.LauncherJarPlugin.autoImport.packageJavaLauncherJar
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._

object BuildSettings {

  // PROJECT

  lazy val projectSettings = Seq(
    organization := "com.snowplowanalytics",
    scalaVersion := "2.12.20",
    scalacOptions ++= Seq(
      "-Ywarn-macros:after",
      "-Wconf:msg=discarding unmoored doc comment:silent" // so we can put scaladoc on nested functions
    ),
    licenses += ("SLULA-1.1", url("https://docs.snowplow.io/limited-use-license-1.1"))
  )

  lazy val commonProjectSettings = projectSettings ++ Seq(
    name := "snowplow-common-enrich",
    moduleName := "snowplow-common-enrich",
    description := "Common functionality for enriching raw Snowplow events"
  )

  lazy val coreProjectSettings = projectSettings ++ Seq(
    name := "snowplow-enrich-core",
    moduleName := "snowplow-enrich-core",
    description := "Core library to build Enrich apps, based on common-streams"
  )

  lazy val cloudUtilsProjectSettings = projectSettings ++ Seq(
    name := "snowplow-enrich-cloud-utils",
    moduleName := "snowplow-enrich-cloud-utils",
    description := "Cloud utils interfaces"
  )

  lazy val awsUtilsProjectSettings = projectSettings ++ Seq(
    name := "snowplow-enrich-aws-utils",
    moduleName := "snowplow-enrich-aws-utils",
    description := "AWS specific utils"
  )

  lazy val gcpUtilsProjectSettings = projectSettings ++ Seq(
    name := "snowplow-enrich-gcp-utils",
    moduleName := "snowplow-enrich-gcp-utils",
    description := "GCP specific utils"
  )

  lazy val azureUtilsProjectSettings = projectSettings ++ Seq(
    name := "snowplow-enrich-azure-utils",
    moduleName := "snowplow-enrich-azure-utils",
    description := "Azure specific utils"
  )

  lazy val pubsubProjectSettings = projectSettings ++ Seq(
    name := "snowplow-enrich-pubsub",
    moduleName := "snowplow-enrich-pubsub",
    description := "Enrich application for Pubsub",
    buildInfoOptions += BuildInfoOption.Traits("com.snowplowanalytics.snowplow.runtime.AppInfo"),
    buildInfoKeys := Seq[BuildInfoKey](name, version, dockerAlias, BuildInfoKey("cloud" -> "GCP")),
    buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.pubsub"
  )

  lazy val kinesisProjectSettings = projectSettings ++ Seq(
    name := "snowplow-enrich-kinesis",
    moduleName := "snowplow-enrich-kinesis",
    description := "Enrich application for Kinesis",
    buildInfoOptions += BuildInfoOption.Traits("com.snowplowanalytics.snowplow.runtime.AppInfo"),
    buildInfoKeys := Seq[BuildInfoKey](name, version, dockerAlias, BuildInfoKey("cloud" -> "AWS")),
    buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.kinesis"
  )

  lazy val kafkaProjectSettings = projectSettings ++ Seq(
    name := "snowplow-enrich-kafka",
    moduleName := "snowplow-enrich-kafka",
    description := "Enrich application for Kafka",
    buildInfoOptions += BuildInfoOption.Traits("com.snowplowanalytics.snowplow.runtime.AppInfo"),
    buildInfoKeys := Seq[BuildInfoKey](name, version, dockerAlias, BuildInfoKey("cloud" -> "Azure")),
    buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.kafka"
  )

  lazy val nsqProjectSettings = projectSettings ++ Seq(
    name := "snowplow-enrich-nsq",
    moduleName := "snowplow-enrich-nsq",
    description := "Enrich application for NSQ",
    buildInfoOptions += BuildInfoOption.Traits("com.snowplowanalytics.snowplow.runtime.AppInfo"),
    buildInfoKeys := Seq[BuildInfoKey](name, version, dockerAlias, BuildInfoKey("cloud" -> "AWS, Azure, GCP")),
    buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.nsq"
  )

  /** Make package (build) metadata available within source code. */
  lazy val scalifiedSettings = Seq(
    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "settings.scala"
      IO.write(
        file,
        """package com.snowplowanalytics.snowplow.enrich.common.generated
                       |object ProjectSettings {
                       |  val version = "%s"
                       |  val name = "%s"
                       |  val organization = "%s"
                       |  val scalaVersion = "%s"
                       |}
                       |""".stripMargin.format(version.value, name.value, organization.value, scalaVersion.value)
      )
      Seq(file)
    }.taskValue
  )

  lazy val compilerSettings = Seq(
    javacOptions := Seq("-source", "21", "-target", "21")
  )

  lazy val resolverSettings = Seq(
    resolvers ++= Dependencies.resolutionRepos
  )

  lazy val formattingSettings = Seq(
    scalafmtConfig := file(".scalafmt.conf"),
    scalafmtOnCompile := false
  )

  // BUILD AND PUBLISH

  /** Snowplow Common Enrich Maven publishing settings. */
  lazy val publishSettings = Seq(
    publishArtifact := true,
    Test / publishArtifact := false,
    pomIncludeRepository := { _ => false },
    homepage := Some(url("http://snowplowanalytics.com")),
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags are required to have v-prefix
    ThisBuild / dynverSeparator := "-", // to be compatible with Docker
    developers := List(
      Developer(
        "Snowplow Analytics Ltd",
        "Snowplow Analytics Ltd",
        "support@snowplowanalytics.com",
        url("https://snowplowanalytics.com")
      )
    )
  )

  lazy val dockerSettingsFocal = Seq(
    Universal / javaOptions ++= Seq("-Dnashorn.args=--language=es6")
  )

  lazy val dockerSettingsDistroless = Seq(
    dockerEntrypoint := {
      val orig = dockerEntrypoint.value
      orig.head +: "-Dnashorn.args=--language=es6" +: orig.tail
    }
  )

  // TESTS

  lazy val noParallelTestExecution = Seq(Test / parallelExecution := false)

  /** Add example config for integration tests. */
  lazy val addExampleConfToTestCp = Seq(
    Test / unmanagedClasspath += {
      baseDirectory.value.getParentFile.getParentFile / "config"
    }
  )

  lazy val buildSettings = Seq(
    Global / cancelable := true,
    Global / onChangedBuildSource := ReloadOnSourceChanges
  ) ++ compilerSettings ++ resolverSettings ++ formattingSettings

  lazy val commonBuildSettings = {
    // Project
    commonProjectSettings ++ buildSettings ++ scalifiedSettings ++
      // Build and publish
      publishSettings ++
      // Tests
      noParallelTestExecution ++ Seq(
      Test / fork := true,
      Test / javaOptions := Seq("-Dnashorn.args=--language=es6")
    )
  }

  lazy val coreBuildSettings = {
    // Project
    coreProjectSettings ++ buildSettings ++
      // Tests
      noParallelTestExecution ++ addExampleConfToTestCp ++ Seq(
      Test / fork := true,
      Test / javaOptions := Seq("-Dnashorn.args=--language=es6")
    )
  }

  lazy val cloudUtilsBuildSettings = {
    // Project
    cloudUtilsProjectSettings ++ buildSettings ++
      // Tests
      noParallelTestExecution ++ addExampleConfToTestCp
  }

  lazy val awsUtilsBuildSettings = {
    // Project
    awsUtilsProjectSettings ++ buildSettings ++
      // Tests
      noParallelTestExecution ++ addExampleConfToTestCp
  }

  lazy val gcpUtilsBuildSettings = {
    // Project
    gcpUtilsProjectSettings ++ buildSettings ++
      // Tests
      noParallelTestExecution ++ addExampleConfToTestCp
  }

  lazy val azureUtilsBuildSettings = {
    // Project
    azureUtilsProjectSettings ++ buildSettings ++
      // Tests
      noParallelTestExecution ++ addExampleConfToTestCp
  }

  lazy val pubsubBuildSettings = {
    // Project
    pubsubProjectSettings ++ buildSettings ++
      // Build and publish
      dockerSettingsFocal ++
      Seq(Docker / packageName := "snowplow-enrich-pubsub") ++
      // Tests
      noParallelTestExecution ++ addExampleConfToTestCp
  }

  lazy val pubsubDistrolessBuildSettings = pubsubBuildSettings.diff(dockerSettingsFocal) ++ dockerSettingsDistroless

  lazy val kinesisBuildSettings = {
    // Project
    kinesisProjectSettings ++ buildSettings ++
      // Build and publish
      dockerSettingsFocal ++
      Seq(Docker / packageName := "snowplow-enrich-kinesis") ++
      // Tests
      noParallelTestExecution ++ Seq(Test / fork := true) ++ addExampleConfToTestCp ++ Seq(
      // used in configuration parsing unit tests
      Test / envVars := Map(
        "HOSTNAME" -> "testWorkerId"
      )
    )
  }

  lazy val kinesisDistrolessBuildSettings = kinesisBuildSettings.diff(dockerSettingsFocal) ++ dockerSettingsDistroless

  lazy val kafkaBuildSettings = {
    // Project
    kafkaProjectSettings ++ buildSettings ++
      // Build and publish
      dockerSettingsFocal ++
      Seq(Docker / packageName := "snowplow-enrich-kafka") ++
      // Tests
      noParallelTestExecution ++ addExampleConfToTestCp
  }

  lazy val kafkaDistrolessBuildSettings = kafkaBuildSettings.diff(dockerSettingsFocal) ++ dockerSettingsDistroless

  lazy val nsqBuildSettings = {
    // Project
    nsqProjectSettings ++ buildSettings ++
      // Build and publish
      dockerSettingsFocal ++
      Seq(Docker / packageName := "snowplow-enrich-nsq") ++
      // Tests
      noParallelTestExecution ++ addExampleConfToTestCp
  }

  lazy val nsqDistrolessBuildSettings = nsqBuildSettings.diff(dockerSettingsFocal) ++ dockerSettingsDistroless

  lazy val itSettings = Seq(publish / skip := true)

  /** Fork a JVM per test in order to not reuse enrichment registries. */
  def oneJVMPerTest(tests: Seq[TestDefinition]): Seq[Tests.Group] =
    tests.map(t => Tests.Group(t.name, Seq(t), Tests.SubProcess(ForkOptions())))
}
