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
import sbtbuildinfo.BuildInfoPlugin.autoImport.{BuildInfoKey, buildInfoKeys, buildInfoPackage}
import sbtdynver.DynVerPlugin.autoImport._
import com.typesafe.sbt.SbtNativePackager.autoImport._
import com.typesafe.sbt.packager.archetypes.jar.LauncherJarPlugin.autoImport.packageJavaLauncherJar
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import scoverage.ScoverageKeys._

object BuildSettings {

  // PROJECT

  lazy val projectSettings = Seq(
    organization := "com.snowplowanalytics",
    scalaVersion := "2.12.15",
    licenses += ("SLULA-1.1", url("https://docs.snowplow.io/limited-use-license-1.1"))
  )

  lazy val commonProjectSettings = projectSettings ++ Seq(
    name := "snowplow-common-enrich",
    moduleName := "snowplow-common-enrich",
    description := "Common functionality for enriching raw Snowplow events"
  )

  lazy val commonFs2ProjectSettings = projectSettings ++ Seq(
    name := "snowplow-enrich-common-fs2",
    moduleName := "snowplow-enrich-common-fs2",
    description := "Common functionality for streaming enrich applications built on top of functional streams"
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
    description := "High-performance streaming enrich app working with Pub/Sub, built on top of functional streams",
    buildInfoKeys := Seq[BuildInfoKey](organization, name, version, description),
    buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.pubsub.generated"
  )

  lazy val kinesisProjectSettings = projectSettings ++ Seq(
    name := "snowplow-enrich-kinesis",
    moduleName := "snowplow-enrich-kinesis",
    description := "High-performance streaming enrich app working with Kinesis, built on top of functional streams",
    buildInfoKeys := Seq[BuildInfoKey](organization, name, version, description),
    buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.kinesis.generated"
  )

  lazy val kafkaProjectSettings = projectSettings ++ Seq(
    name := "snowplow-enrich-kafka",
    moduleName := "snowplow-enrich-kafka",
    description := "High-performance streaming enrich app working with Kafka, built on top of functional streams",
    buildInfoKeys := Seq[BuildInfoKey](organization, name, version, description),
    buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.kafka.generated"
  )

  lazy val nsqProjectSettings = projectSettings ++ Seq(
    name := "snowplow-enrich-nsq",
    moduleName := "snowplow-enrich-nsq",
    description := "High-performance streaming enrich app working with Nsq, built on top of functional streams",
    buildInfoKeys := Seq[BuildInfoKey](organization, name, version, description),
    buildInfoPackage := "com.snowplowanalytics.snowplow.enrich.nsq.generated"
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
    javacOptions := Seq("-source", "11", "-target", "11")
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

  lazy val scoverageSettings = Seq(
    coverageMinimumStmtTotal := 50,
    coverageFailOnMinimum := false,
    (Test / test) := {
      (coverageReport dependsOn (Test / test)).value
    }
  )

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
    scoverageSettings ++ noParallelTestExecution ++ Seq(
      Test / fork := true,
      Test / javaOptions := Seq("-Dnashorn.args=--language=es6")
    )
  }

  lazy val commonFs2BuildSettings = {
    // Project
    commonFs2ProjectSettings ++ buildSettings ++
    // Tests
    scoverageSettings ++ noParallelTestExecution ++ addExampleConfToTestCp ++ Seq(
      Test / fork := true,
      Test / javaOptions := Seq("-Dnashorn.args=--language=es6")
    )
  }

  lazy val awsUtilsBuildSettings = {
    // Project
    awsUtilsProjectSettings ++ buildSettings ++
    // Tests
    scoverageSettings ++ noParallelTestExecution ++ addExampleConfToTestCp
  }

  lazy val gcpUtilsBuildSettings = {
    // Project
    gcpUtilsProjectSettings ++ buildSettings ++
    // Tests
    scoverageSettings ++ noParallelTestExecution ++ addExampleConfToTestCp
  }

  lazy val azureUtilsBuildSettings = {
    // Project
    azureUtilsProjectSettings ++ buildSettings ++
    // Tests
    scoverageSettings ++ noParallelTestExecution ++ addExampleConfToTestCp
  }

  lazy val pubsubBuildSettings = {
    // Project
    pubsubProjectSettings ++ buildSettings ++
    // Build and publish
    dockerSettingsFocal ++
      Seq(Docker / packageName := "snowplow-enrich-pubsub") ++
    // Tests
    scoverageSettings ++ noParallelTestExecution ++ addExampleConfToTestCp
  }

  lazy val pubsubDistrolessBuildSettings = pubsubBuildSettings.diff(dockerSettingsFocal) ++ dockerSettingsDistroless

  lazy val kinesisBuildSettings = {
    // Project
    kinesisProjectSettings ++ buildSettings ++
    // Build and publish
    dockerSettingsFocal ++
      Seq(Docker / packageName := "snowplow-enrich-kinesis") ++
    // Tests
    scoverageSettings ++ noParallelTestExecution ++ Seq(Test / fork := true) ++ addExampleConfToTestCp
  }

  lazy val kinesisDistrolessBuildSettings = kinesisBuildSettings.diff(dockerSettingsFocal) ++ dockerSettingsDistroless

  lazy val kafkaBuildSettings = {
    // Project
    kafkaProjectSettings ++ buildSettings ++
    // Build and publish
    dockerSettingsFocal ++
      Seq(Docker / packageName := "snowplow-enrich-kafka") ++
    // Tests
    scoverageSettings ++ noParallelTestExecution ++ addExampleConfToTestCp
  }

  lazy val kafkaDistrolessBuildSettings = kafkaBuildSettings.diff(dockerSettingsFocal) ++ dockerSettingsDistroless

  lazy val nsqBuildSettings = {
    // Project
    nsqProjectSettings ++ buildSettings ++
      // Build and publish
      dockerSettingsFocal ++
      Seq(Docker / packageName := "snowplow-enrich-nsq") ++
      // Tests
      scoverageSettings ++ noParallelTestExecution ++ addExampleConfToTestCp
  }

  lazy val nsqDistrolessBuildSettings = nsqBuildSettings.diff(dockerSettingsFocal) ++ dockerSettingsDistroless

  /** Fork a JVM per test in order to not reuse enrichment registries. */
  def oneJVMPerTest(tests: Seq[TestDefinition]): Seq[Tests.Group] =
    tests.map(t => Tests.Group(t.name, Seq(t), Tests.SubProcess(ForkOptions())))
}
