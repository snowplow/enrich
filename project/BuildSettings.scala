/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the
 * Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.  See the Apache License Version 2.0 for the specific
 * language governing permissions and limitations there under.
 */

// SBT
import sbt._
import Keys._

import sbtdynver.DynVerPlugin.autoImport._

import com.typesafe.sbt.SbtNativePackager.autoImport._
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.{ DockerVersion, ExecCmd }

import scoverage.ScoverageKeys._

import org.scalafmt.sbt.ScalafmtPlugin.autoImport._

object BuildSettings {

  /** Common base settings */
  lazy val basicSettings = Seq(
    organization          :=  "com.snowplowanalytics",
    scalaVersion          :=  "2.12.11",
    javacOptions          :=  Seq("-source", "11", "-target", "11"),
    resolvers             ++= Dependencies.resolutionRepos,
    licenses              += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  )

  /** Custom sbt-buildinfo replacement, used by SCE only */
  lazy val scalifySettings = Seq(
    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "settings.scala"
      IO.write(file, """package com.snowplowanalytics.snowplow.enrich.common.generated
        |object ProjectSettings {
        |  val version = "%s"
        |  val name = "%s"
        |  val organization = "%s"
        |  val scalaVersion = "%s"
        |}
        |""".stripMargin.format(version.value, name.value, organization.value, scalaVersion.value))
      Seq(file)
    }.taskValue
  )

  /** Snowplow Common Enrich Maven publishing settings */
  lazy val publishSettings = Seq(
    publishArtifact := true,
    Test / publishArtifact := false,
    pomIncludeRepository := { _ => false },
    homepage := Some(url("http://snowplowanalytics.com")),
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / dynverSeparator := "-", // to be compatible with docker
    developers := List(
      Developer(
        "Snowplow Analytics Ltd",
        "Snowplow Analytics Ltd",
        "support@snowplowanalytics.com",
        url("https://snowplowanalytics.com")
      )
    )
  )

  lazy val formatting = Seq(
    scalafmtConfig    := file(".scalafmt.conf"),
    scalafmtOnCompile := false
  )

  lazy val scoverageSettings = Seq(
    coverageMinimum := 50,
    coverageFailOnMinimum := false,
    (Test / test) := {
      (coverageReport dependsOn (Test / test)).value
    }
  )

  /** Fork a JVM per test in order to not reuse enrichment registries */
  def oneJVMPerTest(tests: Seq[TestDefinition]): Seq[Tests.Group] =
    tests.map(t => Tests.Group(t.name, Seq(t), Tests.SubProcess(ForkOptions())))

  /** sbt-assembly settings for building a fat jar */
  import sbtassembly.AssemblyPlugin.autoImport._
  lazy val sbtAssemblySettings = Seq(
    assembly / assemblyJarName := { s"${moduleName.value}-${version.value}.jar" },
    assembly / assemblyMergeStrategy := {
      case x if x.endsWith(".properties") => MergeStrategy.first
      case x if x.endsWith("public-suffix-list.txt") => MergeStrategy.first
      case x if x.endsWith("ProjectSettings$.class") => MergeStrategy.first
      case x if x.endsWith("module-info.class") => MergeStrategy.first
      case x if x.endsWith("nowarn.class") => MergeStrategy.first
      case x if x.endsWith(".proto") => MergeStrategy.first
      case x if x.endsWith(".json") => MergeStrategy.first
      case x if x.endsWith("AUTHORS") => MergeStrategy.first
      case x if x.endsWith(".config") => MergeStrategy.first
      case x if x.endsWith(".types") => MergeStrategy.first
      case x if x.contains("netty") => MergeStrategy.first
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )

  /** Add example config for integration tests */
  lazy val addExampleConfToTestCp = Seq(
    Test / unmanagedClasspath += {
      baseDirectory.value.getParentFile.getParentFile / "config"
    }
  )

  /** Docker settings, used by SE */
  lazy val dockerSettings = Seq(
    Docker / maintainer := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    dockerBaseImage := "snowplow/base-debian:0.2.1",
    Docker / daemonUser := "snowplow",
    dockerUpdateLatest := true,
    dockerVersion := Some(DockerVersion(18, 9, 0, Some("ce"))),
    Docker / daemonUserUid := None,
    Docker / defaultLinuxInstallLocation := "/home/snowplow" // must be home directory of daemonUser
  )

  /** Docker settings, used by BE */
  lazy val dataflowDockerSettings = Seq(
    Docker / maintainer := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    dockerBaseImage := "snowplow/k8s-dataflow:0.2.0",
    dockerEnvVars := Map("HOME" -> "/tmp", "JAVA_OPTS" -> "-Duser.home=$HOME"),
    Docker / daemonUser := "snowplow",
    dockerUpdateLatest := true,
    dockerVersion := Some(DockerVersion(18, 9, 0, Some("ce"))),
    dockerCommands := dockerCommands.value.map {
      case ExecCmd("ENTRYPOINT", args) => ExecCmd("ENTRYPOINT", "docker-entrypoint.sh", args)
      case e => e
    }
  )
}
