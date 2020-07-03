/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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

import bintray.BintrayPlugin._
import bintray.BintrayKeys._

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
    version               :=  "1.3.0-rc9",
    javacOptions          :=  Seq("-source", "1.8", "-target", "1.8"),
    resolvers             ++= Dependencies.resolutionRepos
  )

  /** Custom sbt-buildinfo replacement, used by SCE only */
  lazy val scalifySettings = Seq(
    sourceGenerators in Compile += Def.task {
      val file = (sourceManaged in Compile).value / "settings.scala"
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
  lazy val publishSettings = bintraySettings ++ Seq(
    publishMavenStyle := true,
    publishArtifact := true,
    publishArtifact in Test := false,
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),
    bintrayOrganization := Some("snowplow"),
    bintrayRepository := "snowplow-maven",
    pomIncludeRepository := { _ => false },
    homepage := Some(url("http://snowplowanalytics.com")),
    scmInfo := Some(ScmInfo(url("https://github.com/snowplow/snowplow"),
      "scm:git@github.com:snowplow/snowplow.git")),
    pomExtra := (
      <developers>
        <developer>
          <name>Snowplow Analytics Ltd</name>
          <email>support@snowplowanalytics.com</email>
          <organization>Snowplow Analytics Ltd</organization>
          <organizationUrl>http://snowplowanalytics.com</organizationUrl>
        </developer>
      </developers>)
  )

  lazy val formatting = Seq(
    scalafmtConfig    := file(".scalafmt.conf"),
    scalafmtOnCompile := true
  )

  lazy val scoverageSettings = Seq(
    coverageMinimum := 50,
    coverageFailOnMinimum := false,
    (test in Test) := {
      (coverageReport dependsOn (test in Test)).value
    }
  )

  /** Fork a JVM per test in order to not reuse enrichment registries */
  def oneJVMPerTest(tests: Seq[TestDefinition]): Seq[Tests.Group] =
    tests.map(t => Tests.Group(t.name, Seq(t), Tests.SubProcess(ForkOptions())))

  /** sbt-assembly settings for building a fat jar */
  import sbtassembly.AssemblyPlugin.autoImport._
  lazy val sbtAssemblySettings = Seq(
    assemblyJarName in assembly := { s"${moduleName.value}-${version.value}.jar" },
    assemblyMergeStrategy in assembly := {
      case x if x.endsWith("ProjectSettings$.class") => MergeStrategy.first
      case x if x.endsWith("module-info.class") => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )

  /** Add example config for integration tests */
  lazy val addExampleConfToTestCp = Seq(
    unmanagedClasspath in Test += {
      baseDirectory.value.getParentFile.getParentFile / "config"
    }
  )

  /** Docker settings, used by SE */
  lazy val dockerSettings = Seq(
    maintainer in Docker := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    dockerBaseImage := "snowplow-docker-registry.bintray.io/snowplow/base-debian:0.1.0",
    daemonUser in Docker := "snowplow",
    dockerUpdateLatest := true,
    dockerVersion := Some(DockerVersion(18, 9, 0, Some("ce"))),
    daemonUserUid in Docker := None,
    defaultLinuxInstallLocation in Docker := "/home/snowplow" // must be home directory of daemonUser
  )

  /** Docker settings, used by BE */
  lazy val dataflowDockerSettings = Seq(
    maintainer in Docker := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    dockerBaseImage := "snowplow-docker-registry.bintray.io/snowplow/k8s-dataflow:0.1.1",
    daemonUser in Docker := "snowplow",
    dockerUpdateLatest := true,
    dockerVersion := Some(DockerVersion(18, 9, 0, Some("ce"))),
    dockerCommands := dockerCommands.value.map {
      case ExecCmd("ENTRYPOINT", args) => ExecCmd("ENTRYPOINT", "docker-entrypoint.sh", args)
      case e => e
    }
  )
}
