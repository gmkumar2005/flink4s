val scala213Version = "2.13.10"
val scala3Version = "3.2.1"

val supportedScalaVersions = List(scala3Version)

lazy val mavenSnapshots = "apache.snapshots" at "https://repository.apache.org/content/groups/snapshots"
ThisBuild/parallelExecution := false
resolvers ++= Seq(mavenSnapshots)

val flinkVersion = "1.16.0"

val flinkLibs = Seq(
   "org.apache.flink" % "flink-streaming-java" % flinkVersion,
   "org.apache.flink" % "flink-core" % flinkVersion,
   "org.apache.flink" % "flink-statebackend-rocksdb" % flinkVersion,
   "org.apache.flink" % "flink-test-utils" % flinkVersion % Test
)

val otherLibs = Seq(
  "org.typelevel" %% "cats-core" % "2.9.0",
    "com.lihaoyi" %% "pprint" % "0.8.1"
)

val testingLibs = Seq(
  "org.scalatest" %% "scalatest" % "3.2.14" % Test,
  "ch.qos.logback"%"logback-classic"% "1.4.5" % Test
)

lazy val root = project
  .in(file("."))
  .settings(
    name := "flink4s",
    scalaVersion := scala3Version,
//    crossScalaVersions := Seq(scala3Version, scala213Version),
    libraryDependencies ++= flinkLibs ++ otherLibs ++ testingLibs,
    publishingSettings
  )

import ReleaseTransformations._

lazy val publishingSettings = Seq(
  organization := "com.ariskk",
  organizationName := "ariskk",
  organizationHomepage := Some(url("https://ariskk.com/")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/ariskk/flink4s"),
      "scm:git@github.com:ariskk/flink4s.git"
    )
  ),
  developers := List(
    Developer(
      id    = "ariskk",
      name  = "Aris Koliopoulos",
      email = "aris@ariskk.com",
      url   = url("https://ariskk.com")
    )
  ),
  description := "Scala 3 wrapper for Apache Flink",
  licenses := List("MIT" -> new URL("https://opensource.org/licenses/MIT")),
  homepage := Some(url("https://github.com/ariskk/flink4s")),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  publishMavenStyle := true,
  releaseProcess := Seq[ReleaseStep](
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    publishArtifacts,
    setNextVersion,
    commitNextVersion,
    ReleaseStep(action = Command.process("sonatypeReleaseAll", _)),
    pushChanges
  )
)