import Deps._
import Helpers._

val scala210 = "2.10.6"

scalaVersion := scala210

crossScalaVersions := Seq(scala210, "2.11.8")

name := "redshift-fake-driver"

organization := "jp.ne.opt"

version := "0.0.1-SNAPSHOT"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature"
)

libraryDependencies ++= (compileScope(jawn, scalaCsv) ++
  (if (scalaVersion.value.startsWith("2.10")) Nil else compileScope(parser)) ++
  testScope(postgres, h2, s3, scalatest) ++
  providedScope(postgres, h2, s3))

publishMavenStyle := true
