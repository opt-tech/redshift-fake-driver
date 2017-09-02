import Deps._
import Helpers._

val scala210 = "2.10.6"

scalaVersion := scala210

crossScalaVersions := Seq(scala210, "2.11.8", "2.12.3")

name := "redshift-fake-driver"

licenses += "Apache 2" -> url("https://raw.githubusercontent.com/opt-tech/redshift-fake-driver/master/LICENSE")

organization := "jp.ne.opt"

version := "1.0.3"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature"
)

libraryDependencies ++= (compileScope(jawn, jsqlparser, scalaCsv) ++
  (if (scalaVersion.value.startsWith("2.10")) Nil else compileScope(parser)) ++
  testScope(postgres, h2, s3, scalatest) ++
  providedScope(postgres, h2, s3))

publishMavenStyle := true
