import Deps._
import Helpers._

val scala210 = "2.10.7"

scalaVersion := scala210

crossScalaVersions := Seq(scala210, "2.11.12", "2.12.4")

name := "redshift-fake-driver"

licenses += "Apache 2" -> url("https://raw.githubusercontent.com/opt-tech/redshift-fake-driver/master/LICENSE")

organization := "jp.ne.opt"

version := "1.0.8"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature"
)

libraryDependencies ++= (compileScope(jawn, jsqlparser, scalaCsv) ++
  (if (scalaVersion.value.startsWith("2.10")) Nil else compileScope(parser)) ++
  testScope(postgres, h2, s3, sts, scalatest) ++
  providedScope(postgres, h2, s3, sts))

publishMavenStyle := true
