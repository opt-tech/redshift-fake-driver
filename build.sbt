import Deps._
import Helpers._

scalaVersion := "2.11.12"

crossScalaVersions := Seq("2.11.12", "2.12.8")

name := "redshift-fake-driver"

licenses += "Apache 2" -> url("https://raw.githubusercontent.com/opt-tech/redshift-fake-driver/master/LICENSE")

organization := "jp.ne.opt"

version := "1.0.14-SNAPSHOT"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature"
)

libraryDependencies ++= (compileScope(jawn, jsqlparser, scalaCsv, commonsCompress) ++
  compileScope(parser) ++
  testScope(postgres, h2, s3, sts, scalatest, s3Proxy) ++
  providedScope(postgres, h2, s3, sts))

publishMavenStyle := true
