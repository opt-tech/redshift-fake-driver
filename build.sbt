import Deps._
import Helpers._

name := "redshift-fake-driver"

version := "0.0.1"

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature"
)

libraryDependencies ++= (
  compileScope(parser, jawn, scalaCsv) ++ testScope(postgres, h2, s3, scalatest) ++ providedScope(postgres, h2, s3))
