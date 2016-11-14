import Deps._
import Helpers._

name := "redshift-fake-driver"

version := "0.0.1"

scalaVersion := "2.11.8"

libraryDependencies ++=
  (compileScope(parser, jawn) ++ testScope(postgres, h2, s3, scalatest) ++ providedScope(postgres, h2, s3))
