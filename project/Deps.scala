import sbt._

object Deps {
  val parser = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
  val scalaCsv = "com.github.tototoshi" %% "scala-csv" % "1.3.4"
  val postgres = "org.postgresql" % "postgresql" % "9.4.1211"
  val h2 = "com.h2database" % "h2" % "1.4.193"
  val s3 = "com.amazonaws" % "aws-java-sdk-s3" % "1.11.43"
  val jawn = "org.spire-math" %% "jawn-ast" % "0.10.4"
  val scalatest = "org.scalatest" %% "scalatest" % "3.0.0"
}
