scalaVersion := "2.12.17"

name := "redshift-fake-driver"

licenses += "Apache 2" -> url("https://raw.githubusercontent.com/opt-tech/redshift-fake-driver/master/LICENSE")

version := "1.0.16-SNAPSHOT"

lazy val root = (project in file("."))
  .settings(
    inThisBuild(
      List(
        organization := "jp.ne.opt",
        crossScalaVersions := List("2.12.17", "2.13.10"),
        version := s"${version.value}-SNAPSHOT"
      )
    ),
    name := name.value,
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-parser-combinators" % versions.scala.parser % Compile,
      "com.github.jsqlparser" % "jsqlparser" % versions.jsqlparser % Compile,
      "com.github.tototoshi" %% "scala-csv" % versions.scala.csv % Compile,
      "org.postgresql" % "postgresql" % versions.postgresql % Compile,
      "com.h2database" % "h2" % versions.h2 % Compile,
      "software.amazon.awssdk" % "s3" % versions.aws.sdk % Compile,
      "software.amazon.awssdk" % "sts" % versions.aws.sdk % Compile,
      "org.typelevel" %% "jawn-ast" % versions.jawnast % Compile,
      "org.scalatest" %% "scalatest" % versions.scalatest % Test,
      "com.adobe.testing" % "s3mock" % versions.s3mock % Test,
      "org.apache.commons" % "commons-compress" % versions.compress % Compile,
      "org.flywaydb" % "flyway-community-db-support" % versions.flyway % Compile,
    )
  )

val versions = new {
  val aws = new {
    val sdk = "2.20.17"
  }
  val scala = new {
    val parser = "2.2.0"
    val csv = "1.3.10"
  }
  val h2 = "2.1.214"
  val jsqlparser = "4.6"
  val postgresql = "42.5.4"
  val jawnast = "1.4.0"
  val scalatest = "3.2.15"
  val s3mock = "2.11.0"
  val compress = "1.22"
  val flyway = "9.12.0"
}

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.first
  case x                             => MergeStrategy.first
}

publishMavenStyle := true
