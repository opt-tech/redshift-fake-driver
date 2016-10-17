name := "redshift-local-driver"

version := "1.0"

scalaVersion := "2.11.8"

val postgres = "org.postgresql" % "postgresql" % "9.4.1211"
val s3 = "com.amazonaws" % "aws-java-sdk-s3" % "1.11.43"

libraryDependencies ++= Seq(
  postgres % Test,
  postgres % Provided,

  s3 % Test,
  s3 % Provided,

  "org.scalatest" %% "scalatest" % "3.0.0" % Test
)
