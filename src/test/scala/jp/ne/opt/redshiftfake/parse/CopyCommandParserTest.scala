package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake._
import jp.ne.opt.redshiftfake.s3.S3Location
import org.scalatest.FlatSpec

class CopyCommandParserTest extends FlatSpec {
  it should "parse minimal COPY command" in {
    val command =
      s"""
        |COPY public._foo_42 FROM '${Global.s3Endpoint}some-bucket/path/to/data/foo-bar.csv'
        |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY';
        |""".stripMargin

    val expected = CopyCommand(
      schemaName = Some("public"),
      tableName = "_foo_42",
      columnList = None,
      dataSource = CopyDataSource.S3(S3Location("some-bucket", "path/to/data/foo-bar.csv")),
      credentials = Credentials.WithKey(
        accessKeyId = "AKIAXXXXXXXXXXXXXXX",
        secretAccessKey = "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
      ),
      copyFormat = CopyFormat.Default
    )

    assert(CopyCommandParser.parse(command).contains(expected))
  }

  it should "parse JSON format" in {
    val command =
      s"""
        |COPY public._foo_42 FROM '${Global.s3Endpoint}some-bucket/path/to/data/foo-bar.csv'
        |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
        |JSON;
        |""".stripMargin

    val commandWithJsonpath =
      s"""
        |COPY public._foo_42 FROM '${Global.s3Endpoint}some-bucket/path/to/data/foo-bar.csv'
        |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
        |JSON '${Global.s3Endpoint}some-bucket/path/to/jsonpaths.txt';
        |""".stripMargin

    assert(CopyCommandParser.parse(command).map(_.copyFormat).contains(CopyFormat.Json(None)))
    assert(CopyCommandParser.parse(commandWithJsonpath).map(_.copyFormat).contains(CopyFormat.Json(Some(S3Location("some-bucket", "path/to/jsonpaths.txt")))))
  }
}
