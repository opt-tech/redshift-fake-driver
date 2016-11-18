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
      copyFormat = CopyFormat.Default,
      dateFormatType = DateFormatType.Default,
      timeFormatType = TimeFormatType.Default
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

  it should "parse date format" in {
    val auto =
      s"""
         |COPY public._foo_42 FROM '${Global.s3Endpoint}some-bucket/path/to/data/foo-bar.csv'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |DATEFORMAT 'auto';
         |""".stripMargin

    val custom =
      s"""
         |COPY public._foo_42 FROM '${Global.s3Endpoint}some-bucket/path/to/data/foo-bar.csv'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |DATEFORMAT 'YYYY/MM/DD';
         |""".stripMargin

    assert(CopyCommandParser.parse(auto).map(_.dateFormatType).contains(DateFormatType.Auto))
    assert(CopyCommandParser.parse(custom).map(_.dateFormatType).contains(DateFormatType.Custom("YYYY/MM/DD")))
  }

  it should "parse time format" in {
    val auto =
      s"""
         |COPY public._foo_42 FROM '${Global.s3Endpoint}some-bucket/path/to/data/foo-bar.csv'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |TIMEFORMAT 'auto';
         |""".stripMargin

    val epochsecs =
      s"""
         |COPY public._foo_42 FROM '${Global.s3Endpoint}some-bucket/path/to/data/foo-bar.csv'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |TIMEFORMAT 'epochsecs';
         |""".stripMargin

    val epochmillisecs =
      s"""
         |COPY public._foo_42 FROM '${Global.s3Endpoint}some-bucket/path/to/data/foo-bar.csv'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |TIMEFORMAT 'epochmillisecs';
         |""".stripMargin

    val custom =
      s"""
         |COPY public._foo_42 FROM '${Global.s3Endpoint}some-bucket/path/to/data/foo-bar.csv'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |TIMEFORMAT 'YYYY/MM/DD HH:MI:SS.SSS';
         |""".stripMargin

    assert(CopyCommandParser.parse(auto).map(_.timeFormatType).contains(TimeFormatType.Auto))
    assert(CopyCommandParser.parse(epochsecs).map(_.timeFormatType).contains(TimeFormatType.Epochsecs))
    assert(CopyCommandParser.parse(epochmillisecs).map(_.timeFormatType).contains(TimeFormatType.EpochMillisecs))
    assert(CopyCommandParser.parse(custom).map(_.timeFormatType).contains(TimeFormatType.Custom("YYYY/MM/DD HH:MI:SS.SSS")))
  }
}
