package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake._
import jp.ne.opt.redshiftfake.s3.S3Location
import org.scalatest.FlatSpec

class CopyCommandParserTest extends FlatSpec {
  it should "parse minimal COPY command" in {
    val command =
      s"""
        |COPY public._foo_42 FROM '${Global.s3Scheme}some-bucket/path/to/data/foo-bar.csv'
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
      timeFormatType = TimeFormatType.Default,
      emptyAsNull = false,
      delimiter = '|',
      nullAs = "\u000e"
    )

    assert(CopyCommandParser.parse(command) == Some(expected))
  }

  it should "parse JSON format" in {
    val command =
      s"""
        |COPY public._foo_42 FROM '${Global.s3Scheme}some-bucket/path/to/data/foo-bar.csv'
        |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
        |JSON;
        |""".stripMargin

    val commandWithJsonpath =
      s"""
        |COPY public._foo_42 FROM '${Global.s3Scheme}some-bucket/path/to/data/foo-bar.csv'
        |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
        |JSON '${Global.s3Scheme}some-bucket/path/to/jsonpaths.txt';
        |""".stripMargin

    assert(CopyCommandParser.parse(command).map(_.copyFormat) == Some(CopyFormat.Json(None)))
    assert(CopyCommandParser.parse(commandWithJsonpath).map(_.copyFormat) == Some(CopyFormat.Json(Some(S3Location("some-bucket", "path/to/jsonpaths.txt")))))
  }

  it should "parse date format" in {
    val auto =
      s"""
         |COPY public._foo_42 FROM '${Global.s3Scheme}some-bucket/path/to/data/foo-bar.csv'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |DATEFORMAT 'auto';
         |""".stripMargin

    val custom =
      s"""
         |COPY public._foo_42 FROM '${Global.s3Scheme}some-bucket/path/to/data/foo-bar.csv'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |DATEFORMAT 'YYYY/MM/DD';
         |""".stripMargin

    assert(CopyCommandParser.parse(auto).map(_.dateFormatType) == Some(DateFormatType.Auto))
    assert(CopyCommandParser.parse(custom).map(_.dateFormatType) == Some(DateFormatType.Custom("YYYY/MM/DD")))
  }

  it should "parse time format" in {
    val auto =
      s"""
         |COPY public._foo_42 FROM '${Global.s3Scheme}some-bucket/path/to/data/foo-bar.csv'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |TIMEFORMAT 'auto';
         |""".stripMargin

    val epochsecs =
      s"""
         |COPY public._foo_42 FROM '${Global.s3Scheme}some-bucket/path/to/data/foo-bar.csv'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |TIMEFORMAT 'epochsecs';
         |""".stripMargin

    val epochmillisecs =
      s"""
         |COPY public._foo_42 FROM '${Global.s3Scheme}some-bucket/path/to/data/foo-bar.csv'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |TIMEFORMAT 'epochmillisecs';
         |""".stripMargin

    val custom =
      s"""
         |COPY public._foo_42 FROM '${Global.s3Scheme}some-bucket/path/to/data/foo-bar.csv'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |TIMEFORMAT 'YYYY/MM/DD HH:MI:SS.SSS';
         |""".stripMargin

    assert(CopyCommandParser.parse(auto).map(_.timeFormatType) == Some(TimeFormatType.Auto))
    assert(CopyCommandParser.parse(epochsecs).map(_.timeFormatType) == Some(TimeFormatType.Epochsecs))
    assert(CopyCommandParser.parse(epochmillisecs).map(_.timeFormatType) == Some(TimeFormatType.EpochMillisecs))
    assert(CopyCommandParser.parse(custom).map(_.timeFormatType) == Some(TimeFormatType.Custom("YYYY/MM/DD HH:MI:SS.SSS")))
  }

  it should "parse manifest" in {
    val command =
      s"""
         |COPY public._foo_42 FROM '${Global.s3Scheme}some-bucket/path/to/unloaded_manifest'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |MANIFEST;
         |""".stripMargin

    assert(CopyCommandParser.parse(command).map(_.copyFormat) == Some(CopyFormat.Manifest(S3Location("some-bucket", "path/to/unloaded_manifest"))))
  }

  it should "parse emptyAsNull" in {
    val command =
      s"""
         |COPY public._foo_42 FROM '${Global.s3Scheme}some-bucket/path/to/unloaded_manifest'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |EMPTYASNULL;
         |""".stripMargin

    assert(CopyCommandParser.parse(command).map(_.emptyAsNull) == Some(true))
  }

  it should "parse quoted schema and table names" in {
    val command =
      s"""
         |COPY "public"."mytable"
         |FROM '${Global.s3Scheme}some-bucket/path/to/unloaded_manifest.json'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |manifest
         |""".stripMargin

    assert(CopyCommandParser.parse(command).map(_.schemaName) == Some(Some("public")))
    assert(CopyCommandParser.parse(command).map(_.tableName) == Some("mytable"))
  }

  it should "parse delimiter from copy command" in {
    val command =
      s"""
         |COPY "public"."mytable"
         |FROM '${Global.s3Scheme}some-bucket/path/to/unloaded_manifest.json'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |DELIMITER AS ','
         |manifest
         |""".stripMargin

    assert(CopyCommandParser.parse(command).map(_.delimiter) == Some(','))
  }

  it should "set default delimiter correctly" in {
    val command =
      s"""
         |COPY "public"."mytable"
         |FROM '${Global.s3Scheme}some-bucket/path/to/unloaded_manifest.json'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |FORMAT AS CSV
         |manifest
         |""".stripMargin

    assert(CopyCommandParser.parse(command).map(_.delimiter) == Some('|'))
  }

  it should "parse 'null as' from copy command" in {
    val command =
      s"""
         |COPY "public"."mytable"
         |FROM '${Global.s3Scheme}some-bucket/path/to/unloaded_manifest.json'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |NULL AS '@NULL@'
         |manifest
         |""".stripMargin

    assert(CopyCommandParser.parse(command).map(_.nullAs) == Some("@NULL@"))
  }

  it should "set default 'null as' correctly" in {
    val command =
      s"""
         |COPY "public"."mytable"
         |FROM '${Global.s3Scheme}some-bucket/path/to/unloaded_manifest.json'
         |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
         |FORMAT AS CSV
         |manifest
         |""".stripMargin

    assert(CopyCommandParser.parse(command).map(_.nullAs) == Some("\u000e"))
  }
}
