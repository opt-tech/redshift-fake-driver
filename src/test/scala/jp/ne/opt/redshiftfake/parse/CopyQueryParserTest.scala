package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake.s3.{S3Location, Credentials}
import org.scalatest.FlatSpec

class CopyQueryParserTest extends FlatSpec {
  it should "parse minimal COPY query" in {
    val query =
      """
        |COPY public._foo_42 FROM 's3://some-bucket/path/to/data/foo-bar.csv'
        |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY';
        |""".stripMargin

    val expected = CopyQuery(
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

    assert(CopyQueryParser.parse(query).contains(expected))
  }

  it should "parse JSON format" in {
    val query =
      """
        |COPY public._foo_42 FROM 's3://some-bucket/path/to/data/foo-bar.csv'
        |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
        |JSON;
        |""".stripMargin

    val queryWithJsonpath =
      """
        |COPY public._foo_42 FROM 's3://some-bucket/path/to/data/foo-bar.csv'
        |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
        |JSON 's3://some-bucket/path/to/jsonpaths.txt';
        |""".stripMargin

    assert(CopyQueryParser.parse(query).map(_.copyFormat).contains(CopyFormat.Json(None)))
    assert(CopyQueryParser.parse(queryWithJsonpath).map(_.copyFormat).contains(CopyFormat.Json(Some(S3Location("some-bucket", "path/to/jsonpaths.txt")))))
  }
}
