package jp.ne.opt.redshiftfake.parse

import jp.ne.opt.redshiftfake.s3.Credentials
import org.scalatest.FlatSpec

class CopyQueryParserTest extends FlatSpec {
  it should "parse minimal COPY query" in {
    val query =
      """
        |COPY public._foo_42 FROM 's3://some-bucket/path/to/data/foo-bar.csv'
        |CREDENTIALS 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY';
        |""".stripMargin

    val expected = CopyQuery(
      tableName = "public._foo_42",
      columnList = None,
      dataSource = "s3://some-bucket/path/to/data/foo-bar.csv",
      credentials = Credentials.WithKey(
        accessKeyId = "AKIAXXXXXXXXXXXXXXX",
        secretAccessKey = "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
      )
    )

    assert(CopyQueryParser.parse(query).contains(expected))
  }
}
